#!/usr/bin/env python3
"""
Lido CSM (Community Staking Module) reward reporting.

CSM rewards are paid to a *node operator id* (LIBC = operator 105), not to
individual validators, as **stETH shares** allocated by a cumulative
OpenZeppelin StandardMerkleTree. The tree is published hourly to
``github.com/lidofinance/csm-rewards`` (mainnet branch, ``tree.json``) and its
IPFS CID is exposed on-chain by ``CSFeeDistributor.treeCid()``.

Reporting model:
  * cumulative shares for an operator = its leaf value in the latest tree
  * ETH value = ``stETH.getPooledEthByShares(shares)``
  * rewards earned in a period = cumulative(end) − cumulative(start), where the
    boundary trees are fetched from the repo's git history (commit nearest the
    boundary timestamp).

This is read-only reporting — we never claim.
"""

import logging
from typing import Any, Dict, List, Optional

import requests
from web3 import Web3

logger = logging.getLogger(__name__)

# Ethereum mainnet (docs.lido.fi/deployed-contracts).
CSFEE_DISTRIBUTOR = '0xD99CC66fEC647E68294C6477B40fC7E0F6F618D0'
STETH = '0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84'

CSM_REWARDS_REPO = 'lidofinance/csm-rewards'
TREE_BRANCH = 'mainnet'

_CSFD_ABI = [
    {"name": "treeCid", "inputs": [], "outputs": [{"type": "string"}],
     "stateMutability": "view", "type": "function"},
]
_STETH_ABI = [
    {"name": "getPooledEthByShares", "inputs": [{"type": "uint256"}],
     "outputs": [{"type": "uint256"}], "stateMutability": "view", "type": "function"},
]


class CSMRewardsClient:
    """Reads CSM operator reward entitlements from the published Merkle tree."""

    def __init__(self, rpc_url: str, timeout: float = 25.0):
        self.w3 = Web3(Web3.HTTPProvider(rpc_url, request_kwargs={'timeout': timeout}))
        self.timeout = timeout
        self.csfd = self.w3.eth.contract(
            address=Web3.to_checksum_address(CSFEE_DISTRIBUTOR), abi=_CSFD_ABI)
        self.steth = self.w3.eth.contract(
            address=Web3.to_checksum_address(STETH), abi=_STETH_ABI)
        self.session = requests.Session()
        self._tree_cache: Dict[str, Dict[str, Any]] = {}

    # ------------------------------------------------------------------
    # Tree fetching
    # ------------------------------------------------------------------
    def tree_cid(self) -> str:
        """The IPFS CID of the current tree, per the on-chain distributor."""
        return self.csfd.functions.treeCid().call()

    def fetch_tree(self, ref: str = TREE_BRANCH) -> Dict[str, Any]:
        """
        Fetch the StandardMerkleTree dump at a git ``ref`` (branch or commit SHA).

        ``{format, leafEncoding: ['uint256','uint256'], values: [{value:[opId,
        cumulativeShares]}]}``. Cached per ref.
        """
        if ref in self._tree_cache:
            return self._tree_cache[ref]
        url = f"https://raw.githubusercontent.com/{CSM_REWARDS_REPO}/{ref}/tree.json"
        resp = self.session.get(url, timeout=self.timeout)
        resp.raise_for_status()
        tree = resp.json()
        self._tree_cache[ref] = tree
        return tree

    def _commit_before(self, timestamp: int) -> Optional[str]:
        """Newest tree.json commit SHA at or before ``timestamp`` (unix seconds)."""
        from datetime import datetime, timezone
        until = datetime.fromtimestamp(timestamp, timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
        url = f"https://api.github.com/repos/{CSM_REWARDS_REPO}/commits"
        try:
            resp = self.session.get(
                url, params={'sha': TREE_BRANCH, 'path': 'tree.json',
                             'until': until, 'per_page': 1},
                timeout=self.timeout)
            resp.raise_for_status()
            commits = resp.json()
            return commits[0]['sha'] if commits else None
        except (requests.RequestException, KeyError, IndexError) as e:
            logger.warning(f"Could not resolve CSM tree commit before {until}: {e}")
            return None

    # ------------------------------------------------------------------
    # Reward reads
    # ------------------------------------------------------------------
    @staticmethod
    def cumulative_shares_in_tree(tree: Dict[str, Any], operator_id: int) -> int:
        """Operator's cumulative fee shares in a given tree (0 if absent)."""
        target = str(operator_id)
        for entry in tree.get('values', []):
            if str(entry['value'][0]) == target:
                return int(entry['value'][1])
        return 0

    def shares_to_eth(self, shares: int) -> float:
        """Convert stETH shares to ETH at the current pool rate."""
        if shares <= 0:
            return 0.0
        return self.steth.functions.getPooledEthByShares(int(shares)).call() / 1e18

    def get_cumulative_eth(self, operator_id: int, ref: str = TREE_BRANCH) -> float:
        tree = self.fetch_tree(ref)
        return self.shares_to_eth(self.cumulative_shares_in_tree(tree, operator_id))

    def get_period_rewards(self, operator_ids: List[int],
                           start_ts: Optional[int] = None,
                           end_ts: Optional[int] = None) -> Dict[str, Any]:
        """
        Rewards earned by the operators between two timestamps.

        ``end`` defaults to the latest tree; ``start`` defaults to "from zero"
        (cumulative) when no boundary tree can be resolved. Returns per-operator
        and total ETH, plus current cumulative ETH for the report.
        """
        end_ref = TREE_BRANCH
        if end_ts is not None:
            sha = self._commit_before(end_ts)
            if sha:
                end_ref = sha
        end_tree = self.fetch_tree(end_ref)

        start_tree = None
        if start_ts is not None:
            sha = self._commit_before(start_ts)
            if sha:
                start_tree = self.fetch_tree(sha)

        per_operator = {}
        total_period_shares = 0
        total_cumulative_shares = 0
        for op in operator_ids:
            end_shares = self.cumulative_shares_in_tree(end_tree, op)
            start_shares = self.cumulative_shares_in_tree(start_tree, op) if start_tree else 0
            period_shares = max(0, end_shares - start_shares)
            total_period_shares += period_shares
            total_cumulative_shares += end_shares
            per_operator[op] = {'start_shares': start_shares, 'end_shares': end_shares,
                                'period_shares': period_shares}

        period_eth = self.shares_to_eth(total_period_shares)
        cumulative_eth = self.shares_to_eth(total_cumulative_shares)
        for op, d in per_operator.items():
            d['period_eth'] = self.shares_to_eth(d['period_shares'])

        return {
            'operator_ids': operator_ids,
            'period_eth': period_eth,
            'cumulative_eth': cumulative_eth,
            'per_operator': per_operator,
            'boundary': {'start_resolved': start_tree is not None, 'end_ref': end_ref},
        }
