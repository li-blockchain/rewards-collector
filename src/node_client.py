#!/usr/bin/env python3
"""
Local Ethereum node clients for validator rewards collection.

These clients replace the Beaconcha.in API with direct queries against
self-hosted nodes:

  * ``LighthouseClient``  - consensus layer (Lighthouse Beacon API).
        Source of truth for withdrawals (skimmed consensus rewards) and
        for *which* validators proposed blocks in a given epoch.

  * ``ExecutionClient``   - execution layer (Nethermind JSON-RPC).
        Used to compute the block proposer's execution-layer reward
        (priority fees) for locally-built blocks that did NOT go through
        an MEV relay.

Units (kept identical to the historical Beaconcha.in schema so the two
data sources are directly comparable):

  * Withdrawal ``amount``        -> Gwei   (matches EL payload withdrawal.amount)
  * Proposal   ``amount``        -> Wei    (MEV relay value or EL producer reward)
"""

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional, Any

import requests
from requests.adapters import HTTPAdapter

logger = logging.getLogger(__name__)

# Mainnet beacon chain constants. Pulled once from /eth/v1/config/spec and
# /eth/v1/beacon/genesis; hard-coded here to avoid a network round-trip on
# every slot->timestamp conversion. Overridable via the constructor for
# testnets.
MAINNET_SLOTS_PER_EPOCH = 32
MAINNET_SECONDS_PER_SLOT = 12
MAINNET_GENESIS_TIME = 1606824023

# Validator statuses that mean the validator has exited - a withdrawal for
# one of these is (at least partly) a principal return, not a reward.
EXITED_STATUSES = frozenset({
    'exited_unslashed',
    'exited_slashed',
    'withdrawal_possible',
    'withdrawal_done',
})


class LighthouseClient:
    """Beacon API client for a local Lighthouse consensus node."""

    def __init__(self, base_url: str = "http://libc-prod2:5052",
                 slots_per_epoch: int = MAINNET_SLOTS_PER_EPOCH,
                 seconds_per_slot: int = MAINNET_SECONDS_PER_SLOT,
                 genesis_time: int = MAINNET_GENESIS_TIME,
                 timeout: float = 10.0, max_workers: int = 6, retries: int = 2):
        self.base_url = base_url.rstrip('/')
        self.slots_per_epoch = slots_per_epoch
        self.seconds_per_slot = seconds_per_slot
        self.genesis_time = genesis_time
        self.timeout = timeout
        # Concurrency for the per-epoch slot scan. Kept modest so we stay gentle
        # on a node that may also be serving production validators.
        self.max_workers = max_workers
        self.retries = retries
        self.session = requests.Session()
        self.session.headers.update({'Accept': 'application/json'})
        # Size the connection pool to the worker count so concurrent block
        # fetches reuse connections instead of churning them.
        adapter = HTTPAdapter(pool_connections=max_workers, pool_maxsize=max_workers)
        self.session.mount('http://', adapter)
        self.session.mount('https://', adapter)

    # ------------------------------------------------------------------
    # Low-level helpers
    # ------------------------------------------------------------------
    def slot_to_timestamp(self, slot: int) -> int:
        """Unix timestamp at which a slot was/will be produced."""
        return self.genesis_time + slot * self.seconds_per_slot

    def epoch_slots(self, epoch: int) -> range:
        """The slot numbers belonging to an epoch."""
        first = epoch * self.slots_per_epoch
        return range(first, first + self.slots_per_epoch)

    def get_finalized_epoch(self) -> int:
        """Latest finalized epoch (replacement for BeaconchainAPI.get_latest_epoch)."""
        url = f"{self.base_url}/eth/v1/beacon/states/finalized/finality_checkpoints"
        try:
            resp = self.session.get(url, timeout=self.timeout)
            resp.raise_for_status()
            return int(resp.json()['data']['finalized']['epoch'])
        except (requests.RequestException, KeyError, ValueError) as e:
            logger.error(f"Error fetching finalized epoch: {e}")
            return 0

    def get_block(self, slot: int) -> Optional[Dict[str, Any]]:
        """
        Fetch a signed beacon block by slot.

        Returns the inner ``message`` dict, or ``None`` for a missed/orphaned
        slot (the node returns 404 - this is normal and not an error).
        """
        url = f"{self.base_url}/eth/v2/beacon/blocks/{slot}"
        last_exc = None
        for attempt in range(self.retries + 1):
            try:
                resp = self.session.get(url, timeout=self.timeout)
            except (requests.Timeout, requests.ConnectionError) as e:
                # Transient - retry with a short linear backoff before giving up.
                last_exc = e
                if attempt < self.retries:
                    time.sleep(0.5 * (attempt + 1))
                    continue
                logger.error(f"Error fetching block at slot {slot} after "
                             f"{self.retries + 1} attempts: {e}")
                raise

            if resp.status_code == 404:
                # Missed slot (no block proposed) - expected, skip silently.
                return None
            resp.raise_for_status()
            return resp.json()['data']['message']
        raise last_exc  # unreachable, but keeps type-checkers happy

    def get_validator_statuses(self, validator_indices: List[str]) -> Dict[str, str]:
        """
        Map validator index -> status string via the beacon node.

        Used to distinguish exit withdrawals (principal return) from regular
        reward withdrawals. Returns an empty dict on error so callers degrade
        gracefully.
        """
        if not validator_indices:
            return {}

        url = f"{self.base_url}/eth/v1/beacon/states/head/validators"
        try:
            resp = self.session.post(
                url,
                json={'ids': [str(i) for i in validator_indices]},
                timeout=self.timeout,
            )
            resp.raise_for_status()
            data = resp.json()
        except requests.RequestException as e:
            logger.error(f"Error fetching validator statuses: {e}")
            return {}

        statuses = {}
        for validator in data.get('data', []):
            statuses[str(validator['index'])] = validator['status']
        logger.info(f"📊 Fetched status for {len(statuses)} validators from beacon node")
        return statuses

    @staticmethod
    def is_validator_exited(status: str) -> bool:
        """True if the status indicates the validator has exited."""
        return status in EXITED_STATUSES

    # ------------------------------------------------------------------
    # Epoch-level extraction
    # ------------------------------------------------------------------
    def get_epoch_blocks(self, epoch: int) -> List[Dict[str, Any]]:
        """
        Fetch all present blocks for an epoch (skipping missed slots).

        Slots are fetched concurrently (``max_workers``). Each returned block
        message carries ``slot``, ``proposer_index`` and the execution payload,
        so a single pass yields both withdrawals and proposals.

        A genuine fetch failure (after retries) propagates, so the caller skips
        the whole epoch rather than persisting partial data; re-running fills it.
        """
        slots = list(self.epoch_slots(epoch))

        if self.max_workers <= 1:
            return [b for b in (self.get_block(s) for s in slots) if b is not None]

        results: List[Optional[Dict[str, Any]]] = [None] * len(slots)
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_idx = {executor.submit(self.get_block, slot): i
                             for i, slot in enumerate(slots)}
            for future in as_completed(future_to_idx):
                results[future_to_idx[future]] = future.result()
        return [b for b in results if b is not None]

    def get_withdrawals(self, validator_indices: List[str], epoch: int,
                        blocks: Optional[List[Dict[str, Any]]] = None) -> List[Dict[str, Any]]:
        """
        Withdrawals for the given validators that were included in ``epoch``.

        Returns records shaped like the Beaconcha.in withdrawals API so the
        downstream processor is source-agnostic::

            {'validatorindex': int, 'amount': int (Gwei), 'epoch': int,
             'timestamp': int}

        ``blocks`` may be passed to reuse an already-fetched epoch (avoids
        re-querying when withdrawals and proposals are collected together).
        """
        wanted = {str(i) for i in validator_indices}
        if blocks is None:
            blocks = self.get_epoch_blocks(epoch)

        withdrawals = []
        for block in blocks:
            slot = int(block['slot'])
            payload = block.get('body', {}).get('execution_payload', {})
            for w in payload.get('withdrawals', []):
                if str(w['validator_index']) in wanted:
                    withdrawals.append({
                        'validatorindex': int(w['validator_index']),
                        'amount': int(w['amount']),  # Gwei
                        'epoch': epoch,
                        'timestamp': self.slot_to_timestamp(slot),
                    })
        return withdrawals

    def get_proposals(self, validator_indices: List[str], epoch: int,
                     blocks: Optional[List[Dict[str, Any]]] = None) -> List[Dict[str, Any]]:
        """
        Blocks proposed by the given validators during ``epoch``.

        Returns the consensus-layer half of a proposal record; the
        execution/MEV reward is resolved separately (relay query or EL
        producer reward)::

            {'proposerindex': int, 'slot': int, 'exec_block_number': int,
             'epoch': int, 'timestamp': int, 'fee_recipient': str}
        """
        wanted = {str(i) for i in validator_indices}
        if blocks is None:
            blocks = self.get_epoch_blocks(epoch)

        proposals = []
        for block in blocks:
            if str(block['proposer_index']) not in wanted:
                continue
            slot = int(block['slot'])
            payload = block.get('body', {}).get('execution_payload', {})
            block_number = payload.get('block_number')
            if block_number is None:
                # Pre-merge block or empty payload - no execution reward.
                continue
            proposals.append({
                'proposerindex': int(block['proposer_index']),
                'slot': slot,
                'exec_block_number': int(block_number),
                'epoch': epoch,
                'timestamp': self.slot_to_timestamp(slot),
                'fee_recipient': payload.get('fee_recipient', ''),
            })
        return proposals


class ExecutionClient:
    """
    Minimal Nethermind (or any EL) JSON-RPC client.

    Only responsibility: compute the proposer's execution-layer reward
    (total priority fees) for a block that was built locally rather than
    delivered by an MEV relay. Base fee is burned, so the proposer reward is::

        sum over txs of (effectiveGasPrice - baseFeePerGas) * gasUsed
    """

    def __init__(self, rpc_url: str = "http://libc-prod2:8545", timeout: float = 30.0):
        self.rpc_url = rpc_url
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({'Content-Type': 'application/json'})
        self._id = 0

    def _rpc(self, method: str, params: List[Any]) -> Any:
        self._id += 1
        payload = {'jsonrpc': '2.0', 'method': method, 'params': params, 'id': self._id}
        resp = self.session.post(self.rpc_url, json=payload, timeout=self.timeout)
        resp.raise_for_status()
        data = resp.json()
        if 'error' in data:
            raise RuntimeError(f"RPC error for {method}: {data['error']}")
        return data['result']

    def get_producer_reward(self, block_number: int) -> int:
        """
        Execution-layer proposer reward (priority fees) for a block, in Wei.

        Equivalent to Beaconcha.in's ``producerReward`` field used when a
        block was not delivered through an MEV relay.
        """
        block_hex = hex(block_number)
        block = self._rpc('eth_getBlockByNumber', [block_hex, False])
        if block is None:
            raise ValueError(f"Block {block_number} not found on execution node")

        base_fee = int(block.get('baseFeePerGas', '0x0'), 16)

        # eth_getBlockReceipts gives effectiveGasPrice + gasUsed per tx in one call.
        receipts = self._rpc('eth_getBlockReceipts', [block_hex])
        total_tip = 0
        for r in receipts:
            gas_used = int(r['gasUsed'], 16)
            effective_price = int(r['effectiveGasPrice'], 16)
            tip = effective_price - base_fee
            if tip > 0:
                total_tip += tip * gas_used
        return total_tip
