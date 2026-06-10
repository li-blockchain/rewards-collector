#!/usr/bin/env python3
"""
Lido v3 stVault reporting (REPORT-ONLY).

Reads a Lido v3 staking vault via its Dashboard contract. The node-operator fee
is auto-collected on-chain, so this never produces a billable amount — it feeds
the invoice's metrics/report page only.

Dashboard view functions (confirmed by on-chain probe of
0x2bb82089511d3231be7bc52d3c79d06b21a2f13b):

    totalValue()        uint256  - vault total value (wei)
    locked()            uint256  - locked value (wei)
    withdrawableValue() uint256  - currently withdrawable (wei)
    liabilityShares()   uint256  - minted stETH liability (shares)
    feeRate()           uint256  - node-operator fee, basis points (500 = 5%)
    accruedFee()        uint256  - unclaimed node-operator fee (wei)
    feeRecipient()      address  - node-operator fee recipient
    stakingVault()      address  - the managed StakingVault

Period rewards are approximated as the change in ``totalValue`` between the
period-boundary blocks (resolved from timestamps). For exact accounting the
snapshot collector records ``totalValue``/``accruedFee`` over time.
"""

import logging
from typing import Any, Dict, Optional

from web3 import Web3

logger = logging.getLogger(__name__)

_DASHBOARD_ABI = [
    {"name": n, "inputs": [], "outputs": [{"type": t}],
     "stateMutability": "view", "type": "function"}
    for n, t in [
        ('totalValue', 'uint256'), ('locked', 'uint256'),
        ('withdrawableValue', 'uint256'), ('liabilityShares', 'uint256'),
        ('feeRate', 'uint256'), ('accruedFee', 'uint256'),
        ('feeRecipient', 'address'), ('stakingVault', 'address'),
    ]
]


class StVaultClient:
    """Report-only reader for a Lido v3 stVault Dashboard."""

    def __init__(self, rpc_url: str, dashboard: str, timeout: float = 25.0):
        self.w3 = Web3(Web3.HTTPProvider(rpc_url, request_kwargs={'timeout': timeout}))
        self.dashboard = self.w3.eth.contract(
            address=Web3.to_checksum_address(dashboard), abi=_DASHBOARD_ABI)

    def get_state(self, block_identifier='latest') -> Dict[str, Any]:
        """Full current (or historical) vault state."""
        d = self.dashboard.functions
        fee_bps = d.feeRate().call(block_identifier=block_identifier)
        return {
            'total_value_eth': d.totalValue().call(block_identifier=block_identifier) / 1e18,
            'locked_eth': d.locked().call(block_identifier=block_identifier) / 1e18,
            'withdrawable_eth': d.withdrawableValue().call(block_identifier=block_identifier) / 1e18,
            'liability_shares': d.liabilityShares().call(block_identifier=block_identifier) / 1e18,
            'fee_rate_bps': fee_bps,
            'fee_rate_pct': fee_bps / 100.0,
            'accrued_fee_eth': d.accruedFee().call(block_identifier=block_identifier) / 1e18,
            'fee_recipient': d.feeRecipient().call(block_identifier=block_identifier),
        }

    def total_value_at(self, block_identifier) -> float:
        return self.dashboard.functions.totalValue().call(block_identifier=block_identifier) / 1e18

    def block_at_timestamp(self, target_ts: int) -> Optional[int]:
        """
        Approximate the block number at/just before ``target_ts``.

        Estimates by ~12s/slot from head, then refines with a short walk. Returns
        None if the node can't serve state that far back.
        """
        try:
            head = self.w3.eth.get_block('latest')
            head_num, head_ts = head['number'], head['timestamp']
            if target_ts >= head_ts:
                return head_num
            guess = max(1, head_num - (head_ts - target_ts) // 12)
            for _ in range(40):
                blk = self.w3.eth.get_block(guess)
                diff = blk['timestamp'] - target_ts
                if abs(diff) <= 12:
                    return blk['number']
                step = max(1, abs(diff) // 12)
                guess += -step if diff > 0 else step
                guess = max(1, min(guess, head_num))
            return guess
        except Exception as e:
            logger.warning(f"Could not resolve block at ts {target_ts}: {e}")
            return None

    def get_period_report(self, start_ts: Optional[int] = None,
                          end_ts: Optional[int] = None) -> Dict[str, Any]:
        """
        Report-only vault figures for the period: current state plus the
        ``totalValue`` change between boundary blocks (gross value change).
        """
        state = self.get_state()
        period_rewards = None
        if start_ts is not None:
            try:
                start_blk = self.block_at_timestamp(start_ts)
                end_blk = self.block_at_timestamp(end_ts) if end_ts else 'latest'
                if start_blk is not None:
                    tv_start = self.total_value_at(start_blk)
                    tv_end = self.total_value_at(end_blk) if end_blk != 'latest' \
                        else state['total_value_eth']
                    period_rewards = tv_end - tv_start
            except Exception as e:
                logger.warning(f"stVault period delta unavailable: {e}")

        return {
            'total_value_eth': state['total_value_eth'],
            'period_rewards_eth': period_rewards if period_rewards is not None else 0.0,
            'fees_eth': state['accrued_fee_eth'],
            'fee_rate_pct': state['fee_rate_pct'],
            'liability_shares': state['liability_shares'],
            'withdrawable_eth': state['withdrawable_eth'],
            'report_only': True,
        }
