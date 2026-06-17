#!/usr/bin/env python3
"""
Lido v3 stVault reporting (REPORT-ONLY).

Reads a Lido v3 staking vault for the invoice's metrics page. Nothing here is
billable: the node-operator fee is auto-collected, and the Lido protocol fees are
costs the vault pays — both are reported, not charged.

Two data sources:

  * Dashboard (per-vault management contract) - current ``totalValue``, the
    LIBC node-operator fee (``feeRate`` bps + ``accruedFee``), and the address of
    the VaultHub via ``VAULT_HUB()``.

  * VaultHub (the protocol accounting hub) - the authoritative figures:
      - ``vaultRecord(vault)``      -> cumulative/settled Lido fees, inOutDelta,
                                       liability; rewards = totalValue - inOutDelta
      - ``vaultConnection(vault)``  -> Lido fee-rate basis points
                                       (infra / liquidity / reservation / reserve)
      - event ``VaultReportApplied``-> per-oracle-report totalValue, inOutDelta,
                                       cumulativeLidoFees (emitted ~weekly)

Vault value only changes when an oracle report is applied (~weekly), so period
figures are derived from ``VaultReportApplied`` logs (exact, historical, and
state-independent — libc-prod2 prunes historical state). Reading logs needs only
block data, which the node retains.
"""

import logging
from typing import Any, Dict, List, Optional

from eth_abi import decode as abi_decode
from web3 import Web3

logger = logging.getLogger(__name__)

_DASHBOARD_ABI = [
    {"name": n, "inputs": [], "outputs": [{"type": t}],
     "stateMutability": "view", "type": "function"}
    for n, t in [
        ('totalValue', 'uint256'), ('feeRate', 'uint256'),
        ('accruedFee', 'uint256'), ('feeRecipient', 'address'),
        ('stakingVault', 'address'), ('VAULT_HUB', 'address'),
    ]
]

# VaultHub.vaultRecord / vaultConnection return structs. We only need a subset of
# fields, but a tuple ABI must describe the whole struct in order. Mirrors
# lidofinance/core VaultHub.sol (0.8.25/vaults).
_REPORT_TUPLE = {'type': 'tuple', 'components': [
    {'name': 'totalValue', 'type': 'uint104'},
    {'name': 'inOutDelta', 'type': 'int104'},
    {'name': 'timestamp', 'type': 'uint48'},
]}
_INOUT_CACHE_TUPLE = {'type': 'tuple', 'components': [
    {'name': 'value', 'type': 'int104'},
    {'name': 'valueOnRefSlot', 'type': 'int104'},
    {'name': 'refSlot', 'type': 'uint48'},
]}
_VAULT_RECORD_TUPLE = {'type': 'tuple', 'components': [
    {'name': 'report', **_REPORT_TUPLE},
    {'name': 'maxLiabilityShares', 'type': 'uint96'},
    {'name': 'liabilityShares', 'type': 'uint96'},
    {'name': 'inOutDelta', 'type': 'tuple[2]', 'components': _INOUT_CACHE_TUPLE['components']},
    {'name': 'minimalReserve', 'type': 'uint128'},
    {'name': 'redemptionShares', 'type': 'uint128'},
    {'name': 'cumulativeLidoFees', 'type': 'uint128'},
    {'name': 'settledLidoFees', 'type': 'uint128'},
]}
_VAULT_CONNECTION_TUPLE = {'type': 'tuple', 'components': [
    {'name': 'owner', 'type': 'address'},
    {'name': 'shareLimit', 'type': 'uint96'},
    {'name': 'vaultIndex', 'type': 'uint96'},
    {'name': 'disconnectInitiatedTs', 'type': 'uint48'},
    {'name': 'reserveRatioBP', 'type': 'uint16'},
    {'name': 'forcedRebalanceThresholdBP', 'type': 'uint16'},
    {'name': 'infraFeeBP', 'type': 'uint16'},
    {'name': 'liquidityFeeBP', 'type': 'uint16'},
    {'name': 'reservationFeeBP', 'type': 'uint16'},
    {'name': 'beaconChainDepositsPauseIntent', 'type': 'bool'},
]}
_VAULTHUB_ABI = [
    {"name": "vaultRecord", "inputs": [{"type": "address"}],
     "outputs": [_VAULT_RECORD_TUPLE], "stateMutability": "view", "type": "function"},
    {"name": "vaultConnection", "inputs": [{"type": "address"}],
     "outputs": [_VAULT_CONNECTION_TUPLE], "stateMutability": "view", "type": "function"},
]

# event VaultReportApplied(address indexed vault, uint256 reportTimestamp,
#   uint256 reportTotalValue, int256 reportInOutDelta, uint256 reportCumulativeLidoFees,
#   uint256 reportLiabilityShares, uint256 reportMaxLiabilityShares, uint256 reportSlashingReserve)
_REPORT_EVENT_SIG = ("VaultReportApplied(address,uint256,uint256,int256,uint256,"
                     "uint256,uint256,uint256)")
_REPORT_EVENT_TYPES = ['uint256', 'uint256', 'int256', 'uint256', 'uint256', 'uint256', 'uint256']


class StVaultClient:
    """Report-only reader for a Lido v3 stVault (Dashboard + VaultHub)."""

    def __init__(self, rpc_url: str, dashboard: str, timeout: float = 25.0):
        self.w3 = Web3(Web3.HTTPProvider(rpc_url, request_kwargs={'timeout': timeout}))
        self.dashboard = self.w3.eth.contract(
            address=Web3.to_checksum_address(dashboard), abi=_DASHBOARD_ABI)
        self._vault: Optional[str] = None
        self._hub = None

    # ------------------------------------------------------------------
    # Resolution helpers
    # ------------------------------------------------------------------
    @property
    def vault(self) -> str:
        if self._vault is None:
            self._vault = self.dashboard.functions.stakingVault().call()
        return self._vault

    @property
    def hub(self):
        if self._hub is None:
            hub_addr = self.dashboard.functions.VAULT_HUB().call()
            self._hub = self.w3.eth.contract(
                address=Web3.to_checksum_address(hub_addr), abi=_VAULTHUB_ABI)
        return self._hub

    # ------------------------------------------------------------------
    # Current state (VaultHub authoritative + Dashboard NO fee)
    # ------------------------------------------------------------------
    def get_state(self) -> Dict[str, Any]:
        rec = self.hub.functions.vaultRecord(Web3.to_checksum_address(self.vault)).call()
        conn = self.hub.functions.vaultConnection(Web3.to_checksum_address(self.vault)).call()
        report = rec[0]                       # (totalValue, inOutDelta, timestamp)
        total_value = report[0] / 1e18
        inout_delta = report[1] / 1e18
        cumulative_lido_fees = rec[6] / 1e18
        settled_lido_fees = rec[7] / 1e18

        no_fee_bps = self.dashboard.functions.feeRate().call()
        return {
            'total_value_eth': total_value,
            'inout_delta_eth': inout_delta,
            'cumulative_rewards_eth': total_value - inout_delta,
            'lido_fees_cumulative_eth': cumulative_lido_fees,
            'lido_fees_settled_eth': settled_lido_fees,
            'lido_fees_unsettled_eth': cumulative_lido_fees - settled_lido_fees,
            'fee_rates_bps': {
                'reserve_ratio': conn[4], 'infra': conn[6],
                'liquidity': conn[7], 'reservation': conn[8],
            },
            'no_fee_rate_pct': no_fee_bps / 100.0,
            'no_fee_accrued_eth': self.dashboard.functions.accruedFee().call() / 1e18,
            'last_report_ts': report[2],
        }

    # ------------------------------------------------------------------
    # Per-report history (VaultReportApplied logs)
    # ------------------------------------------------------------------
    def block_at_timestamp(self, target_ts: int) -> Optional[int]:
        """Approximate the block at/just before ``target_ts`` (header-only walk)."""
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
                guess = max(1, min(head_num, guess + (-1 if diff > 0 else 1) * max(1, abs(diff) // 12)))
            return guess
        except Exception as e:
            logger.warning(f"Could not resolve block at ts {target_ts}: {e}")
            return None

    def get_reports(self, from_ts: int, to_ts: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Decoded ``VaultReportApplied`` reports from ~3 weeks before ``from_ts``
        through ``to_ts`` (default head). Each: ts, total_value, inout_delta,
        rewards (= tv-io), cumulative_lido_fees.
        """
        topic0 = Web3.to_hex(Web3.keccak(text=_REPORT_EVENT_SIG))
        topic1 = '0x' + self.vault.lower()[2:].rjust(64, '0')
        # Look back ~3 weeks so the report bounding the period start is included.
        from_block = self.block_at_timestamp(from_ts - 21 * 86400) or 0
        # A report is APPLIED on-chain hours-to-days after its reference
        # timestamp, so fetch past the window end (and filter by each report's
        # own timestamp downstream). Otherwise a just-applied report whose
        # reference ts is inside the window is missed -> false 0.
        to_block = (self.block_at_timestamp(to_ts + 5 * 86400) if to_ts else None) \
            or self.w3.eth.block_number
        # libc-prod2 cancels a single getLogs over a wide range (-32016), so scan
        # in node-safe chunks and aggregate.
        logs = []
        chunk = 45000
        b = from_block
        while b <= to_block:
            chunk_to = min(b + chunk - 1, to_block)
            logs.extend(self.w3.eth.get_logs({
                'address': self.hub.address, 'topics': [topic0, topic1],
                'fromBlock': b, 'toBlock': chunk_to}))
            b = chunk_to + 1
        reports = []
        for lg in logs:
            ts, tv, io, cfees, _liab, _maxliab, _slash = abi_decode(
                _REPORT_EVENT_TYPES, bytes(lg['data']))
            reports.append({
                'ts': ts, 'total_value_eth': tv / 1e18, 'inout_delta_eth': io / 1e18,
                'rewards_eth': (tv - io) / 1e18, 'cumulative_lido_fees_eth': cfees / 1e18,
            })
        reports.sort(key=lambda r: r['ts'])
        return reports

    def get_period_report(self, start_ts: Optional[int] = None,
                          end_ts: Optional[int] = None) -> Dict[str, Any]:
        """Report-only vault figures: current cumulative state + per-period deltas."""
        state = self.get_state()
        out = {
            'total_value_eth': state['total_value_eth'],
            'cumulative_rewards_eth': state['cumulative_rewards_eth'],
            'lido_fees_cumulative_eth': state['lido_fees_cumulative_eth'],
            'lido_fees_settled_eth': state['lido_fees_settled_eth'],
            'fee_rates_bps': state['fee_rates_bps'],
            'no_fee_rate_pct': state['no_fee_rate_pct'],
            'no_fee_accrued_eth': state['no_fee_accrued_eth'],
            'period_rewards_eth': 0.0,
            'lido_fees_period_eth': 0.0,
            'reports_in_period': 0,
            'report_only': True,
        }
        if start_ts is None:
            return out
        try:
            reports = self.get_reports(start_ts, end_ts)
            end_cut = end_ts if end_ts else (reports[-1]['ts'] if reports else 0)
            before = [r for r in reports if r['ts'] <= start_ts]
            within = [r for r in reports if start_ts < r['ts'] <= end_cut]
            out['reports_in_period'] = len(within)
            if within:
                # Baseline = the last report before the period (so a full period's
                # accrual is captured); fall back to the first in-window report.
                base = before[-1] if before else within[0]
                last = within[-1]
                out['period_rewards_eth'] = last['rewards_eth'] - base['rewards_eth']
                out['lido_fees_period_eth'] = (last['cumulative_lido_fees_eth']
                                               - base['cumulative_lido_fees_eth'])
        except Exception as e:
            logger.warning(f"stVault period reports unavailable: {e}")
        return out
