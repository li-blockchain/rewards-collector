#!/usr/bin/env python3
"""
On-chain cumulative reward snapshots.

libc-prod2 prunes historical state, so a past ``eth_call`` (e.g. a vault's
``totalValue`` 8 days ago) reverts with "no state". This collector periodically
records the cumulative on-chain values that the invoice needs to diff over a
billing period:

  * Lido stVault  - ``totalValue`` and accrued node-operator fee per vault
  * Lido CSM      - cumulative reward ETH per operator id

Rows are appended to ``rewards_data/onchain_snapshots.parquet`` in long format
``{ts, block, metric, key, value}``. ``period_value`` then diffs the snapshots
bracketing a period — robust without an archive node.

Run on a timer (systemd ``onchain-snapshot.timer``). CSM period accounting still
also works off the published tree's git history; snapshots are the reliable
source for the vault.
"""

import logging
import os
import time
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
from web3 import Web3

from clients import list_clients, get_client
from lido_vault import StVaultClient
from lido_csm import CSMRewardsClient

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

SNAPSHOT_FILE = 'rewards_data/onchain_snapshots.parquet'


def _rpc_url() -> str:
    return (os.getenv('EXECUTION_RPC_URL') or os.getenv('RPC_URL')
            or 'http://libc-prod2:8545')


class SnapshotCollector:
    def __init__(self, rpc_url: Optional[str] = None, snapshot_file: str = SNAPSHOT_FILE):
        self.rpc_url = rpc_url or _rpc_url()
        self.snapshot_file = Path(snapshot_file)
        self.w3 = Web3(Web3.HTTPProvider(self.rpc_url))

    def collect(self) -> List[Dict]:
        """Snapshot every distinct vault + CSM operator across all clients."""
        block = self.w3.eth.block_number
        ts = self.w3.eth.get_block(block)['timestamp']
        rows: List[Dict] = []

        vaults = {}
        operators = set()
        for cid in list_clients():
            c = get_client(cid)
            if c.stvault and c.stvault.get('dashboard'):
                vaults[c.stvault['dashboard'].lower()] = c.stvault['dashboard']
            operators.update(c.csm_operator_ids)

        for dash in vaults.values():
            try:
                st = StVaultClient(self.rpc_url, dash).get_state()
                rows.append({'ts': ts, 'block': block, 'metric': 'vault_total_value',
                             'key': dash.lower(), 'value': st['total_value_eth']})
                rows.append({'ts': ts, 'block': block, 'metric': 'vault_accrued_fee',
                             'key': dash.lower(), 'value': st['accrued_fee_eth']})
            except Exception as e:
                logger.warning(f"vault snapshot failed for {dash}: {e}")

        if operators:
            try:
                csm = CSMRewardsClient(self.rpc_url)
                for op in operators:
                    rows.append({'ts': ts, 'block': block, 'metric': 'csm_cumulative_eth',
                                 'key': str(op), 'value': csm.get_cumulative_eth(op)})
            except Exception as e:
                logger.warning(f"csm snapshot failed: {e}")

        if rows:
            self._append(rows)
        logger.info(f"📸 Snapshot @ block {block}: {len(rows)} metrics recorded")
        return rows

    def _append(self, rows: List[Dict]) -> None:
        self.snapshot_file.parent.mkdir(parents=True, exist_ok=True)
        new = pd.DataFrame(rows)
        if self.snapshot_file.exists():
            existing = pd.read_parquet(self.snapshot_file)
            new = pd.concat([existing, new], ignore_index=True)
        new.to_parquet(self.snapshot_file, index=False)


def value_at(snapshot_file: str, metric: str, key: str, at_ts: int) -> Optional[float]:
    """Latest snapshot value for (metric, key) at or before ``at_ts``."""
    p = Path(snapshot_file)
    if not p.exists():
        return None
    df = pd.read_parquet(p)
    sel = df[(df['metric'] == metric) & (df['key'] == str(key).lower()) & (df['ts'] <= at_ts)]
    if sel.empty:
        return None
    return float(sel.sort_values('ts').iloc[-1]['value'])


def period_value(snapshot_file: str, metric: str, key: str,
                 start_ts: int, end_ts: int) -> Optional[float]:
    """Change in (metric, key) between the snapshots bracketing the period."""
    start = value_at(snapshot_file, metric, key, start_ts)
    end = value_at(snapshot_file, metric, key, end_ts)
    if start is None or end is None:
        return None
    return end - start


def main():
    env = Path(__file__).parent / '.env'
    if env.exists():
        try:
            from dotenv import load_dotenv
            load_dotenv(env)
        except ImportError:
            pass
    SnapshotCollector().collect()


if __name__ == '__main__':
    main()
