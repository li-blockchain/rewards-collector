#!/usr/bin/env python3
"""
Historical rewards backfill from local nodes.

Re-derives validator reward history directly from the local Lighthouse /
Nethermind nodes and MEV relays, replacing data previously sourced from
Beaconcha.in. Can also validate freshly-derived data against the existing
``rewards_master.parquet`` to prove equivalence before cutting over.

Usage::

    # Backfill a range of epochs (inclusive) into the master parquet
    python backfill.py --start-epoch 409000 --end-epoch 409200

    # Validate without writing - compares node data to existing parquet
    python backfill.py --validate 409143

Note: a standard (non-archive) Lighthouse node retains finalized *state* for
only ~5 months, but retains *blocks* much longer. Backfill relies on block
data, so it works for any epoch the node still serves; epochs beyond
retention are logged and skipped rather than aborting the run.
"""

import os
import sys
import asyncio
import logging
import argparse
from pathlib import Path
from typing import Dict, List, Optional, Any

import pandas as pd

from rewards_collector import RewardsCollector

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class HistoricalBackfill:
    """Backfills and validates historical rewards from local nodes."""

    def __init__(self, config: Dict[str, str]):
        # Force the local node source - backfill is the whole point of the migration.
        config = dict(config)
        config.setdefault('data_source', 'local')
        self.config = config
        self.collector = RewardsCollector(config)
        self.output_dir = Path(config.get('output_dir', './rewards_data'))
        self.parquet_file = self.output_dir / 'rewards_master.parquet'

    def _existing_epochs(self) -> set:
        """Epochs already present in the master parquet (for resume)."""
        if not self.parquet_file.exists():
            return set()
        try:
            df = pd.read_parquet(self.parquet_file, columns=['epoch'])
            return set(df['epoch'].astype(int).unique())
        except Exception as e:
            logger.warning(f"Could not read existing epochs for resume: {e}")
            return set()

    async def backfill_epoch_range(self, start_epoch: int, end_epoch: int,
                                   step: int = 1, skip_existing: bool = True) -> Dict[str, int]:
        """
        Collect and persist rewards for every epoch in ``[start_epoch, end_epoch]``.

        Errors for a single epoch (e.g. epoch beyond node retention) are logged
        and skipped so a long backfill is not derailed by one bad epoch. When
        ``skip_existing`` is set, epochs already present in the master parquet
        are skipped, making the run resumable after an interruption.

        Returns a summary dict: processed / skipped / already / withdrawals / proposals.
        """
        if end_epoch < start_epoch:
            raise ValueError(f"end_epoch ({end_epoch}) must be >= start_epoch ({start_epoch})")

        epochs = list(range(start_epoch, end_epoch + 1, step))
        already = self._existing_epochs() if skip_existing else set()
        total = len(epochs)
        logger.info("=" * 50)
        logger.info(f"🔄 Backfilling epochs {start_epoch}..{end_epoch} "
                    f"(step {step}, {total} epochs)")
        if already:
            in_range_done = len([e for e in epochs if e in already])
            logger.info(f"⏭️  Resume: {in_range_done} of these epochs already in parquet, skipping them")
        logger.info("=" * 50)

        summary = {'processed': 0, 'skipped': 0, 'already': 0,
                   'withdrawals': 0, 'proposals': 0}
        for i, epoch in enumerate(epochs, 1):
            if epoch in already:
                summary['already'] += 1
                continue
            try:
                w, p = await self.collector.collect_rewards(epoch)
                summary['processed'] += 1
                summary['withdrawals'] += w
                summary['proposals'] += p
                logger.info(f"📊 Progress {i}/{total} (epoch {epoch}): "
                            f"{w} withdrawals, {p} proposals")
            except Exception as e:
                summary['skipped'] += 1
                logger.warning(f"⚠️  Skipping epoch {epoch} after error: {e}")

        logger.info("🎉 Backfill complete: "
                    f"{summary['processed']} processed, {summary['already']} already-present, "
                    f"{summary['skipped']} skipped (errors), "
                    f"{summary['withdrawals']} withdrawals, {summary['proposals']} proposals")
        if self.collector.fallback_count:
            logger.warning(f"⚠️  Beaconcha.in fallback was used {self.collector.fallback_count} time(s)")
        return summary

    def _load_existing_epoch(self, epoch: int) -> pd.DataFrame:
        if not self.parquet_file.exists():
            return pd.DataFrame()
        df = pd.read_parquet(self.parquet_file)
        return df[df['epoch'] == epoch]

    async def validate_against_existing(self, epoch: int,
                                        amount_tolerance: int = 0) -> Dict[str, Any]:
        """
        Compare node-derived data against existing parquet data for ``epoch``.

        Does NOT persist. Returns a diff report covering withdrawal amounts,
        proposal counts and MEV relay attribution. ``amount_tolerance`` allows
        a small absolute (Gwei/Wei) difference if exact equality is too strict.
        """
        existing = self._load_existing_epoch(epoch)
        new_w, new_p = await self.collector.gather_records(epoch)

        report: Dict[str, Any] = {'epoch': epoch, 'match': True,
                                  'withdrawals': {}, 'proposals': {}}

        # --- Withdrawals: validator_index -> amount (Gwei) ---
        ex_w = existing[existing['record_type'] == 'withdrawal'] if not existing.empty else pd.DataFrame()
        ex_w_map = dict(zip(ex_w['validator_index'].astype(int), ex_w['amount'].astype(int))) \
            if not ex_w.empty else {}
        new_w_map = {int(r['validator_index']): int(r['amount']) for r in new_w}
        w_mismatch = [
            {'validator_index': v, 'new': new_w_map.get(v), 'existing': ex_w_map.get(v)}
            for v in set(new_w_map) | set(ex_w_map)
            if abs(new_w_map.get(v, 0) - ex_w_map.get(v, 0)) > amount_tolerance
        ]
        report['withdrawals'] = {
            'new_count': len(new_w_map), 'existing_count': len(ex_w_map),
            'mismatches': w_mismatch,
        }

        # --- Proposals: validator_index -> (mev_source, amount Wei) ---
        ex_p = existing[existing['record_type'] == 'proposal'] if not existing.empty else pd.DataFrame()
        ex_p_map = {int(r.validator_index): (r.mev_source, int(r.amount)) for r in ex_p.itertuples()} \
            if not ex_p.empty else {}
        new_p_map = {int(r['validator_index']): (r['mev_source'], int(r['amount'])) for r in new_p}
        p_mismatch = []
        for v in set(new_p_map) | set(ex_p_map):
            n = new_p_map.get(v)
            x = ex_p_map.get(v)
            if n is None or x is None:
                p_mismatch.append({'validator_index': v, 'new': n, 'existing': x})
            elif n[0] != x[0] or abs(n[1] - x[1]) > amount_tolerance:
                p_mismatch.append({'validator_index': v, 'new': n, 'existing': x})
        report['proposals'] = {
            'new_count': len(new_p_map), 'existing_count': len(ex_p_map),
            'mismatches': p_mismatch,
        }

        report['match'] = not w_mismatch and not p_mismatch
        return report


def print_validation_report(report: Dict[str, Any]) -> None:
    w, p = report['withdrawals'], report['proposals']
    status = "✅ MATCH" if report['match'] else "❌ DIFFERENCES FOUND"
    logger.info(f"\n=== Validation report for epoch {report['epoch']}: {status} ===")
    logger.info(f"Withdrawals: node={w['new_count']} existing={w['existing_count']} "
                f"mismatches={len(w['mismatches'])}")
    for m in w['mismatches'][:10]:
        logger.info(f"   ⚠️  validator {m['validator_index']}: node={m['new']} existing={m['existing']}")
    logger.info(f"Proposals: node={p['new_count']} existing={p['existing_count']} "
                f"mismatches={len(p['mismatches'])}")
    for m in p['mismatches'][:10]:
        logger.info(f"   ⚠️  validator {m['validator_index']}: node={m['new']} existing={m['existing']}")


def load_config() -> Dict[str, str]:
    env_file = Path(__file__).parent / '.env'
    if env_file.exists():
        try:
            from dotenv import load_dotenv
            load_dotenv(env_file)
        except ImportError:
            pass
    return {
        'api_key': os.getenv('API_KEY'),
        'validator_csv': os.getenv('VALIDATOR_CSV', '../data/validators_updated.csv'),
        'output_dir': os.getenv('OUTPUT_DIR', './rewards_data'),
        'data_source': 'local',
        'beaconchain_fallback': os.getenv('BEACONCHAIN_FALLBACK', 'true'),
        'beacon_node_url': os.getenv('BEACON_NODE_URL', 'http://libc-prod2:5052'),
        'execution_rpc_url': os.getenv('EXECUTION_RPC_URL') or os.getenv('RPC_URL'),
    }


def main():
    parser = argparse.ArgumentParser(description='Historical rewards backfill from local nodes')
    parser.add_argument('--start-epoch', type=int, help='First epoch to backfill (inclusive)')
    parser.add_argument('--end-epoch', type=int, help='Last epoch to backfill (inclusive)')
    parser.add_argument('--step', type=int, default=1, help='Epoch step (default: 1)')
    parser.add_argument('--force', action='store_true',
                        help='Re-collect epochs even if already present in parquet')
    parser.add_argument('--validate', type=int, metavar='EPOCH',
                        help='Validate node data against existing parquet for EPOCH (no write)')
    parser.add_argument('--tolerance', type=int, default=0,
                        help='Allowed absolute amount difference for validation (default: 0)')
    args = parser.parse_args()

    config = load_config()
    backfill = HistoricalBackfill(config)

    if args.validate is not None:
        report = asyncio.run(backfill.validate_against_existing(args.validate, args.tolerance))
        print_validation_report(report)
        sys.exit(0 if report['match'] else 1)

    if args.start_epoch is None or args.end_epoch is None:
        parser.error("--start-epoch and --end-epoch are required (or use --validate)")

    asyncio.run(backfill.backfill_epoch_range(
        args.start_epoch, args.end_epoch, args.step, skip_existing=not args.force))


if __name__ == '__main__':
    main()
