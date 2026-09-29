"""
Migration validation / comparison tests.

Two layers:

1. TestLocalNodeDataSource - the local source builds canonical records with
   the correct schema and units (withdrawals in Gwei, proposals in Wei, MEV
   attribution vs EL producer-reward fallback, exit flagging).

2. TestValidationDiff - HistoricalBackfill.validate_against_existing correctly
   reports equivalence (and surfaces discrepancies) between node-derived data
   and an existing parquet file.

All node/relay access is mocked, so these run in CI without live nodes.
"""

import sys
import os
import asyncio
from unittest.mock import Mock, AsyncMock

import pandas as pd
import pytest
import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data_sources import LocalNodeDataSource
from backfill import HistoricalBackfill


class FakeValidatorReader:
    """Minimal ValidatorReader stand-in."""

    def __init__(self, meta):
        # meta: index(str) -> {'type','node','minipool'}
        self._meta = meta

    def get_validator_by_index(self, index):
        return self._meta.get(str(index))

    def load_validators(self):
        return [{'index': k, **v} for k, v in self._meta.items()]


FAR_FUTURE = 2**64 - 1

META = {
    '100': {'type': '8', 'node': 'node3', 'minipool': '0xmp100'},
    '200': {'type': '32', 'node': 'node3', 'minipool': '0xmp200'},
}

CANONICAL_COLUMNS = {
    'record_type', 'validator_index', 'amount', 'epoch', 'datetime',
    'validator_type', 'node', 'minipool', 'mev_source', 'exec_block_number', 'is_exit',
}


def build_source(lighthouse, execution, mev):
    return LocalNodeDataSource(FakeValidatorReader(META), lighthouse, execution, mev)


class TestLocalNodeDataSource:
    def test_withdrawal_records_schema_and_gwei(self):
        lh = Mock()
        lh.get_epoch_blocks.return_value = []  # blocks unused (raw provided below)
        lh.get_withdrawable_epochs.return_value = (20, {'100': FAR_FUTURE, '200': 9})
        lh.get_withdrawals.return_value = [
            {'validatorindex': 100, 'amount': 17692142, 'epoch': 10, 'timestamp': 1700000000},
            {'validatorindex': 200, 'amount': 32_000_000_000, 'epoch': 10, 'timestamp': 1700000000},
        ]

        src = build_source(lh, Mock(), Mock())
        records = src.collect_withdrawals(['100', '200'], 10)

        assert len(records) == 2
        for r in records:
            assert set(r.keys()) == CANONICAL_COLUMNS
            assert r['record_type'] == 'withdrawal'
            assert r['mev_source'] is None
            assert r['exec_block_number'] is None
        by_idx = {r['validator_index']: r for r in records}
        assert by_idx[100]['amount'] == 17692142            # Gwei preserved
        assert by_idx[100]['is_exit'] is False
        assert by_idx[100]['validator_type'] == '8'          # metadata joined
        assert by_idx[200]['is_exit'] is True                # exited validator flagged

    def test_proposal_uses_mev_relay_value_in_wei(self):
        lh = Mock()
        lh.get_epoch_blocks.return_value = []
        lh.get_proposals.return_value = [
            {'proposerindex': 100, 'slot': 320, 'exec_block_number': 999,
             'epoch': 10, 'timestamp': 1700000000, 'fee_recipient': '0xfr'},
        ]
        mev = Mock()
        mev.get_payload.return_value = {'relay': 'ultrasound-relay', 'value': 18990254566602138}
        execution = Mock()

        src = build_source(lh, execution, mev)
        records = src.collect_proposals(['100'], 10)

        assert len(records) == 1
        r = records[0]
        assert set(r.keys()) == CANONICAL_COLUMNS
        assert r['record_type'] == 'proposal'
        assert r['mev_source'] == 'ultrasound-relay'
        assert r['amount'] == 18990254566602138             # Wei from relay
        assert r['exec_block_number'] == 999
        assert r['is_exit'] is False
        execution.get_producer_reward.assert_not_called()    # relay hit -> no EL fallback

    def test_proposal_falls_back_to_el_producer_reward(self):
        lh = Mock()
        lh.get_epoch_blocks.return_value = []
        lh.get_proposals.return_value = [
            {'proposerindex': 200, 'slot': 321, 'exec_block_number': 1000,
             'epoch': 10, 'timestamp': 1700000000, 'fee_recipient': '0xfr'},
        ]
        mev = Mock()
        mev.get_payload.return_value = None                   # no relay delivered
        execution = Mock()
        execution.get_producer_reward.return_value = 17503755057090890

        src = build_source(lh, execution, mev)
        records = src.collect_proposals(['200'], 10)

        r = records[0]
        assert r['mev_source'] == ''                          # no relay attribution
        assert r['amount'] == 17503755057090890               # EL producer reward (Wei)
        execution.get_producer_reward.assert_called_once_with(1000)

    def test_blocks_fetched_once_per_epoch(self):
        lh = Mock()
        lh.get_epoch_blocks.return_value = ['block']
        lh.get_withdrawals.return_value = []
        lh.get_proposals.return_value = []
        src = build_source(lh, Mock(), Mock())

        src.collect_withdrawals(['100'], 10)
        src.collect_proposals(['100'], 10)
        # Cached: only one network pass over the epoch's slots.
        lh.get_epoch_blocks.assert_called_once_with(10)

    def test_exit_after_process_start_is_flagged(self):
        """Regression, 2026-08-23: a validator active when the monitor started
        and exited weeks later must still be flagged. The old head-status cache
        lived for the whole process and stored these as rewards."""
        lh = Mock()
        lh.get_epoch_blocks.return_value = []
        lh.get_withdrawable_epochs.side_effect = [
            (10, {'200': FAR_FUTURE}),   # at start: active
            (500, {'200': 480}),         # later: exited, withdrawable at 480
        ]
        src = build_source(lh, Mock(), Mock())

        lh.get_withdrawals.return_value = [
            {'validatorindex': 200, 'amount': 17_000_000, 'epoch': 10, 'timestamp': 1}]
        assert src.collect_withdrawals(['200'], 10)[0]['is_exit'] is False

        lh.get_withdrawals.return_value = [
            {'validatorindex': 200, 'amount': 32_010_000_000, 'epoch': 490, 'timestamp': 2}]
        assert src.collect_withdrawals(['200'], 490)[0]['is_exit'] is True

    def test_snapshot_reused_for_epochs_it_covers(self):
        """Backfill: one head snapshot answers every earlier epoch."""
        lh = Mock()
        lh.get_epoch_blocks.return_value = []
        lh.get_withdrawable_epochs.return_value = (1000, {'200': 480})
        src = build_source(lh, Mock(), Mock())

        for epoch, expect_exit in [(100, False), (479, False), (480, True), (900, True)]:
            lh.get_withdrawals.return_value = [
                {'validatorindex': 200, 'amount': 1, 'epoch': epoch, 'timestamp': 1}]
            assert src.collect_withdrawals(['200'], epoch)[0]['is_exit'] is expect_exit
        lh.get_withdrawable_epochs.assert_called_once()

    def test_skim_between_exit_and_withdrawable_is_not_exit(self):
        lh = Mock()
        lh.get_epoch_blocks.return_value = []
        lh.get_withdrawable_epochs.return_value = (1000, {'200': 480})
        lh.get_withdrawals.return_value = [
            {'validatorindex': 200, 'amount': 17_000_000, 'epoch': 300, 'timestamp': 1}]
        src = build_source(lh, Mock(), Mock())
        assert src.collect_withdrawals(['200'], 300)[0]['is_exit'] is False

    def test_no_withdrawals_skips_status_lookup(self):
        lh = Mock()
        lh.get_epoch_blocks.return_value = []
        lh.get_withdrawals.return_value = []
        src = build_source(lh, Mock(), Mock())
        assert src.collect_withdrawals(['200'], 10) == []
        lh.get_withdrawable_epochs.assert_not_called()

    def test_status_lookup_failure_propagates(self):
        """Fail the epoch rather than persist exits unflagged."""
        lh = Mock()
        lh.get_epoch_blocks.return_value = []
        lh.get_withdrawals.return_value = [
            {'validatorindex': 200, 'amount': 32_000_000_000, 'epoch': 10, 'timestamp': 1}]
        lh.get_withdrawable_epochs.side_effect = requests.ConnectionError("down")
        src = build_source(lh, Mock(), Mock())
        with pytest.raises(requests.ConnectionError):
            src.collect_withdrawals(['200'], 10)


def _write_parquet(path, rows):
    pd.DataFrame(rows).to_parquet(path, index=False)


def _record(record_type, idx, amount, epoch, mev_source=None, block=None, is_exit=False):
    return {
        'record_type': record_type, 'validator_index': idx, 'amount': amount,
        'epoch': epoch, 'datetime': 1700000000, 'validator_type': '8',
        'node': 'node3', 'minipool': '0xmp', 'mev_source': mev_source,
        'exec_block_number': block, 'is_exit': is_exit,
    }


class TestValidationDiff:
    def _backfill_with(self, tmp_path, existing_rows, gathered):
        parquet = tmp_path / 'rewards_master.parquet'
        _write_parquet(str(parquet), existing_rows)

        # Build HistoricalBackfill but swap in a mocked collector so no real
        # RewardsCollector / node access is constructed.
        bf = HistoricalBackfill.__new__(HistoricalBackfill)
        bf.output_dir = tmp_path
        bf.parquet_file = parquet
        bf.collector = Mock()
        bf.collector.gather_records = AsyncMock(return_value=gathered)
        return bf

    def test_reports_match_when_identical(self, tmp_path):
        existing = [
            _record('withdrawal', 100, 17692142, 10),
            _record('proposal', 200, 18990254566602138, 10, 'ultrasound-relay', 999),
        ]
        gathered = (
            [_record('withdrawal', 100, 17692142, 10)],
            [_record('proposal', 200, 18990254566602138, 10, 'ultrasound-relay', 999)],
        )
        bf = self._backfill_with(tmp_path, existing, gathered)
        report = asyncio.run(bf.validate_against_existing(10))
        assert report['match'] is True
        assert report['withdrawals']['mismatches'] == []
        assert report['proposals']['mismatches'] == []

    def test_detects_withdrawal_amount_mismatch(self, tmp_path):
        existing = [_record('withdrawal', 100, 17692142, 10)]
        gathered = ([_record('withdrawal', 100, 9999999, 10)], [])
        bf = self._backfill_with(tmp_path, existing, gathered)
        report = asyncio.run(bf.validate_against_existing(10))
        assert report['match'] is False
        assert len(report['withdrawals']['mismatches']) == 1

    def test_detects_relay_attribution_mismatch(self, tmp_path):
        existing = [_record('proposal', 200, 500, 10, 'ultrasound-relay', 999)]
        gathered = ([], [_record('proposal', 200, 500, 10, 'flashbots', 999)])
        bf = self._backfill_with(tmp_path, existing, gathered)
        report = asyncio.run(bf.validate_against_existing(10))
        assert report['match'] is False
        assert len(report['proposals']['mismatches']) == 1

    def test_detects_missing_proposal(self, tmp_path):
        existing = [_record('proposal', 200, 500, 10, 'ultrasound-relay', 999)]
        gathered = ([], [])  # node found no proposal that exists in parquet
        bf = self._backfill_with(tmp_path, existing, gathered)
        report = asyncio.run(bf.validate_against_existing(10))
        assert report['match'] is False
        assert len(report['proposals']['mismatches']) == 1

    def test_tolerance_allows_small_diff(self, tmp_path):
        existing = [_record('withdrawal', 100, 1000, 10)]
        gathered = ([_record('withdrawal', 100, 1005, 10)], [])
        bf = self._backfill_with(tmp_path, existing, gathered)
        report = asyncio.run(bf.validate_against_existing(10, amount_tolerance=10))
        assert report['match'] is True
