"""
Unit tests for the event-driven continuous monitor.

Two halves:

  * SSE subscription on LighthouseClient (node_client.py) - frame parsing,
    epoch yielding, reconnect/backoff, single-shot mode.
  * ContinuousRewardsMonitor (rewards_continuous_monitor.py) - state
    persistence, resume precedence, startup gap backfill, live event
    processing, failure handling and graceful shutdown.

Everything runs against mocks - no beacon node, execution node or network.
"""

import sys
import os
import json
from unittest.mock import Mock, patch

import pytest
import requests
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from node_client import LighthouseClient
import rewards_continuous_monitor as mod


# ======================================================================
# SSE subscription (LighthouseClient)
# ======================================================================

def sse_lines(*events):
    """
    Build SSE wire lines for a sequence of finalized_checkpoint events.

    Each ``events`` item is an epoch (int) or a raw data string. Mirrors what
    requests' iter_lines(decode_unicode=True) yields: per-line, newline
    stripped, with a blank line terminating each event.
    """
    lines = []
    for ev in events:
        if isinstance(ev, int):
            data = json.dumps({'block': '0xabc', 'state': '0xdef',
                               'epoch': str(ev), 'execution_optimistic': False})
        else:
            data = ev
        lines.append('event: finalized_checkpoint')
        lines.append(f'data: {data}')
        lines.append('')  # event terminator
    return lines


class FakeStreamResponse:
    """Stand-in for a streaming requests.Response usable as a context manager."""

    def __init__(self, lines, status_code=200):
        self._lines = lines
        self.status_code = status_code

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"{self.status_code} Error")

    def iter_lines(self, decode_unicode=False):
        for line in self._lines:
            yield line

    def close(self):
        pass


class TestSSEFrameParsing:
    def test_parses_multiple_events(self):
        resp = FakeStreamResponse(sse_lines(100, 101))
        out = list(LighthouseClient._iter_sse_data(resp))
        assert [int(e['epoch']) for e in out] == [100, 101]

    def test_ignores_comments_and_keepalives(self):
        lines = [':', ':keep-alive'] + sse_lines(200)
        resp = FakeStreamResponse(lines)
        out = list(LighthouseClient._iter_sse_data(resp))
        assert len(out) == 1 and int(out[0]['epoch']) == 200

    def test_skips_non_json_payload(self):
        lines = ['data: not-json', '', *sse_lines(300)]
        resp = FakeStreamResponse(lines)
        out = list(LighthouseClient._iter_sse_data(resp))
        assert [int(e['epoch']) for e in out] == [300]

    def test_no_trailing_blank_line_drops_incomplete_event(self):
        # An event whose terminating blank line never arrives is not emitted.
        lines = ['event: finalized_checkpoint', 'data: {"epoch": "5"}']
        resp = FakeStreamResponse(lines)
        assert list(LighthouseClient._iter_sse_data(resp)) == []


class TestSubscribeFinalizedCheckpoints:
    def test_yields_epoch_ints(self):
        lh = LighthouseClient()
        lh.session = Mock()
        lh.session.get.return_value = FakeStreamResponse(sse_lines(100, 101, 102))
        gen = lh.subscribe_finalized_checkpoints(reconnect=False)
        assert list(gen) == [100, 101, 102]

    def test_skips_event_without_epoch(self):
        lh = LighthouseClient()
        lh.session = Mock()
        lh.session.get.return_value = FakeStreamResponse(
            sse_lines('{"block": "0xabc"}', 101))
        assert list(lh.subscribe_finalized_checkpoints(reconnect=False)) == [101]

    def test_single_shot_returns_on_clean_close(self):
        # reconnect=False: generator ends when the server closes the stream.
        lh = LighthouseClient()
        lh.session = Mock()
        lh.session.get.return_value = FakeStreamResponse(sse_lines(7))
        assert list(lh.subscribe_finalized_checkpoints(reconnect=False)) == [7]

    def test_single_shot_raises_on_connection_error(self):
        lh = LighthouseClient()
        lh.session = Mock()
        lh.session.get.side_effect = requests.ConnectionError("down")
        with pytest.raises(requests.ConnectionError):
            list(lh.subscribe_finalized_checkpoints(reconnect=False))

    def test_reconnects_after_connection_error(self):
        lh = LighthouseClient()
        lh.session = Mock()
        # First attempt drops, second attempt delivers an event.
        lh.session.get.side_effect = [
            requests.ConnectionError("reset"),
            FakeStreamResponse(sse_lines(500)),
        ]
        slept = []
        with patch('node_client.time.sleep', side_effect=slept.append):
            gen = lh.subscribe_finalized_checkpoints(reconnect=True)
            first = next(gen)
        assert first == 500
        assert slept == [1.0]  # backed off once before the successful retry

    def test_backoff_is_exponential_and_capped(self):
        lh = LighthouseClient()
        lh.session = Mock()
        lh.session.get.side_effect = [
            requests.ConnectionError("1"),
            requests.ConnectionError("2"),
            requests.ConnectionError("3"),
            FakeStreamResponse(sse_lines(9)),
        ]
        slept = []
        with patch('node_client.time.sleep', side_effect=slept.append):
            gen = lh.subscribe_finalized_checkpoints(reconnect=True, max_backoff=2.0)
            assert next(gen) == 9
        assert slept == [1.0, 2.0, 2.0]  # 1, 2, then capped at 2


# ======================================================================
# ContinuousRewardsMonitor
# ======================================================================

@pytest.fixture
def build_monitor(tmp_path, monkeypatch):
    """
    Factory producing a ContinuousRewardsMonitor with RewardsCollector and
    LighthouseClient replaced by mocks. Returns (monitor, collected_epochs).
    `collected_epochs` records every epoch passed to collect_rewards.
    """
    def make(finalized=100, events=None, fail_on=None, configured=None):
        collected = []

        fake_collector = Mock()
        fake_collector.data_source_name = 'local'

        async def collect(epoch):
            if fail_on is not None and epoch == fail_on:
                raise RuntimeError(f"boom on {epoch}")
            collected.append(epoch)
            return (1, 0)
        fake_collector.collect_rewards.side_effect = collect

        fake_lh = Mock()
        fake_lh.base_url = 'http://test:5052'
        fake_lh.get_finalized_epoch.return_value = finalized
        fake_lh.subscribe_finalized_checkpoints.return_value = iter(events or [])
        fake_lh.session = Mock()

        monkeypatch.setattr(mod, 'RewardsCollector', lambda config: fake_collector)
        monkeypatch.setattr(mod, 'LighthouseClient', lambda url: fake_lh)

        config = {'output_dir': str(tmp_path), 'validator_csv': 'x',
                  'monitor_start_epoch': configured}
        monitor = mod.ContinuousRewardsMonitor(config)
        return monitor, collected

    return make


class TestStatePersistence:
    def test_write_then_read_roundtrip(self, build_monitor):
        monitor, _ = build_monitor()
        monitor._write_state(4242)
        assert monitor._read_state() == 4242
        # File is valid JSON with the documented shape.
        data = json.loads(monitor.state_file.read_text())
        assert data['last_processed_epoch'] == 4242
        assert 'last_updated' in data

    def test_read_missing_state_returns_none(self, build_monitor):
        monitor, _ = build_monitor()
        assert monitor._read_state() is None

    def test_read_corrupt_state_returns_none(self, build_monitor):
        monitor, _ = build_monitor()
        monitor.state_file.write_text("{ not json")
        assert monitor._read_state() is None


class TestResumePrecedence:
    def test_resume_from_state_file(self, build_monitor):
        monitor, _ = build_monitor(finalized=100)
        monitor._write_state(90)
        assert monitor._resume_epoch(100) == 91

    def test_resume_from_parquet_when_no_state(self, build_monitor, tmp_path):
        monitor, _ = build_monitor(finalized=100)
        pd.DataFrame({'epoch': [80, 81, 82]}).to_parquet(
            tmp_path / 'rewards_master.parquet', index=False)
        assert monitor._resume_epoch(100) == 83

    def test_resume_from_configured_when_no_state_or_parquet(self, build_monitor):
        monitor, _ = build_monitor(finalized=100, configured=70)
        assert monitor._resume_epoch(100) == 70

    def test_resume_live_when_nothing_known(self, build_monitor):
        monitor, _ = build_monitor(finalized=100)
        assert monitor._resume_epoch(100) == 100


class TestStartupBackfill:
    def test_backfills_gap_then_processes_events(self, build_monitor):
        # Down since epoch 95; node now at 100; then 101, 102 finalize live.
        monitor, collected = build_monitor(finalized=100, events=[101, 102])
        monitor._write_state(95)
        monitor.run()
        assert collected == [96, 97, 98, 99, 100, 101, 102]
        assert monitor._read_state() == 102

    def test_up_to_date_skips_backfill(self, build_monitor):
        monitor, collected = build_monitor(finalized=100, events=[101])
        monitor._write_state(100)
        monitor.run()
        assert collected == [101]

    def test_live_start_collects_current_finalized(self, build_monitor):
        # No prior state -> begins live at finalized (100), then 101 arrives.
        monitor, collected = build_monitor(finalized=100, events=[101])
        monitor.run()
        assert collected == [100, 101]


class TestEventHandling:
    def test_ignores_already_processed_event(self, build_monitor):
        monitor, collected = build_monitor(finalized=100, events=[100, 101])
        monitor._write_state(100)
        monitor.run()
        assert collected == [101]  # 100 already done, skipped

    def test_event_gap_is_filled(self, build_monitor):
        # A missed SSE event leaves a gap; the next event catches up the range.
        monitor, collected = build_monitor(finalized=100, events=[103])
        monitor._write_state(100)
        monitor.run()
        assert collected == [101, 102, 103]


class TestFailureHandling:
    def test_failure_does_not_advance_state(self, build_monitor):
        # Backfill 96..100 but epoch 98 fails -> state stops at 97.
        monitor, collected = build_monitor(finalized=100, fail_on=98)
        monitor._write_state(95)
        monitor.run()
        assert collected == [96, 97]
        assert monitor._read_state() == 97


class TestGracefulShutdown:
    def test_shutdown_flag_halts_catchup(self, build_monitor):
        monitor, collected = build_monitor(finalized=110)
        monitor.last_processed_epoch = 100
        monitor._shutdown = True
        monitor._process_through(110)
        assert collected == []  # nothing collected once shutdown is set

    def test_signal_handler_sets_shutdown_when_idle(self, build_monitor):
        monitor, _ = build_monitor()
        monitor._collecting = False
        import signal as _signal
        with pytest.raises(KeyboardInterrupt):
            monitor._handle_signal(_signal.SIGTERM, None)
        assert monitor._shutdown is True

    def test_signal_handler_defers_when_collecting(self, build_monitor):
        monitor, _ = build_monitor()
        monitor._collecting = True
        import signal as _signal
        # No raise while mid-collection - just flips the flag.
        monitor._handle_signal(_signal.SIGTERM, None)
        assert monitor._shutdown is True
