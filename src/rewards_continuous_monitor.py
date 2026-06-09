#!/usr/bin/env python3
"""
Event-driven continuous validator rewards monitor.

Subscribes to the local beacon node's ``finalized_checkpoint`` Server-Sent
Events stream and, as each epoch finalizes (~every 6.4 min on mainnet),
collects that epoch's rewards via :class:`RewardsCollector` and appends them
to ``rewards_master.parquet``.

This is the real-time replacement for the polling-based ``rewards_monitor.py``.
It uses the local Lighthouse/Nethermind nodes (the migration target) and needs
no Beaconcha.in API key - the key is only consulted as an optional fallback if
configured.

State is tracked in ``OUTPUT_DIR/.monitor_state.json`` so the service resumes
exactly where it left off and backfills any epochs that finalized while it was
down (e.g. across a restart or deploy).

Usage::

    python rewards_continuous_monitor.py

Configuration is read from the environment / ``.env`` (see ``load_config``).
"""

import os
import sys
import json
import signal
import asyncio
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Optional

from node_client import LighthouseClient
from rewards_collector import RewardsCollector

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

STATE_FILENAME = '.monitor_state.json'


class ContinuousRewardsMonitor:
    """
    Long-running, event-driven monitor.

    Lifecycle:
      1. On startup, determine the resume epoch (state file -> parquet ->
         configured start -> live) and backfill any epochs missed while down.
      2. Subscribe to ``finalized_checkpoint`` SSE events and collect each
         newly-finalized epoch as it arrives.
      3. On SIGTERM/SIGINT, finish the in-flight epoch (if any) and exit
         cleanly without leaving partial state.
    """

    def __init__(self, config: Dict[str, str]):
        config = dict(config)
        config.setdefault('data_source', 'local')
        self.config = config
        self.collector = RewardsCollector(config)

        beacon_url = config.get('beacon_node_url', 'http://libc-prod2:5052')
        # Dedicated client for finalization + the long-lived SSE connection,
        # kept separate from the collector's internal block-fetch client.
        self.lighthouse = LighthouseClient(beacon_url)

        self.output_dir = Path(config.get('output_dir', './rewards_data'))
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.state_file = self.output_dir / STATE_FILENAME

        # Explicit starting epoch for a fresh deployment with no prior state
        # and no existing parquet. Unset -> begin live at the current finalized
        # epoch (use backfill.py to recover deep history).
        start = config.get('monitor_start_epoch')
        self.configured_start_epoch = int(start) if start not in (None, '') else None

        self._shutdown = False
        self._collecting = False
        self._loop = asyncio.new_event_loop()
        self.last_processed_epoch: Optional[int] = None

    # ------------------------------------------------------------------
    # State persistence
    # ------------------------------------------------------------------
    def _read_state(self) -> Optional[int]:
        """Last processed epoch from the state file, or None if unavailable."""
        if not self.state_file.exists():
            return None
        try:
            data = json.loads(self.state_file.read_text())
            return int(data['last_processed_epoch'])
        except (ValueError, KeyError, OSError) as e:
            logger.warning(f"Could not read monitor state ({e}); ignoring")
            return None

    def _write_state(self, epoch: int) -> None:
        """Persist the last processed epoch atomically (write-tmp-then-rename)."""
        self.last_processed_epoch = epoch
        payload = {
            'last_processed_epoch': epoch,
            'last_updated': datetime.now(timezone.utc).isoformat(),
        }
        tmp = self.state_file.with_name(self.state_file.name + '.tmp')
        tmp.write_text(json.dumps(payload, indent=2))
        tmp.replace(self.state_file)  # atomic on POSIX

    def _parquet_max_epoch(self) -> Optional[int]:
        """Highest epoch already present in the master parquet, if any."""
        parquet = self.output_dir / 'rewards_master.parquet'
        if not parquet.exists():
            return None
        try:
            import pandas as pd
            df = pd.read_parquet(parquet, columns=['epoch'])
            if df.empty:
                return None
            return int(df['epoch'].max())
        except Exception as e:
            logger.warning(f"Could not read parquet for resume point: {e}")
            return None

    def _resume_epoch(self, finalized: int) -> int:
        """
        First epoch the monitor should collect on startup.

        Precedence: state file -> existing parquet -> configured start epoch
        -> live (current finalized epoch).
        """
        state = self._read_state()
        if state is not None:
            logger.info(f"📒 Resuming from state file: last processed epoch {state}")
            return state + 1
        pq = self._parquet_max_epoch()
        if pq is not None:
            logger.info(f"📒 No state file; resuming after max parquet epoch {pq}")
            return pq + 1
        if self.configured_start_epoch is not None:
            logger.info(f"📒 No prior data; starting at configured epoch "
                        f"{self.configured_start_epoch}")
            return self.configured_start_epoch
        logger.info(f"📒 No prior data; starting live at current finalized "
                    f"epoch {finalized}")
        return finalized

    # ------------------------------------------------------------------
    # Collection
    # ------------------------------------------------------------------
    def _process_epoch(self, epoch: int) -> None:
        """Collect and persist one epoch, then advance state."""
        self._collecting = True
        try:
            w, p = self._loop.run_until_complete(self.collector.collect_rewards(epoch))
        finally:
            self._collecting = False
        self._write_state(epoch)
        logger.info(f"✅ Epoch {epoch} collected: {w} withdrawals, {p} proposals")

    def _process_through(self, target_epoch: int) -> None:
        """
        Collect every not-yet-processed epoch up to and including ``target_epoch``.

        Picks up at ``last_processed_epoch + 1``. A failure on an epoch stops
        the catch-up *without* advancing state past it, so the next event
        retries from the failed epoch rather than skipping it.
        """
        if self.last_processed_epoch is None:
            start = target_epoch
        else:
            start = self.last_processed_epoch + 1
        for epoch in range(start, target_epoch + 1):
            if self._shutdown:
                logger.info("🛑 Shutdown requested; stopping catch-up")
                return
            try:
                self._process_epoch(epoch)
            except Exception as e:
                logger.error(f"❌ Failed to collect epoch {epoch}: {e}; "
                             f"will retry on next event")
                return  # leave state at the last good epoch

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    def run(self) -> None:
        """Backfill any gap, then process finalized epochs as they arrive."""
        self._install_signal_handlers()
        logger.info("=" * 50)
        logger.info("🚀 LIBC Continuous Rewards Monitor (event-driven)")
        logger.info("=" * 50)
        logger.info(f"🛰️  Beacon node:  {self.lighthouse.base_url}")
        logger.info(f"📁 Output dir:   {self.output_dir}")
        logger.info(f"🔌 Data source:  {self.collector.data_source_name}")

        finalized = self.lighthouse.get_finalized_epoch()
        if finalized <= 0:
            logger.error("Could not reach beacon node for finalized epoch; aborting")
            sys.exit(1)

        resume = self._resume_epoch(finalized)
        # last_processed sits one below the resume point so catch-up starts there.
        self.last_processed_epoch = resume - 1

        if resume <= finalized:
            logger.info(f"⏪ Backfilling missed epochs {resume}..{finalized}")
            self._process_through(finalized)
        else:
            logger.info(f"✅ Up to date (resume {resume} > finalized {finalized}); "
                        f"waiting for new epochs")

        if self._shutdown:
            self._close()
            logger.info("👋 Monitor stopped")
            return

        logger.info("👂 Subscribing to finalized_checkpoint events...")
        try:
            for epoch in self.lighthouse.subscribe_finalized_checkpoints():
                if self._shutdown:
                    break
                if self.last_processed_epoch is not None and epoch <= self.last_processed_epoch:
                    logger.debug(f"Ignoring already-processed finalized epoch {epoch}")
                    continue
                logger.info(f"🔔 Finalized checkpoint event: epoch {epoch}")
                self._process_through(epoch)
                if self._shutdown:
                    break
        except KeyboardInterrupt:
            # Raised by the signal handler to break the blocking SSE read.
            pass
        finally:
            self._close()
        logger.info("👋 Monitor stopped")

    def _install_signal_handlers(self) -> None:
        signal.signal(signal.SIGTERM, self._handle_signal)
        signal.signal(signal.SIGINT, self._handle_signal)

    def _handle_signal(self, signum, frame) -> None:
        name = signal.Signals(signum).name
        logger.info(f"🛑 Received {name}; shutting down gracefully...")
        self._shutdown = True
        # If we're idle on the blocking SSE read, interrupt it so we exit
        # promptly. If we're mid-collection, let it finish to avoid leaving a
        # partially-written parquet; the loop checks _shutdown right after.
        if not self._collecting:
            raise KeyboardInterrupt

    def _close(self) -> None:
        try:
            self.lighthouse.session.close()
        finally:
            if not self._loop.is_closed():
                self._loop.close()


def load_config() -> Dict[str, str]:
    """Load configuration from the environment / ``.env`` (no API key required)."""
    env_file = Path(__file__).parent / '.env'
    if env_file.exists():
        try:
            from dotenv import load_dotenv
            load_dotenv(env_file)
        except ImportError:
            pass
    return {
        # Optional - only used if a local-node query fails and fallback is on.
        'api_key': os.getenv('API_KEY'),
        'validator_csv': os.getenv('VALIDATOR_CSV', '../data/validators_updated.csv'),
        'output_dir': os.getenv('OUTPUT_DIR', './rewards_data'),
        'data_source': os.getenv('DATA_SOURCE', 'local'),
        'beaconchain_fallback': os.getenv('BEACONCHAIN_FALLBACK', 'true'),
        'beacon_node_url': os.getenv('BEACON_NODE_URL', 'http://libc-prod2:5052'),
        'execution_rpc_url': os.getenv('EXECUTION_RPC_URL') or os.getenv('RPC_URL'),
        # Optional explicit start epoch for a fresh deploy with no prior data.
        'monitor_start_epoch': os.getenv('MONITOR_START_EPOCH'),
    }


def main() -> None:
    try:
        config = load_config()
        monitor = ContinuousRewardsMonitor(config)
        monitor.run()
    except KeyboardInterrupt:
        logger.info("\n👋 Goodbye!")
    except Exception as e:
        logger.error(f"❌ Fatal error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
