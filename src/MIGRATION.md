# Beaconcha.in → Local Node Migration

This project can collect validator rewards either from **Beaconcha.in** (legacy)
or directly from **self-hosted nodes** (Lighthouse consensus + Nethermind
execution) and **MEV-Boost relays**. The local-node path is the migration
target and is now the default (`DATA_SOURCE=local`).

## Why migrate

- No third-party API dependency, rate limits, or API key for ongoing collection.
- Direct MEV relay attribution (Flashbots, Ultra Sound, bloXroute, Titan, …).
- Full control over historical backfill.

## Architecture

| Concern | Source | Module |
|---|---|---|
| Withdrawals (consensus rewards, Gwei) | Lighthouse `/eth/v2/beacon/blocks/{slot}` → execution_payload.withdrawals | `node_client.LighthouseClient` |
| Proposals (which validator proposed) | same block scan → `proposer_index` | `node_client.LighthouseClient` |
| MEV reward + relay tag (Wei) | relay `/relay/v1/data/bidtraces/proposer_payload_delivered` | `mev_relay_client.MEVRelayClient` |
| Non-relay proposer reward (priority fees, Wei) | Nethermind `eth_getBlockByNumber` + `eth_getBlockReceipts` | `node_client.ExecutionClient` |
| Validator status / exit detection | Lighthouse `/eth/v1/beacon/states/head/validators` | `node_client.LighthouseClient` |

A single pass over an epoch's 32 slot-blocks yields **both** withdrawals and
proposals. Output schema and units are **identical** to the historical
Beaconcha.in data, so the two are directly comparable:

- withdrawal `amount` → **Gwei**
- proposal `amount` → **Wei** (relay value, or EL producer reward when no relay)
- proposal `mev_source` → relay tag (matches Beaconcha.in tags) or `''`

## Configuration

```
DATA_SOURCE=local                       # 'local' or 'beaconchain'
BEACON_NODE_URL=http://libc-prod2:5052  # Lighthouse
EXECUTION_RPC_URL=http://libc-prod2:8545 # Nethermind (defaults to RPC_URL)
BEACONCHAIN_FALLBACK=true               # fall back to Beaconcha.in on local failure
API_KEY=...                             # only needed for beaconchain / fallback
```

## Usage

```bash
# Collect one epoch from local nodes (default source)
python rewards_collector.py 453620

# Backfill a range of epochs (inclusive)
python backfill.py --start-epoch 409000 --end-epoch 409200

# Validate node data against existing parquet for an epoch (no write)
python backfill.py --validate 409143
```

## Phased rollout

1. **Parallel / validate** — keep `BEACONCHAIN_FALLBACK=true`. Run
   `backfill.py --validate <epoch>` across sampled epochs to confirm parity
   (the comparison covers withdrawal amounts, proposal counts, and MEV relay
   attribution). Equivalence has been verified against historical data with
   zero discrepancies.
2. **Primary switch** — run live collection with `DATA_SOURCE=local`; fallback
   absorbs transient node outages and is logged at WARNING level
   (`RewardsCollector.fallback_count`).
3. **Full migration** — once validated, set `BEACONCHAIN_FALLBACK=false`.
   `API_KEY` then becomes unnecessary for collection.

## Notes / limits

- A standard (non-archive) Lighthouse node retains finalized **state** for
  ~5 months but retains **blocks** much longer; backfill relies on block data.
  Epochs beyond retention are logged and skipped, not fatal.
- Missed/orphaned slots return 404 and are skipped silently.
- A single unreachable relay never aborts attribution — other relays are still
  queried, and a block with no relay match falls back to the EL producer reward.

---

# Polling → Event-Driven Continuous Monitoring

A second migration builds on the local-node source above: the polling monitor
(`rewards_monitor.py`) is superseded by an **event-driven** monitor
(`rewards_continuous_monitor.py`) that reacts the moment an epoch finalizes and
needs **no Beaconcha.in API key**.

## Why

| | `rewards_monitor.py` (old) | `rewards_continuous_monitor.py` (new) |
|---|---|---|
| Trigger | polls every 60s, jumps 100 epochs | SSE `finalized_checkpoint` push (~6.4 min) |
| Reward source | Beaconcha.in (`API_KEY` required) | local nodes (key only as fallback) |
| Restart behaviour | restarts from `EPOCH_WATCH_START` | resumes from state file, backfills the gap |
| Latency | up to a full poll interval | near-real-time on finalization |

## Architecture

```
 Lighthouse beacon node                 rewards_continuous_monitor.py
 ┌─────────────────────┐                ┌──────────────────────────────┐
 │ GET /eth/v1/events  │  SSE event     │ subscribe_finalized_          │
 │  ?topics=finalized_ │ ─────────────► │   checkpoints()  (epoch N)    │
 │   checkpoint        │                │            ▼                  │
 └─────────────────────┘                │   _process_through(N)         │
                                        │            ▼ per epoch        │
                                        │   RewardsCollector            │
                                        │     .collect_rewards(epoch)   │
                                        │            ▼                  │
                                        │   rewards_master.parquet      │
                                        │   .monitor_state.json (resume)│
                                        └──────────────────────────────┘
```

The SSE subscription is `LighthouseClient.subscribe_finalized_checkpoints()`
(`node_client.py`): a lightweight inline SSE parser over `requests` streaming
(**no new dependencies**) with automatic reconnect and exponential backoff.

## Configuration

Reuses the local-node config above, plus:

| Variable | Purpose | Default |
|---|---|---|
| `OUTPUT_DIR` | Where `rewards_master.parquet` + state live | `./rewards_data` |
| `MONITOR_START_EPOCH` | Explicit start for a fresh deploy (no state/parquet) | live |

### Resume / start-point precedence

On startup the monitor picks its first epoch in this order:

1. `.monitor_state.json` → resume after `last_processed_epoch`.
2. Max epoch already in `rewards_master.parquet` → resume after it.
3. `MONITOR_START_EPOCH` if set.
4. Otherwise start **live** at the current finalized epoch.

Any gap between the resume point and the current finalized epoch is backfilled
before the live subscription begins. For deep historical recovery, use
`backfill.py`. State is written after every epoch:

```json
{ "last_processed_epoch": 453630, "last_updated": "2026-06-09T21:00:00+00:00" }
```

## Usage

```bash
# Run the continuous monitor in the foreground
python rewards_continuous_monitor.py
```

## systemd

`systemd/rewards-monitor.service` runs it as a long-running daemon (no timer,
unlike `rewards-backfiller`):

```bash
sudo cp systemd/rewards-monitor.service /etc/systemd/system/   # edit paths first
sudo systemctl daemon-reload
sudo systemctl enable --now rewards-monitor.service

systemctl status rewards-monitor.service      # health + last lines
journalctl -u rewards-monitor.service -f      # follow live logs
```

`Restart=on-failure` (`RestartSec=30`) auto-recovers from crashes;
`TimeoutStopSec=90` lets the in-flight epoch finish on stop. A restart is safe
at any time — the monitor resumes from `.monitor_state.json`.

## Troubleshooting

| Symptom | Likely cause | Action |
|---|---|---|
| Exits with "Could not reach beacon node" | `BEACON_NODE_URL` wrong / node down | `curl $BEACON_NODE_URL/eth/v1/beacon/states/finalized/finality_checkpoints` |
| Repeated "Beacon SSE connection error … reconnecting" | `/eth/v1/events` not exposed / node restarting | Confirm the events endpoint is enabled |
| No events arrive | proxy buffering `text/event-stream`, or node not finalizing | Ensure no proxy buffers SSE; check node is synced |
| Same epoch fails repeatedly | execution RPC unreachable, or epoch beyond retention | Check `EXECUTION_RPC_URL`; deep history → `backfill.py` |
| Reprocesses epochs after restart | state file not writable | Ensure `OUTPUT_DIR` ∈ `ReadWritePaths` and writable |

## Deprecation of `rewards_monitor.py`

The polling monitor is retained only for reference/rollback. Both write the
same `rewards_master.parquet` schema, so cutover is just stopping one and
starting the other — the continuous monitor backfills the gap.

## Tests

```bash
python -m pytest tests/   # pytest.ini disables an unrelated broken web3 plugin
```

- `tests/test_node_client.py` — LighthouseClient + ExecutionClient (mocked HTTP)
- `tests/test_mev_relay_client.py` — MEVRelayClient (mocked relays)
- `tests/test_migration_comparison.py` — LocalNodeDataSource records + validation diff
- `tests/test_continuous_monitor.py` — SSE subscription + ContinuousRewardsMonitor
