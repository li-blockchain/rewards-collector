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

## Tests

```bash
python -m pytest tests/   # pytest.ini disables an unrelated broken web3 plugin
```

- `tests/test_node_client.py` — LighthouseClient + ExecutionClient (mocked HTTP)
- `tests/test_mev_relay_client.py` — MEVRelayClient (mocked relays)
- `tests/test_migration_comparison.py` — LocalNodeDataSource records + validation diff
