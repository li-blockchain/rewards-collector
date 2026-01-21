# Python Rewards Collector

A Python implementation to extract Ethereum validator rewards using the Beaconcha.in API and save them to efficient Parquet files.

Features efficient Parquet columnar storage for analytics.

## Features

- 🚀 **Fast**: No rate limiting, parallel processing
- 💾 **Efficient Storage**: Parquet columnar format for analytics
- 🔄 **Flexible**: Single epoch, continuous monitoring, or backfilling
- 📊 **Analytics Ready**: Pandas/Arrow compatible output
- 🎯 **Type Safety**: Full type hints throughout

## Installation

```bash
cd src
pip install -r requirements.txt
```

## Configuration

Set up your environment variables (create a `.env` file or export):

```bash
# Required
API_KEY=your_beaconcha_in_api_key

# Optional - with defaults
VALIDATOR_CSV=../data/validators_updated.csv
OUTPUT_DIR=./rewards_data
EPOCH_WATCH_START=0
EPOCH_INTERVAL=100
CHECK_INTERVAL=60
DATA_DIR=./
```

## Usage

### 1. Single Epoch Collection

Collect rewards for a specific epoch:

```bash
python rewards_collector.py 390000
python rewards_collector.py 390000 --csv ../data/validators.csv --output ./output
```

### 2. Continuous Monitoring

Run continuous monitoring:

```bash
python rewards_monitor.py
```

This will:
- Check for new epochs every 60 seconds (configurable)
- Process rewards every 100 epochs (configurable)
- Save to parquet files automatically

### 3. Historical Backfilling

Fill in historical data:

```bash
python rewards_backfiller.py
python rewards_backfiller.py --start-epoch 380000 --delay 10
```

Features:
- Tracks progress in `.lastepoch` file
- Resumes from last processed epoch
- Discord webhook notifications (optional)
- Configurable delay between batches

## Output Format

The collector generates a single unified Parquet file per epoch: `rewards_{epoch}.parquet`

### Schema: `rewards_{epoch}.parquet`
```
record_type     | validator_index | amount | epoch  | datetime   | validator_type | node | minipool | mev_source | exec_block_number
withdrawal      | 1299681        | 32.1   | 390000 | 1234567890 | 32             | 3    | 0x...    | null       | null
proposal        | 1299681        | 0.05   | 390000 | 1234567890 | 32             | 3    | 0x...    | flashbots  | 17403189
```

**Column Descriptions:**
- `record_type`: "withdrawal" or "proposal"
- `validator_index`: Validator index number
- `amount`: Reward amount in ETH
- `epoch`: Epoch number when reward occurred
- `datetime`: Unix timestamp
- `validator_type`: Validator type (e.g., "32", "8", "16" for LEB)
- `node`: Node identifier
- `minipool`: Minipool address
- `mev_source`: MEV relay name (proposals only, null for withdrawals)
- `exec_block_number`: Execution block number (proposals only, null for withdrawals)

## Reading Parquet Files

```python
import pandas as pd

# Read all rewards for a specific epoch
rewards = pd.read_parquet('rewards_data/rewards_390000.parquet')
print(rewards.head())

# Filter by record type
withdrawals = rewards[rewards['record_type'] == 'withdrawal']
proposals = rewards[rewards['record_type'] == 'proposal']

# Combine multiple epochs
import glob
all_rewards = pd.concat([
    pd.read_parquet(f) for f in glob.glob('rewards_data/rewards_*.parquet')
])

# Analysis examples
total_withdrawals = all_rewards[all_rewards['record_type'] == 'withdrawal']['amount'].sum()
mev_rewards = all_rewards[
    (all_rewards['record_type'] == 'proposal') &
    (all_rewards['mev_source'].notna())
]['amount'].sum()

print(f"Total withdrawals: {total_withdrawals} ETH")
print(f"Total MEV rewards: {mev_rewards} ETH")
```

## Architecture

```
rewards_collector.py
├── ValidatorReader: Read CSV, skip headers, chunk validators
├── BeaconchainAPI: HTTP client for Beaconcha.in API calls
├── RewardProcessor: Clean & flatten API responses
└── ParquetWriter: Save to efficient columnar format

rewards_monitor.py: Continuous monitoring daemon
rewards_backfiller.py: Historical data backfilling
```

## Troubleshooting

**Missing API Key:**
```
❌ API_KEY environment variable is required
```
→ Set your Beaconcha.in API key in `.env` or environment

**CSV File Not Found:**
```
❌ Validator CSV file not found: ../data/validators.csv
```
→ Check the path to your validator CSV file

**No Data Retrieved:**
```
💾 No withdrawals to save
💾 No proposals to save
```
→ Normal for epochs with no validator activity

## Performance

- **Faster**: No rate limiting delays
- **Concurrent**: Processes multiple chunks efficiently
- **Compact**: Parquet compression saves ~70% storage
- **Queryable**: Direct SQL queries with DuckDB/Arrow