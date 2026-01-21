# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Validator Performance Aggregator (aka rewards-collector) that extracts Ethereum validator rewards using the Beaconcha.in API. The system monitors validators every 100 epochs and captures consensus layer withdrawals and execution layer block proposal rewards.

## Architecture

**Core Components:**
- `src/rewards_collector.py` - Single epoch rewards collection with Parquet output
- `src/rewards_monitor.py` - Continuous monitoring daemon
- `src/rewards_backfiller.py` - Historical data backfilling with Discord notifications
- `src/bot.py` - Discord bot for queries and notifications
- `src/cdp_monitor.py` - CDP position monitoring with alerts
- `src/invoice.py` - Invoice generation with LEB adjustments
- `src/generate_invoice.py` - Invoice export to Excel

**Data Flow:**
1. Validator indices read from CSV files in `data/` directory
2. Beaconcha.in API calls made in chunks of 100 validators
3. Rewards data saved to Parquet files for efficient analytics
4. Invoice scripts process stored data with LEB (Liquid Ethereum Bonds) adjustments

## Development Commands

```bash
# Install dependencies
cd src
pip install -r requirements.txt

# Single epoch collection
python rewards_collector.py 390000

# Continuous monitoring
python rewards_monitor.py

# Historical backfilling
python rewards_backfiller.py

# Run Discord bot
python bot.py

# Generate invoice
python invoice.py
```

## Environment Configuration

Copy `.env.dist` to `.env` and configure:
- `API_KEY` - Beaconcha.in API key (required)
- `DISCORD_BOT_TOKEN` - Discord bot token
- `DISCORD_WEBHOOK_URL` - For webhook notifications
- `RPC_URL` - Ethereum RPC endpoint
- `EPOCH_START` - Starting epoch for monitoring
- `EPOCH_INTERVAL` - Epochs between checks (default: 100)
- `CDP_POSITION_ADDRESS` - CDP position to monitor
- `OPENAI_API_KEY` - For AI features

## Validator Data Format

Validator CSV files in `data/` directory require format:
`Index,Pubkey,Type,Node,Minipool address`

The system primarily uses the validator index. The Type field is used for LEB (Liquid Ethereum Bond) reward adjustments:
- Type 8-14: LEB8 (14% commission on borrowed portion)
- Type 16: LEB16 (15% commission on borrowed portion)

## Output Format

Rewards are stored in Parquet files (`rewards_{epoch}.parquet`) with schema:
- `record_type`: "withdrawal" or "proposal"
- `validator_index`: Validator index number
- `amount`: Reward amount in ETH
- `epoch`: Epoch number
- `datetime`: Unix timestamp
- `validator_type`, `node`, `minipool`: Validator metadata
- `mev_source`, `exec_block_number`: Proposal-specific data
