# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Validator Performance Aggregator (aka rewards-collector) that extracts Ethereum validator rewards using the Beaconcha.in API. The system monitors validators every 100 epochs and captures consensus layer withdrawals and execution layer block proposal rewards. It consists of both Node.js monitoring scripts and Python invoicing/reporting components.

## Architecture

**Core Components:**
- `index.js` - Main continuous monitoring daemon that checks for new epochs every minute
- `backfiller.js` - Historical data collection with Discord webhook integration  
- `selectiveRun.js` - One-time epoch processing script
- `lib/rewardExtractor.js` - Core API interface to Beaconcha.in for fetching validator data
- `lib/rewardSave.js` - Firebase Firestore database operations
- `invoicing/invoice.py` - Python-based reward calculation and invoice generation

**Data Flow:**
1. Validator indices read from CSV files in `data/` directory
2. Beaconcha.in API calls made in chunks of 100 validators
3. Rewards data saved to Firebase Firestore collections
4. Python scripts process stored data for invoicing with LEB (Liquid Ethereum Bonds) adjustments

## Development Commands

**Node.js:**
- `npm start` - Run main monitoring daemon with nodemon
- `node index.js` - Direct execution of main monitor
- `node selectiveRun.js <epoch_number>` - Process specific epoch
- `node backfiller.js` - Run historical backfilling
- `./run_backfiller.sh` - Production backfiller script (sets environment and runs backfiller + rplNodeUpdater)

**Python (invoicing):**
- `cd invoicing && python invoice.py` - Generate invoices from collected data
- Uses virtual environment at `invoicing/venv/`
- Requirements in `invoicing/requirements.txt`

## Environment Configuration

Copy `.env.dist` to `.env` and configure:
- `API_KEY` - Beaconcha.in API key (required)
- `EPOCH_START` - Starting epoch for monitoring
- `EPOCH_INTERVAL` - Epochs between checks (default: 100)  
- `DEBUG` - Enable verbose logging
- `DISCORD_KEY` & `DISCORD_CHANNEL` - For Discord notifications
- `CLIENT_URL` - Ethereum client endpoint
- `REWARD_COLLECTION` - Firebase collection name

## Validator Data Format

Validator CSV files in `data/` directory require format:
`Index,Pubkey,Type,Node,Minipool address`

The system primarily uses the validator index. The Type field is used in Python invoicing for LEB (Liquid Ethereum Bond) reward adjustments:
- Type 8-14: LEB8 (14% commission on borrowed portion)
- Type 16: LEB16 (15% commission on borrowed portion)

## Firebase Integration

Uses Firebase Admin SDK with service account key (`serviceAccountKey.json`). Data structure:
- Each epoch stored as document with epoch number as document ID
- Contains `withdrawals`, `proposals`, and `mev_data` arrays
- Documents grouped by 100-epoch intervals for Python processing