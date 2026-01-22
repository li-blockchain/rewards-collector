#!/usr/bin/env python3
"""
Ethereum Validator Rewards Collector

A Python implementation to extract validator rewards using the Beaconcha.in API
and save them to Parquet files for efficient columnar storage.

This replaces the Node.js implementation with better performance and
parquet output instead of Firebase.
"""

import os
import sys
import csv
import json
import logging
import argparse
import asyncio
import time
from typing import List, Dict, Any, Tuple, Optional
from datetime import datetime, timezone
import requests
import pandas as pd
from pathlib import Path

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ValidatorReader:
    """Reads and manages validator data from CSV files."""

    def __init__(self, csv_path: str):
        self.csv_path = csv_path
        self.validators = []

    def load_validators(self) -> List[Dict[str, str]]:
        """Load validators from CSV file, skipping header row."""
        try:
            with open(self.csv_path, 'r') as file:
                reader = csv.reader(file)
                next(reader)  # Skip header row

                self.validators = []
                for row in reader:
                    if len(row) >= 4 and row[0].strip():  # Skip empty rows
                        self.validators.append({
                            'index': row[0].strip(),
                            'pubkey': row[1].strip() if len(row) > 1 else '',
                            'type': row[2].strip() if len(row) > 2 else '',
                            'node': row[3].strip() if len(row) > 3 else '',
                            'minipool': row[4].strip() if len(row) > 4 else ''
                        })

            logger.info(f"Loaded {len(self.validators)} validators from {self.csv_path}")
            return self.validators

        except FileNotFoundError:
            logger.error(f"Validator CSV file not found: {self.csv_path}")
            raise
        except Exception as e:
            logger.error(f"Error reading validator file: {e}")
            raise

    def chunk_validators(self, chunk_size: int = 100) -> List[List[str]]:
        """Split validators into chunks of indices for API calls."""
        if not self.validators:
            self.load_validators()

        chunks = []
        for i in range(0, len(self.validators), chunk_size):
            chunk = self.validators[i:i + chunk_size]
            # Extract just the indices for API calls
            chunk_indices = [v['index'] for v in chunk]
            chunks.append(chunk_indices)

        logger.info(f"Created {len(chunks)} chunks of validators")
        return chunks

    def get_validator_by_index(self, index: str) -> Optional[Dict[str, str]]:
        """Get validator metadata by index."""
        for validator in self.validators:
            if validator['index'] == index:
                return validator
        return None


class BeaconchainAPI:
    """HTTP client for Beaconcha.in API calls with rate limiting."""

    def __init__(self, api_key: str, rate_limit_per_second: float = 8.0):
        self.api_key = api_key
        self.base_url = "https://beaconcha.in/api/v1"
        self.session = requests.Session()
        self.session.headers.update({'apikey': api_key})
        self.rate_limit_per_second = rate_limit_per_second
        self.last_request_time = 0.0

    def _wait_for_rate_limit(self):
        """Ensure we don't exceed rate limits."""
        if self.rate_limit_per_second <= 0:
            return

        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        min_interval = 1.0 / self.rate_limit_per_second

        if time_since_last < min_interval:
            sleep_time = min_interval - time_since_last
            time.sleep(sleep_time)

        self.last_request_time = time.time()

    def get_withdrawals(self, validator_indices: List[str], epoch: int) -> Dict[str, Any]:
        """Get withdrawals for validators. API returns last 100 epochs from given epoch."""
        validators_str = ','.join(validator_indices)

        # API returns last 100 epochs from the given epoch
        # To get withdrawals for epochs N to N+99, we need to query epoch N+99
        query_epoch = epoch + 99

        url = f"{self.base_url}/validator/{validators_str}/withdrawals"
        params = {'epoch': query_epoch}

        logger.info(f"⛏️  Fetching withdrawals for {len(validator_indices)} validators")
        logger.info(f"   🎯 Target epoch range: {epoch}-{epoch+99}")
        logger.info(f"   📅 Querying API at epoch: {query_epoch} (returns epochs {epoch}-{query_epoch})")

        try:
            self._wait_for_rate_limit()
            response = self.session.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logger.error(f"Error fetching withdrawals: {e}")
            raise

    def get_proposals(self, validator_indices: List[str], epoch: int) -> Dict[str, Any]:
        """Get proposals for validators at specific epoch."""
        validators_str = ','.join(validator_indices)
        url = f"{self.base_url}/validator/{validators_str}/proposals"
        params = {'epoch': epoch}

        logger.info(f"💰 Fetching proposals for {len(validator_indices)} validators at epoch {epoch}")

        try:
            self._wait_for_rate_limit()
            response = self.session.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logger.error(f"Error fetching proposals: {e}")
            raise

    def get_execution_block(self, block_number: int) -> Dict[str, Any]:
        """Get execution block data for MEV information."""
        url = f"https://beaconcha.in/api/v1/execution/block/{block_number}"

        try:
            self._wait_for_rate_limit()
            response = self.session.get(url)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logger.error(f"Error fetching execution block {block_number}: {e}")
            raise

    def get_epoch_slots(self, epoch: int) -> Dict[str, Any]:
        """Get slots data for an epoch (for timestamp information)."""
        url = f"{self.base_url}/epoch/{epoch}/slots"

        try:
            self._wait_for_rate_limit()
            response = self.session.get(url)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logger.error(f"Error fetching epoch {epoch} slots: {e}")
            raise

    def get_latest_epoch(self, client_url: str = "http://libc-prod2:5052") -> int:
        """Get latest finalized epoch from Ethereum client."""
        url = f"{client_url}/eth/v1/beacon/states/finalized/finality_checkpoints"

        try:
            response = requests.get(url, headers={'Accept': 'application/json'})
            response.raise_for_status()
            data = response.json()
            return int(data['data']['finalized']['epoch'])
        except requests.RequestException as e:
            logger.error(f"Error fetching latest epoch: {e}")
            return 0

    def get_validator_statuses(self, validator_indices: List[str], client_url: str = "http://libc-prod2:5052") -> Dict[str, str]:
        """
        Get validator statuses from beacon node RPC.

        Queries the beacon node to determine validator status (active, exited, etc.)
        Used to identify exit withdrawals vs regular reward withdrawals.

        Args:
            validator_indices: List of validator index strings to query
            client_url: Beacon node RPC URL

        Returns:
            Dict mapping validator index (str) to status string.
            Status values include: 'pending_initialized', 'pending_queued',
            'active_ongoing', 'active_exiting', 'active_slashed',
            'exited_unslashed', 'exited_slashed', 'withdrawal_possible',
            'withdrawal_done'
        """
        if not validator_indices:
            return {}

        statuses = {}

        # Beacon API supports POST with list of validator IDs for batch queries
        url = f"{client_url}/eth/v1/beacon/states/head/validators"

        try:
            # Use POST with ids in request body for efficient batch query
            response = requests.post(
                url,
                headers={'Accept': 'application/json', 'Content-Type': 'application/json'},
                json={'ids': validator_indices}
            )
            response.raise_for_status()
            data = response.json()

            if data.get('data'):
                for validator in data['data']:
                    index = str(validator['index'])
                    status = validator['status']
                    statuses[index] = status

            logger.info(f"📊 Fetched status for {len(statuses)} validators from beacon node")

            # Log summary of statuses found
            status_counts = {}
            for status in statuses.values():
                status_counts[status] = status_counts.get(status, 0) + 1
            if status_counts:
                logger.debug(f"   Status breakdown: {status_counts}")

            return statuses

        except requests.RequestException as e:
            logger.error(f"Error fetching validator statuses: {e}")
            # Return empty dict on error - caller should handle gracefully
            return {}

    def is_validator_exited(self, status: str) -> bool:
        """
        Check if a validator status indicates the validator has exited.

        Args:
            status: Validator status string from beacon API

        Returns:
            True if validator has exited (withdrawal is principal, not just rewards)
        """
        exited_statuses = {
            'exited_unslashed',
            'exited_slashed',
            'withdrawal_possible',
            'withdrawal_done'
        }
        return status in exited_statuses


class RewardProcessor:
    """Processes raw API data into clean, flattened structures."""

    def __init__(self, validator_reader: ValidatorReader, api: BeaconchainAPI):
        self.validator_reader = validator_reader
        self.api = api

    async def process_withdrawals(self, withdrawals_data: Dict[str, Any], epoch: int,
                                   validator_statuses: Optional[Dict[str, str]] = None) -> List[Dict[str, Any]]:
        """Process withdrawal data into flattened records.

        Args:
            withdrawals_data: Raw withdrawal data from Beaconcha.in API
            epoch: The epoch number being processed
            validator_statuses: Optional dict mapping validator index to status string.
                               Used to flag exit withdrawals.
        """
        if not withdrawals_data.get('data'):
            return []

        if validator_statuses is None:
            validator_statuses = {}

        # Get epoch slots for timestamp
        try:
            slots_data = self.api.get_epoch_slots(epoch)
            timestamp = None
            if slots_data.get('data'):
                last_slot = slots_data['data'][-1]
                timestamp = last_slot.get('exec_timestamp')
        except Exception as e:
            logger.warning(f"Could not get epoch slots for {epoch}: {e}")
            timestamp = None

        processed_withdrawals = []
        exit_count = 0
        for withdrawal in withdrawals_data['data']:
            validator_index = str(withdrawal['validatorindex'])
            validator_info = self.validator_reader.get_validator_by_index(validator_index)

            # Check if this withdrawal is from an exited validator
            status = validator_statuses.get(validator_index, '')
            is_exit = self.api.is_validator_exited(status)
            if is_exit:
                exit_count += 1

            record = {
                'record_type': 'withdrawal',
                'validator_index': withdrawal['validatorindex'],
                'amount': withdrawal['amount'],
                'epoch': withdrawal['epoch'],
                'datetime': timestamp,
                'validator_type': validator_info['type'] if validator_info else '',
                'node': validator_info['node'] if validator_info else '',
                'minipool': validator_info['minipool'] if validator_info else '',
                # Proposal-specific fields (null for withdrawals)
                'mev_source': None,
                'exec_block_number': None,
                # Exit flag for proper reward calculation
                'is_exit': is_exit
            }
            processed_withdrawals.append(record)

        if exit_count > 0:
            logger.info(f"🚪 Found {exit_count} exit withdrawals in epoch {epoch}")

        return processed_withdrawals

    async def process_proposals(self, proposals_data: Dict[str, Any], epoch: int) -> List[Dict[str, Any]]:
        """Process proposal data with MEV information."""
        if not proposals_data.get('data'):
            return []

        processed_proposals = []
        for proposal in proposals_data['data']:
            if not proposal.get('exec_block_number'):
                logger.info("No block number found for proposal, skipping")
                continue

            try:
                # Get execution block data for MEV info
                block_data = self.api.get_execution_block(proposal['exec_block_number'])

                if not block_data.get('data'):
                    continue

                block_info = block_data['data'][0]
                validator_info = self.validator_reader.get_validator_by_index(str(block_info['posConsensus']['proposerIndex']))

                # Determine MEV source and amount
                if block_info.get('relay'):
                    logger.info(f"🚪 Block MEV relay found for block {proposal['exec_block_number']}: {block_info['relay']['tag']}")
                    mev_source = block_info['relay']['tag']
                    amount = block_info['blockMevReward']
                else:
                    logger.info(f"🚫 No MEV relay found for block {proposal['exec_block_number']}")
                    mev_source = ''
                    amount = block_info['producerReward']

                record = {
                    'record_type': 'proposal',
                    'validator_index': block_info['posConsensus']['proposerIndex'],
                    'amount': amount,
                    'epoch': block_info['posConsensus']['epoch'],
                    'datetime': block_info['timestamp'],
                    'validator_type': validator_info['type'] if validator_info else '',
                    'node': validator_info['node'] if validator_info else '',
                    'minipool': validator_info['minipool'] if validator_info else '',
                    # Proposal-specific fields
                    'mev_source': mev_source,
                    'exec_block_number': proposal['exec_block_number'],
                    # Proposals are never exits
                    'is_exit': False
                }
                processed_proposals.append(record)

            except Exception as e:
                logger.error(f"Error processing proposal for block {proposal['exec_block_number']}: {e}")
                continue

        return processed_proposals


class ParquetWriter:
    """Handles writing reward data to Parquet files."""

    def __init__(self, output_dir: str):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)

    def save_rewards(self, withdrawals: List[Dict[str, Any]], proposals: List[Dict[str, Any]], epoch: int):
        """Save both withdrawals and proposals to a single master parquet file."""
        all_records = []

        # Combine withdrawals and proposals
        all_records.extend(withdrawals)
        all_records.extend(proposals)

        if not all_records:
            logger.info("No reward data to save")
            return

        # Create DataFrame with consistent schema
        new_df = pd.DataFrame(all_records)

        # Ensure consistent column order
        column_order = [
            'record_type',
            'validator_index',
            'amount',
            'epoch',
            'datetime',
            'validator_type',
            'node',
            'minipool',
            'mev_source',
            'exec_block_number',
            'is_exit'
        ]

        # Reorder columns and fill any missing ones with None
        for col in column_order:
            if col not in new_df.columns:
                new_df[col] = None

        new_df = new_df[column_order]

        # Master parquet file path
        master_filepath = self.output_dir / "rewards_master.parquet"

        # Append to existing file or create new one
        if master_filepath.exists():
            # Read existing data
            existing_df = pd.read_parquet(master_filepath)

            # Remove any existing records for this epoch to avoid duplicates
            existing_df = existing_df[existing_df['epoch'] != epoch]

            # Combine with new data
            combined_df = pd.concat([existing_df, new_df], ignore_index=True)

            # Deduplicate based on key columns to ensure no duplicates
            # This handles cases where duplicates might exist from previous runs
            key_columns = ['epoch', 'validator_index', 'record_type', 'amount']
            before_dedup = len(combined_df)
            combined_df = combined_df.drop_duplicates(subset=key_columns, keep='first')
            after_dedup = len(combined_df)

            if before_dedup != after_dedup:
                logger.warning(f"⚠️  Removed {before_dedup - after_dedup} duplicate records during save")

            # Sort by epoch for better organization
            combined_df = combined_df.sort_values(['epoch', 'record_type', 'validator_index'])
        else:
            combined_df = new_df

        # Save back to master file
        combined_df.to_parquet(master_filepath, index=False)

        withdrawal_count = len(withdrawals)
        proposal_count = len(proposals)
        total_count = len(all_records)
        total_records = len(combined_df)

        logger.info(f"💾 Appended {total_count} reward records to {master_filepath}")
        logger.info(f"   📥 Withdrawals: {withdrawal_count}")
        logger.info(f"   📤 Proposals: {proposal_count}")
        logger.info(f"   📊 Total records in master file: {total_records}")


class RewardsCollector:
    """Main rewards collection orchestrator."""

    def __init__(self, config: Dict[str, str]):
        self.config = config
        self.validator_reader = ValidatorReader(config['validator_csv'])
        self.api = BeaconchainAPI(config['api_key'])
        self.processor = RewardProcessor(self.validator_reader, self.api)
        self.writer = ParquetWriter(config['output_dir'])

    async def collect_rewards(self, epoch: int) -> Tuple[int, int]:
        """Collect rewards for a specific epoch across all validator chunks."""
        logger.info(f"\n{'='*50}")
        logger.info(f"🔍 Starting reward collection for epoch {epoch}")
        logger.info(f"{'='*50}")

        validator_chunks = self.validator_reader.chunk_validators()
        total_withdrawals = 0
        total_proposals = 0

        all_withdrawals = []
        all_proposals = []

        for i, chunk in enumerate(validator_chunks, 1):
            logger.info(f"\n📦 Processing chunk {i}/{len(validator_chunks)} ({len(chunk)} validators)")

            try:
                # Get validator statuses to identify exits
                validator_statuses = self.api.get_validator_statuses(chunk)

                # Get withdrawals for this chunk
                withdrawals_data = self.api.get_withdrawals(chunk, epoch)
                processed_withdrawals = await self.processor.process_withdrawals(
                    withdrawals_data, epoch, validator_statuses
                )
                all_withdrawals.extend(processed_withdrawals)

                # Get proposals for this chunk
                proposals_data = self.api.get_proposals(chunk, epoch)
                processed_proposals = await self.processor.process_proposals(proposals_data, epoch)
                all_proposals.extend(processed_proposals)

            except Exception as e:
                logger.error(f"Error processing chunk {i}: {e}")
                continue

        # Save all data to single parquet file
        self.writer.save_rewards(all_withdrawals, all_proposals, epoch)

        total_withdrawals = len(all_withdrawals)
        total_proposals = len(all_proposals)

        logger.info(f"\n✅ Completed epoch {epoch}: {total_withdrawals} withdrawals, {total_proposals} proposals")
        return total_withdrawals, total_proposals


def main():
    """Main entry point."""
    # Load .env file
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass

    parser = argparse.ArgumentParser(description='Ethereum Validator Rewards Collector')
    parser.add_argument('epoch', type=int, help='Epoch number to collect rewards for')
    parser.add_argument('--csv', default='../data/validators_updated.csv',
                       help='Path to validator CSV file')
    parser.add_argument('--output', default='./rewards_data',
                       help='Output directory for parquet files')
    parser.add_argument('--api-key', help='Beaconcha.in API key (or use API_KEY env var)')

    args = parser.parse_args()

    # Get API key from args or environment
    api_key = args.api_key or os.getenv('API_KEY')
    if not api_key:
        logger.error("API key required. Use --api-key or set API_KEY environment variable.")
        sys.exit(1)

    # Configuration
    config = {
        'validator_csv': args.csv,
        'output_dir': args.output,
        'api_key': api_key
    }

    # Create and run collector
    collector = RewardsCollector(config)

    try:
        import asyncio
        withdrawals, proposals = asyncio.run(collector.collect_rewards(args.epoch))

        print(f"\n🎉 Successfully collected rewards for epoch {args.epoch}")
        print(f"   📥 Withdrawals: {withdrawals}")
        print(f"   📤 Proposals: {proposals}")

    except KeyboardInterrupt:
        logger.info("Collection interrupted by user")
    except Exception as e:
        logger.error(f"Collection failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()