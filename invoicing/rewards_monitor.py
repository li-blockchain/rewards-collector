#!/usr/bin/env python3
"""
Ethereum Validator Rewards Monitor

Continuous monitoring script that checks for new epochs and collects rewards.
Similar to the Node.js index.js but using Python and parquet output.
"""

import os
import sys
import time
import asyncio
import logging
from datetime import datetime
from pathlib import Path

from rewards_collector import RewardsCollector, BeaconchainAPI

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class RewardsMonitor:
    """Continuous monitor for validator rewards."""

    def __init__(self, config):
        self.config = config
        self.collector = RewardsCollector(config)
        self.api = BeaconchainAPI(config['api_key'])
        self.start_epoch = int(config.get('epoch_start', 0))
        self.epoch_interval = int(config.get('epoch_interval', 100))
        self.check_interval = int(config.get('check_interval', 60))  # seconds

    async def run(self):
        """Main monitoring loop."""
        logger.info("\n" + "="*50)
        logger.info("🚀 LIBC Validator Rewards Monitor")
        logger.info("="*50)
        logger.info(f"📊 Checking every {self.epoch_interval} epochs for new rewards")
        logger.info(f"⏱️  Check interval: {self.check_interval} seconds")
        logger.info(f"🎯 Starting from epoch: {self.start_epoch}")

        current_start_epoch = self.start_epoch

        while True:
            try:
                logger.info(f"\n🔍 Checking for new epoch...")
                latest_epoch = self.api.get_latest_epoch()

                if latest_epoch < current_start_epoch:
                    logger.info(f"⏳ Not yet at starting epoch {current_start_epoch}. Current: {latest_epoch}. Waiting...")
                    await asyncio.sleep(self.check_interval)
                    continue

                logger.info(f"📈 Latest epoch: {latest_epoch}")

                # Collect rewards for this epoch range
                withdrawals, proposals = await self.collector.collect_rewards(current_start_epoch)

                # Set next epoch to check
                current_start_epoch = latest_epoch + self.epoch_interval
                logger.info(f"⏭️  Next check will be for epoch: {current_start_epoch}")

                # Wait before next check
                await asyncio.sleep(self.check_interval)

            except KeyboardInterrupt:
                logger.info("\n👋 Monitor stopped by user")
                break
            except Exception as e:
                logger.error(f"❌ Error in monitoring loop: {e}")
                logger.info(f"⏳ Waiting {self.check_interval} seconds before retry...")
                await asyncio.sleep(self.check_interval)


def load_config():
    """Load configuration from environment variables."""
    # Try to load .env file
    env_file = Path(__file__).parent / '.env'
    if env_file.exists():
        from dotenv import load_dotenv
        load_dotenv(env_file)

    api_key = os.getenv('API_KEY')
    if not api_key:
        logger.error("❌ API_KEY environment variable is required")
        sys.exit(1)

    return {
        'api_key': api_key,
        'validator_csv': os.getenv('VALIDATOR_CSV', '../data/validators_updated.csv'),
        'output_dir': os.getenv('OUTPUT_DIR', './rewards_data'),
        'epoch_start': os.getenv('EPOCH_WATCH_START', '0'),
        'epoch_interval': os.getenv('EPOCH_INTERVAL', '100'),
        'check_interval': os.getenv('CHECK_INTERVAL', '60'),
    }


def main():
    """Main entry point."""
    try:
        config = load_config()
        monitor = RewardsMonitor(config)

        logger.info("🎬 Starting rewards monitor...")
        asyncio.run(monitor.run())

    except KeyboardInterrupt:
        logger.info("\n👋 Goodbye!")
    except Exception as e:
        logger.error(f"❌ Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()