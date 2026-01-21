#!/usr/bin/env python3
"""
Ethereum Validator Rewards Backfiller

Historical data collection script that fills in missing reward data.
Saves rewards data to efficient Parquet files for analytics.
"""

import os
import sys
import time
import asyncio
import logging
import argparse
from datetime import datetime
from pathlib import Path

from rewards_collector import RewardsCollector, BeaconchainAPI

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class RewardsBackfiller:
    """Historical rewards data backfiller."""

    def __init__(self, config):
        self.config = config
        self.collector = RewardsCollector(config)
        self.api = BeaconchainAPI(config['api_key'])
        self.start_epoch = int(config.get('epoch_start', 0))
        self.epoch_interval = int(config.get('epoch_interval', 100))
        self.delay_seconds = int(config.get('backfill_delay', 15))
        self.data_dir = Path(config.get('data_dir', './'))
        self.last_epoch_file = self.data_dir / '.lastepoch'

    def load_last_epoch(self) -> int:
        """Load the last processed epoch from file."""
        if self.last_epoch_file.exists():
            try:
                last_epoch = int(self.last_epoch_file.read_text().strip())
                logger.info(f"📖 Using .lastepoch file: {last_epoch}")
                return last_epoch
            except (ValueError, IOError) as e:
                logger.error(f"❌ Error reading .lastepoch file: {e}")

        logger.info(f"📝 No .lastepoch found, starting from configured epoch: {self.start_epoch}")
        return self.start_epoch

    def save_last_epoch(self, epoch: int):
        """Save the last processed epoch to file."""
        try:
            self.last_epoch_file.write_text(str(epoch))
            logger.info(f"💾 Saved last epoch: {epoch}")
        except IOError as e:
            logger.error(f"❌ Error saving .lastepoch file: {e}")

    async def run(self):
        """Main backfilling loop."""
        start_time = datetime.now()

        logger.info("\n" + "="*50)
        logger.info("🔄 LIBC Validator Rewards Backfiller")
        logger.info("="*50)
        logger.info(f"📊 Processing {self.epoch_interval} epochs at a time")
        logger.info(f"⏱️  Delay between batches: {self.delay_seconds} seconds")

        # Load starting epoch (prioritize command line arg if provided)
        if hasattr(self, 'override_start_epoch') and self.override_start_epoch:
            next_epoch = self.override_start_epoch
            logger.info(f"🎯 Using command line epoch: {next_epoch}")
        else:
            next_epoch = self.load_last_epoch()

        logger.info(f"🎯 Starting backfill from epoch: {next_epoch}")

        while True:
            try:
                # Check current epoch
                current_epoch = self.api.get_latest_epoch()
                logger.info(f"📈 Current epoch: {current_epoch}")

                if next_epoch > current_epoch and current_epoch > 0:
                    # Backfill complete
                    self.save_last_epoch(next_epoch - self.epoch_interval)

                    duration = datetime.now() - start_time
                    logger.info(f"\n🎉 Backfill Complete on Epoch {next_epoch - self.epoch_interval}!")
                    logger.info(f"⏱️  Total time: {duration.total_seconds():.1f} seconds")

                    # Send Discord notification if configured
                    await self.send_discord_notification(
                        f"🎉 Backfill Complete on Epoch {next_epoch - self.epoch_interval}!\n"
                        f"⏱️ Total time: {duration.total_seconds():.1f} seconds"
                    )
                    break

                logger.info(f"\n🔍 Collecting rewards for epoch: {next_epoch}")

                # Collect rewards for this epoch
                withdrawals, proposals = await self.collector.collect_rewards(next_epoch)

                logger.info(f"✅ Processed epoch {next_epoch}: {withdrawals} withdrawals, {proposals} proposals")

                # Move to next epoch
                next_epoch += self.epoch_interval

                # Wait before processing next batch
                if self.delay_seconds > 0:
                    logger.info(f"⏳ Waiting {self.delay_seconds} seconds before next batch...")
                    await asyncio.sleep(self.delay_seconds)

            except KeyboardInterrupt:
                logger.info("\n👋 Backfill stopped by user")
                self.save_last_epoch(next_epoch)
                break
            except Exception as e:
                logger.error(f"❌ Error in backfill loop: {e}")
                logger.info(f"⏳ Waiting {self.delay_seconds} seconds before retry...")
                await asyncio.sleep(self.delay_seconds)

    async def send_discord_notification(self, message: str):
        """Send notification to Discord if configured."""
        discord_webhook = os.getenv('DISCORD_WEBHOOK_URL')
        if not discord_webhook:
            return

        try:
            import aiohttp

            async with aiohttp.ClientSession() as session:
                payload = {'content': message}
                async with session.post(discord_webhook, json=payload) as response:
                    if response.status == 204:
                        logger.info("📢 Discord notification sent")
                    else:
                        logger.warning(f"⚠️ Discord notification failed: {response.status}")

        except Exception as e:
            logger.error(f"❌ Error sending Discord notification: {e}")


def load_config():
    """Load configuration from environment variables."""
    # Try to load .env file
    env_file = Path(__file__).parent / '.env'
    if env_file.exists():
        try:
            from dotenv import load_dotenv
            load_dotenv(env_file)
        except ImportError:
            logger.warning("python-dotenv not installed, skipping .env file")

    api_key = os.getenv('API_KEY')
    if not api_key:
        logger.error("❌ API_KEY environment variable is required")
        sys.exit(1)

    return {
        'api_key': api_key,
        'validator_csv': os.getenv('VALIDATOR_CSV', '../data/validators_updated.csv'),
        'output_dir': os.getenv('OUTPUT_DIR', './rewards_data'),
        'data_dir': os.getenv('DATA_DIR', './'),
        'epoch_start': os.getenv('EPOCH_START', '0'),
        'epoch_interval': os.getenv('EPOCH_INTERVAL', '100'),
        'backfill_delay': os.getenv('BACKFILL_DELAY', '15'),
    }


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description='Ethereum Validator Rewards Backfiller')
    parser.add_argument('--start-epoch', type=int, help='Starting epoch (overrides config)')
    parser.add_argument('--delay', type=int, help='Delay between batches in seconds')

    args = parser.parse_args()

    try:
        config = load_config()

        # Override config with command line args
        if args.start_epoch:
            config['epoch_start'] = str(args.start_epoch)
        if args.delay:
            config['backfill_delay'] = str(args.delay)

        backfiller = RewardsBackfiller(config)

        # Set override epoch if provided via command line
        if args.start_epoch:
            backfiller.override_start_epoch = args.start_epoch

        logger.info("🎬 Starting rewards backfiller...")
        asyncio.run(backfiller.run())

    except KeyboardInterrupt:
        logger.info("\n👋 Goodbye!")
    except Exception as e:
        logger.error(f"❌ Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()