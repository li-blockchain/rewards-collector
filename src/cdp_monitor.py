#!/usr/bin/env python3
"""
CDP Health Monitor Script
This script can be run as a cron job to monitor CDP position health and send alerts.
"""

import os
import sys
import asyncio
import discord
from dotenv import load_dotenv
from commands.cdp import check_cdp_health
import logging
import signal

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('cdp_monitor.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

class CDPAlertBot:
    def __init__(self):
        self.token = os.getenv('DISCORD_BOT_TOKEN')
        self.alert_channel_id = int(os.getenv('CDP_ALERT_CHANNEL_ID', 0))
        
        if not self.token:
            raise ValueError("DISCORD_BOT_TOKEN environment variable not set")
        
        if not self.alert_channel_id:
            raise ValueError("CDP_ALERT_CHANNEL_ID environment variable not set")
        
        self.client = discord.Client(intents=discord.Intents.default())
        self._task_complete = False
        
    async def send_alert(self, message: str):
        """Send alert message to Discord channel"""
        try:
            channel = self.client.get_channel(self.alert_channel_id)
            if channel:
                await channel.send(message)
                logger.info(f"Alert sent to Discord channel {self.alert_channel_id}")
            else:
                logger.error(f"Could not find Discord channel {self.alert_channel_id}")
        except Exception as e:
            logger.error(f"Error sending Discord alert: {e}")
    
    async def check_and_alert(self):
        """Check CDP health and send alert if needed"""
        try:
            logger.info("Checking CDP position health...")
            
            # Check position health
            health_result = check_cdp_health()
            
            if health_result['alert']:
                # Send alert
                alert_message = f"🚨 **CDP Health Alert** 🚨\n\n{health_result['message']}"
                
                if health_result['severity'] == 'error':
                    alert_message = f"❌ **CDP Monitoring Error** ❌\n\n{health_result['message']}"
                
                await self.send_alert(alert_message)
                logger.warning(f"CDP alert sent: {health_result['message']}")
            else:
                logger.info("CDP position is healthy")
                
        except Exception as e:
            error_message = f"❌ **CDP Monitor Error** ❌\n\nError checking CDP health: {str(e)}"
            await self.send_alert(error_message)
            logger.error(f"Error in CDP health check: {e}")
        finally:
            # Mark task as complete
            self._task_complete = True
    
    async def run(self):
        """Run the alert bot"""
        try:
            # Set up the on_ready event handler
            @self.client.event
            async def on_ready():
                logger.info(f'CDP Monitor bot logged in as {self.client.user}')
                await self.check_and_alert()
                # Close the client after the task is complete
                await self.client.close()
            
            # Start the client
            await self.client.start(self.token)
            
        except Exception as e:
            logger.error(f"Error starting Discord client: {e}")
            raise

async def main():
    """Main function to run the CDP monitor"""
    bot = None
    try:
        bot = CDPAlertBot()
        await bot.run()
        
    except KeyboardInterrupt:
        logger.info("Received interrupt signal, shutting down...")
    except Exception as e:
        logger.error(f"Error in main: {e}")
        sys.exit(1)
    finally:
        # Ensure proper cleanup
        if bot and bot.client and not bot.client.is_closed():
            try:
                await bot.client.close()
                logger.info("Discord client closed successfully")
            except Exception as e:
                logger.error(f"Error closing Discord client: {e}")

def signal_handler(signum, frame):
    """Handle interrupt signals"""
    logger.info(f"Received signal {signum}, shutting down...")
    sys.exit(0)

if __name__ == "__main__":
    # Set up signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        # Run the async main function
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Script interrupted by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1) 