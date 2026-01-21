#!/usr/bin/env python3
"""
Simple CDP Health Monitor Script
This script can be run as a cron job to monitor CDP position health and send alerts via Discord webhook.
"""

import os
import sys
import requests
from dotenv import load_dotenv
from commands.cdp import check_cdp_health
import logging

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

def send_discord_webhook(webhook_url: str, message: str):
    """Send message to Discord via webhook"""
    try:
        payload = {
            "content": message,
            "username": "CDP Monitor Bot"
        }
        
        response = requests.post(webhook_url, json=payload, timeout=10)
        response.raise_for_status()
        
        logger.info("Alert sent to Discord webhook successfully")
        return True
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Error sending Discord webhook: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error sending webhook: {e}")
        return False

def main():
    """Main function to run the CDP monitor"""
    try:
        # Get webhook URL from environment
        webhook_url = os.getenv('DISCORD_WEBHOOK_URL')
        if not webhook_url:
            logger.error("DISCORD_WEBHOOK_URL environment variable not set")
            sys.exit(1)
        
        logger.info("Checking CDP position health...")
        
        # Check position health
        health_result = check_cdp_health()
        
        if health_result['alert']:
            # Send alert
            alert_message = f"🚨 **CDP Health Alert** 🚨\n\n{health_result['message']}"
            
            if health_result['severity'] == 'error':
                alert_message = f"❌ **CDP Monitoring Error** ❌\n\n{health_result['message']}"
            
            success = send_discord_webhook(webhook_url, alert_message)
            if success:
                logger.warning(f"CDP alert sent: {health_result['message']}")
            else:
                logger.error("Failed to send CDP alert")
                sys.exit(1)
        else:
            logger.info("CDP position is healthy")
        
        logger.info("CDP monitoring completed successfully")
        
    except Exception as e:
        error_message = f"❌ **CDP Monitor Error** ❌\n\nError checking CDP health: {str(e)}"
        
        # Try to send error alert
        webhook_url = os.getenv('DISCORD_WEBHOOK_URL')
        if webhook_url:
            send_discord_webhook(webhook_url, error_message)
        
        logger.error(f"Error in CDP health check: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 