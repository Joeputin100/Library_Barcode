#!/usr/bin/env python3
"""
Babysitter process to monitor enrichment engine and restart if needed.
Checks every 10 minutes and restarts if process is not running.
"""

import subprocess
import time
import logging
import os
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('babysitter.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def is_enrichment_running():
    """Check if enrichment process is running in tmux"""
    try:
        # Check if tmux session exists and has active processes
        result = subprocess.run(
            ['tmux', 'list-sessions'],
            capture_output=True, text=True, timeout=10
        )
        return 'enrichment:' in result.stdout
    except (subprocess.TimeoutExpired, subprocess.CalledProcessError, FileNotFoundError):
        return False

def restart_enrichment():
    """Restart the enrichment process"""
    try:
        # Kill any existing tmux session
        subprocess.run(['tmux', 'kill-session', '-t', 'enrichment'], 
                      timeout=10, capture_output=True)
        
        # Wait a moment
        time.sleep(2)
        
        # Start new enrichment session
        subprocess.run([
            'tmux', 'new-session', '-d', '-s', 'enrichment', 
            'python3', 'parallel_mangle_processor.py'
        ], timeout=10, capture_output=True)
        
        logger.info("✅ Enrichment process restarted successfully")
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to restart enrichment process: {e}")
        return False

def main():
    """Main babysitter loop"""
    logger.info("🚀 Starting enrichment babysitter process")
    logger.info("👀 Will check every 10 minutes and restart if needed")
    
    check_interval = 600  # 10 minutes
    
    while True:
        try:
            if not is_enrichment_running():
                logger.warning("⚠️  Enrichment process not running! Attempting restart...")
                restart_enrichment()
            else:
                logger.info("✅ Enrichment process is running normally")
            
            # Wait for next check
            time.sleep(check_interval)
            
        except KeyboardInterrupt:
            logger.info("🛑 Babysitter process stopped by user")
            break
        except Exception as e:
            logger.error(f"❌ Unexpected error in babysitter: {e}")
            time.sleep(60)  # Wait 1 minute before retrying on error

if __name__ == "__main__":
    main()