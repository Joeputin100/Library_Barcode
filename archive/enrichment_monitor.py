#!/usr/bin/env python3
"""
Enrichment Monitor - Runs alongside enrichment process to provide real-time CSV updates
"""

import time
import subprocess
import os
from datetime import datetime

def monitor_enrichment():
    """Monitor enrichment process and update CSV periodically"""
    
    print("🔍 Starting Enrichment Monitor")
    print("📊 Will update CSV every 5 minutes while enrichment runs")
    print("💾 CSV saved to: ~/storage/shared/Download/enrichment_status.csv")
    
    update_interval = 300  # 5 minutes
    
    while True:
        try:
            # Check if enrichment process is still running
            result = subprocess.run(
                ['ps', 'aux'], 
                capture_output=True, text=True, timeout=10
            )
            
            if 'python3 parallel_mangle_processor.py' not in result.stdout:
                print("⚠️  Enrichment process not found. Exiting monitor.")
                break
            
            # Update CSV
            print(f"\n🔄 Updating CSV at {datetime.now().strftime('%H:%M:%S')}")
            subprocess.run(['python3', 'enrichment_status_corrected.py'], timeout=60)
            
            # Wait for next update
            time.sleep(update_interval)
            
        except subprocess.TimeoutExpired:
            print("⏰ CSV update timed out, will retry next cycle")
            time.sleep(60)
        except KeyboardInterrupt:
            print("\n🛑 Monitor stopped by user")
            break
        except Exception as e:
            print(f"❌ Error in monitor: {e}")
            time.sleep(60)  # Wait 1 minute before retrying

if __name__ == "__main__":
    monitor_enrichment()