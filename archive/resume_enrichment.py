#!/usr/bin/env python3
"""
Script to resume the enrichment process from current state.
"""

import json
import os
import subprocess
import time

def check_enrichment_state():
    """Check current enrichment state"""
    if os.path.exists("enrichment_state.json"):
        try:
            with open("enrichment_state.json", "r") as f:
                state = json.load(f)
            
            print("=== CURRENT ENRICHMENT STATE ===")
            print(f"Status: {state.get('status', 'unknown')}")
            print(f"Processed: {state.get('processed_records', 0)}/{state.get('total_records', 0)} records")
            print(f"Last successful: {state.get('last_successful_barcode', 'None')}")
            print(f"Failures: {state.get('failed_records', 0)}")
            
            if state.get('micro_details'):
                print(f"\nLast 5 operations:")
                for detail in state['micro_details'][-5:]:
                    print(f"  {detail['timestamp']} - {detail['barcode']} - {detail['status']}")
            
            return state
            
        except Exception as e:
            print(f"Error reading state: {e}")
            return None
    else:
        print("No state file found. Starting fresh.")
        return None

def resume_enrichment():
    """Resume the enrichment process"""
    print("Resuming enrichment process...")
    
    # Check if we need to start from beginning or resume
    state = check_enrichment_state()
    
    if state and state.get('status') == 'completed':
        print("Enrichment already completed!")
        return
    
    # Start the robust enrichment process
    try:
        print("Starting robust enrichment process...")
        result = subprocess.run([
            "python", "robust_enricher.py"
        ], capture_output=True, text=True, timeout=3600)  # 1 hour timeout
        
        print("Enrichment process completed!")
        print(f"Return code: {result.returncode}")
        if result.stdout:
            print(f"Output: {result.stdout[-500:]}")  # Last 500 chars
        if result.stderr:
            print(f"Errors: {result.stderr[-500:]}")
            
    except subprocess.TimeoutExpired:
        print("Enrichment process timed out after 1 hour.")
        print("The process may have been interrupted. Check enrichment_state.json for current status.")
    except Exception as e:
        print(f"Error running enrichment process: {e}")

if __name__ == "__main__":
    resume_enrichment()