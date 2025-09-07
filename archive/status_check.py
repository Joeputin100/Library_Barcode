#!/usr/bin/env python3
"""
Simple one-time status check for enrichment processing
"""

import json
from datetime import datetime

def main():
    try:
        with open('processing_state_full.json', 'r') as f:
            state = json.load(f)
        
        total = state.get('total_records', 808)
        processed = state.get('records_processed', 0)
        batches = state.get('batches_completed', 0)
        status = state.get('status', 'processing')
        
        print("🔍 ENRICHMENT PROCESSING STATUS")
        print("=" * 40)
        print(f"Status: {status.upper()}")
        print(f"Total Records: {total:,}")
        print(f"Processed: {processed:,}")
        print(f"Remaining: {total - processed:,}")
        print(f"Progress: {processed/total*100:.1f}%")
        print(f"Batches Completed: {batches}")
        
        # Source breakdown
        if 'source_counts' in state:
            print("\n📊 SOURCE BREAKDOWN:")
            for source, count in state['source_counts'].items():
                source_name = source.upper().replace('_', ' ')
                estimated = int((count / total) * processed)
                print(f"  {source_name}: {estimated:,}/{count:,}")
        
        # Quality info
        if 'quality_scores' in state and state['quality_scores']:
            scores = state['quality_scores']
            avg = sum(scores) / len(scores)
            print(f"\n🎯 QUALITY: {avg:.3f} average")
            print(f"   Samples: {len(scores)} records scored")
        
        # Timing
        if 'start_time' in state:
            start = datetime.fromisoformat(state['start_time'].replace('Z', '+00:00'))
            duration = datetime.now() - start
            rate = processed / duration.total_seconds() * 60 if duration.total_seconds() > 0 else 0
            remaining = (total - processed) / rate if rate > 0 else 0
            
            print(f"\n⏰ TIMING:")
            print(f"   Running: {str(duration).split('.')[0]}")
            print(f"   Rate: {rate:.1f} records/minute")
            print(f"   ETA: {remaining:.1f} minutes")
        
        print("\n" + "=" * 40)
        
    except FileNotFoundError:
        print("Processing state file not found. Processing may not have started.")
    except Exception as e:
        print(f"Error reading status: {e}")

if __name__ == "__main__":
    main()