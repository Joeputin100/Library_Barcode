#!/usr/bin/env python3
"""
Enhanced persistent visualizer with continuous updates
Shows all 8 enrichment sources with proper progress tracking
"""
import time
import json
import os
import shutil
from datetime import datetime
from caching import load_cache

def analyze_enrichment_sources():
    """Analyze which enrichment sources have been used with detailed breakdown"""
    cache = load_cache()
    
    # Track usage by source type
    source_counts = {
        'GOOGLE_BOOKS': 0,
        'LIBRARY_OF_CONGRESS': 0, 
        'OPEN_LIBRARY': 0,
        'GOODREADS': 0,
        'LIBRARY_THING': 0,
        'WIKIPEDIA': 0,
        'ISBNDB': 0,
        'VERTEX_AI': 0
    }
    
    # Analyze cache entries
    for key, value in cache.items():
        # Check source patterns in cache keys
        if 'google' in key.lower():
            source_counts['GOOGLE_BOOKS'] += 1
        elif 'loc' in key.lower() or 'congress' in key.lower():
            source_counts['LIBRARY_OF_CONGRESS'] += 1
        elif 'open' in key.lower() and 'library' in key.lower():
            source_counts['OPEN_LIBRARY'] += 1
        elif 'goodreads' in key.lower():
            source_counts['GOODREADS'] += 1
        elif 'librarything' in key.lower() or 'lt_' in key.lower():
            source_counts['LIBRARY_THING'] += 1
        elif 'wikipedia' in key.lower() or 'wiki' in key.lower():
            source_counts['WIKIPEDIA'] += 1
        elif 'isbndb' in key.lower():
            source_counts['ISBNDB'] += 1
        elif 'vertex' in key.lower() or 'ai_' in key.lower():
            source_counts['VERTEX_AI'] += 1
    
    return source_counts, len(cache)

def create_progress_bar(percentage, width=40):
    """Create a visual progress bar"""
    filled = int(width * percentage / 100)
    bar = '█' * filled + '░' * (width - filled)
    return f"{bar} {percentage:.1f}%"

def get_terminal_dimensions():
    """Get current terminal dimensions (columns, rows)"""
    try:
        columns, rows = shutil.get_terminal_size()
        return columns, rows
    except:
        # Fallback to default dimensions
        return 80, 24

def display_enrichment_dashboard(source_counts, total_entries):
    """Display the enrichment source dashboard with dynamic terminal sizing"""
    total_possible = 808  # Total records to process
    
    # Get current terminal dimensions
    columns, rows = get_terminal_dimensions()
    
    # Calculate percentages
    percentages = {}
    for source, count in source_counts.items():
        if total_entries > 0:
            percentages[source] = (count / total_entries) * 100
        else:
            percentages[source] = 0
    
    # Calculate overall progress
    overall_progress = (total_entries / (total_possible * 8)) * 100  # 8 sources per record ideally
    overall_progress = min(overall_progress, 100)
    
    # Dynamic width based on terminal size
    separator_width = min(columns, 100)
    progress_bar_width = max(20, min(columns - 40, 50))  # Adjust progress bar width
    
    print("\n" + "=" * separator_width)
    print("🔍 ENRICHMENT SOURCE MONITOR - LIVE UPDATES".center(separator_width))
    print("=" * separator_width)
    print(f"📊 Overall Progress: {create_progress_bar(overall_progress, progress_bar_width)}")
    print(f"📦 Total Cache Entries: {total_entries}")
    print(f"🎯 Target: {total_possible} records × 8 sources = {total_possible * 8} total calls")
    print("-" * separator_width)
    print("🔧 ENRICHMENT SOURCE USAGE BREAKDOWN:".center(separator_width))
    print("-" * separator_width)
    
    # Display each source with progress bar
    for source, count in source_counts.items():
        percentage = percentages[source]
        progress = create_progress_bar(percentage, progress_bar_width - 10)
        # Abbreviate long source names
        display_source = "LOC" if source == "LIBRARY_OF_CONGRESS" else source
        # Adjust formatting based on terminal width
        if columns >= 100:
            print(f"{display_source:20} {progress:40} ({count} calls)")
        else:
            # Compact display for narrower terminals
            print(f"{display_source:15} {progress:30} ({count})")
    
    print("-" * separator_width)
    
    # Display current task information
    task_info = get_current_task()
    task_status = "🔄" if task_info["status"] == "processing" else "✅" if task_info["status"] == "completed" else "⏸️"
    
    print(f"📋 CURRENT TASK: {task_info['current_task']}")
    if task_info["total_records"] > 0:
        task_progress = (task_info["processed_records"] / task_info["total_records"]) * 100
        print(f"   {task_status} Progress: {task_info['processed_records']}/{task_info['total_records']} records ({task_progress:.1f}%)")
    
    status = "✅ COMPLETE" if overall_progress >= 99.9 else "🔄 PROCESSING"
    print(f"💡 STATUS: {status}")
    print(f"🕒 Last Update: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"📱 Terminal: {columns}×{rows}")
    print("=" * separator_width)
    print("Press Ctrl+C to exit | Updates every 3 seconds".center(separator_width))

def get_current_task():
    """Get information about the current enrichment task"""
    try:
        # Check if there's an active enrichment process
        if os.path.exists("enrichment_queue.txt"):
            with open("enrichment_queue.txt", "r") as f:
                queue_lines = [line.strip() for line in f if line.strip()]
            
            if queue_lines:
                total_records = len(queue_lines)
                processed = total_records - len([line for line in queue_lines if not line.startswith("#")])
                progress = (processed / total_records) * 100 if total_records > 0 else 0
                
                return {
                    "current_task": "Processing enrichment queue",
                    "processed_records": processed,
                    "total_records": total_records,
                    "progress_percentage": progress,
                    "status": "processing" if progress < 100 else "completed"
                }
        
        # Default task info
        return {
            "current_task": "Multi-source data enrichment",
            "processed_records": 0,
            "total_records": 808,
            "progress_percentage": 0,
            "status": "idle"
        }
        
    except Exception:
        # Fallback task info
        return {
            "current_task": "Data enrichment",
            "processed_records": 0,
            "total_records": 808,
            "progress_percentage": 0,
            "status": "unknown"
        }

def save_state(source_counts, total_entries):
    """Save current state to JSON file"""
    state = {
        "timestamp": datetime.now().isoformat(),
        "total_cache_entries": total_entries,
        "source_counts": source_counts,
        "active_sources": sum(1 for count in source_counts.values() if count > 0),
        "inactive_sources": sum(1 for count in source_counts.values() if count == 0),
        "overall_progress": (total_entries / (808 * 8)) * 100
    }
    
    with open("enrichment_state.json", "w") as f:
        json.dump(state, f, indent=2)

def main():
    """Main function for persistent monitoring"""
    print("🚀 Starting Enhanced Persistent Enrichment Monitor...")
    print("📡 Monitoring all 8 enrichment sources in real-time")
    print("💾 State will be saved to enrichment_state.json every update")
    print("Press Ctrl+C to exit\n")
    
    try:
        while True:
            # Get current source usage
            source_counts, total_entries = analyze_enrichment_sources()
            
            # Clear screen before redrawing
            os.system('cls' if os.name == 'nt' else 'clear')
            
            # Display dashboard
            display_enrichment_dashboard(source_counts, total_entries)
            
            # Save state
            save_state(source_counts, total_entries)
            
            # Wait before next update
            time.sleep(3)
            
    except KeyboardInterrupt:
        print("\n\n🛑 Monitoring stopped by user")
        print("📊 Final state saved to enrichment_state.json")
    except Exception as e:
        print(f"\n❌ Error in monitoring: {e}")

if __name__ == "__main__":
    main()