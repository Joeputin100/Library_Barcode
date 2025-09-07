#!/usr/bin/env python3
"""
Real-time Vertex AI Batch Processing Monitor
Provides live updates with ANSI line rewriting for clean terminal output
"""

import sqlite3
import time
from datetime import datetime, timedelta
import sys
import os

def clear_lines(num_lines=1):
    """Clear specified number of lines using ANSI escape codes"""
    for _ in range(num_lines):
        sys.stdout.write('\033[F')  # Move cursor up one line
        sys.stdout.write('\033[K')  # Clear line

def get_batch_progress():
    """Get current batch processing progress from database"""
    
    conn = sqlite3.connect('review_app/data/reviews.db')
    cursor = conn.cursor()
    
    # Get total records
    cursor.execute('SELECT COUNT(*) FROM records')
    total_records = cursor.fetchone()[0]
    
    # Get records with Vertex AI research
    cursor.execute('''
        SELECT COUNT(*) FROM records 
        WHERE enhanced_description LIKE '%VERTEX AI RESEARCH%'
    ''')
    processed_records = cursor.fetchone()[0]
    
    # Get critical field completion
    cursor.execute('''
        SELECT 
            SUM(CASE WHEN series_volume != '' AND series_volume != 'None' THEN 1 ELSE 0 END) as series_volume_complete,
            SUM(CASE WHEN description != '' AND description != 'None' THEN 1 ELSE 0 END) as description_complete,
            SUM(CASE WHEN publisher != '' AND publisher != 'None' THEN 1 ELSE 0 END) as publisher_complete
        FROM records
    ''')
    
    series_volume, description, publisher = cursor.fetchone()
    
    # Get last processed record timestamp (use created_at)
    cursor.execute('''
        SELECT MAX(created_at) FROM records 
        WHERE enhanced_description LIKE '%VERTEX AI RESEARCH%'
    ''')
    last_processed_time = cursor.fetchone()[0]
    
    conn.close()
    
    return {
        'total_records': total_records,
        'processed_records': processed_records,
        'pending_records': total_records - processed_records,
        'series_volume_complete': series_volume,
        'description_complete': description,
        'publisher_complete': publisher,
        'last_processed_time': last_processed_time,
        'completion_percentage': (processed_records / total_records * 100) if total_records > 0 else 0
    }

def format_time_delta(timestamp_str):
    """Format time difference for display"""
    if not timestamp_str:
        return "Never"
    
    try:
        last_time = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
        now = datetime.now().astimezone()
        delta = now - last_time
        
        if delta.total_seconds() < 60:
            return f"{int(delta.total_seconds())}s ago"
        elif delta.total_seconds() < 3600:
            return f"{int(delta.total_seconds() / 60)}m ago"
        else:
            return f"{int(delta.total_seconds() / 3600)}h ago"
    except:
        return "Unknown"

def display_progress(progress):
    """Display progress with ANSI formatting"""
    
    # Clear screen and move to top
    print("\033[2J\033[H", end="")
    
    # Display header
    print("🔍 Vertex AI Batch Processing Monitor")
    print("=" * 50)
    
    # Records progress
    print(f"📊 Records: {progress['processed_records']}/{progress['total_records']} "
          f"({progress['completion_percentage']:.1f}%)")
    print(f"⏳ Pending: {progress['pending_records']} records")
    
    # Critical fields
    print(f"📚 Critical Fields:")
    print(f"   • Publisher: {progress['publisher_complete']}/{progress['total_records']} "
          f"({progress['publisher_complete']/progress['total_records']*100:.1f}%)")
    print(f"   • Description: {progress['description_complete']}/{progress['total_records']} "
          f"({progress['description_complete']/progress['total_records']*100:.1f}%)")
    print(f"   • Series Volume: {progress['series_volume_complete']}/{progress['total_records']} "
          f"({progress['series_volume_complete']/progress['total_records']*100:.1f}%)")
    
    # Last processed
    last_time = format_time_delta(progress['last_processed_time'])
    print(f"🕒 Last processed: {last_time}")
    
    # Progress bar (simplified)
    bar_length = 30
    filled = int(bar_length * progress['completion_percentage'] / 100)
    bar = "█" * filled + "░" * (bar_length - filled)
    print(f"[{bar}] {progress['completion_percentage']:.1f}%")

def monitor_loop(update_interval=5):
    """Main monitoring loop"""
    
    try:
        while True:
            progress = get_batch_progress()
            display_progress(progress)
            
            # Check if processing is complete
            if progress['processed_records'] >= progress['total_records']:
                print("\n🎉 BATCH PROCESSING COMPLETE!")
                break
                
            time.sleep(update_interval)
            
    except KeyboardInterrupt:
        print("\n\n🛑 Monitoring stopped")
    except Exception as e:
        print(f"\n\n❌ Monitoring error: {e}")

if __name__ == "__main__":
    print("Starting Vertex AI Batch Processing Monitor...")
    print("Press Ctrl+C to stop monitoring")
    time.sleep(2)
    monitor_loop()