#!/usr/bin/env python3
"""
Simple Vertex AI Batch Processing Monitor
Run this in a separate terminal to watch progress
"""

import sqlite3
import time
from datetime import datetime

def get_progress():
    """Get current processing progress"""
    conn = sqlite3.connect('review_app/data/reviews.db')
    cursor = conn.cursor()
    
    # Total records
    cursor.execute('SELECT COUNT(*) FROM records')
    total = cursor.fetchone()[0]
    
    # Processed records
    cursor.execute('''
        SELECT COUNT(*) FROM records 
        WHERE enhanced_description LIKE '%VERTEX AI RESEARCH%'
    ''')
    processed = cursor.fetchone()[0]
    
    # Critical fields
    cursor.execute('''
        SELECT 
            SUM(CASE WHEN publisher != '' AND publisher != 'None' THEN 1 ELSE 0 END),
            SUM(CASE WHEN description != '' AND description != 'None' THEN 1 ELSE 0 END),
            SUM(CASE WHEN series_volume != '' AND series_volume != 'None' THEN 1 ELSE 0 END)
        FROM records
    ''')
    publisher, description, series_volume = cursor.fetchone()
    
    # Last processed time (use created_at since updated_at doesn't exist)
    cursor.execute('''
        SELECT MAX(created_at) FROM records 
        WHERE enhanced_description LIKE '%VERTEX AI RESEARCH%'
    ''')
    last_time = cursor.fetchone()[0]
    
    conn.close()
    
    return {
        'total': total,
        'processed': processed,
        'pending': total - processed,
        'pct_complete': (processed / total * 100) if total > 0 else 0,
        'publisher': publisher,
        'description': description,
        'series_volume': series_volume,
        'last_time': last_time
    }

def format_time(timestamp):
    """Format timestamp for display"""
    if not timestamp:
        return "Never"
    try:
        dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
        now = datetime.now().astimezone()
        delta = now - dt
        
        if delta.total_seconds() < 60:
            return f"{int(delta.total_seconds())}s ago"
        elif delta.total_seconds() < 3600:
            return f"{int(delta.total_seconds()/60)}m ago"
        else:
            return f"{int(delta.total_seconds()/3600)}h {int((delta.total_seconds()%3600)/60)}m ago"
    except:
        return "Unknown"

def main():
    """Main monitoring loop"""
    print("🔍 Vertex AI Batch Processing Monitor")
    print("Press Ctrl+C to exit")
    print("-" * 50)
    
    try:
        while True:
            progress = get_progress()
            
            # Clear screen and display progress
            print("\033[2J\033[H", end="")  # Clear screen and move to top
            
            print(f"📊 Records: {progress['processed']}/{progress['total']} ({progress['pct_complete']:.1f}%)")
            print(f"⏳ Pending: {progress['pending']}")
            print()
            print("📚 Critical Fields:")
            print(f"   Publisher: {progress['publisher']}/{progress['total']} ({progress['publisher']/progress['total']*100:.1f}%)")
            print(f"   Description: {progress['description']}/{progress['total']} ({progress['description']/progress['total']*100:.1f}%)")
            print(f"   Series Volume: {progress['series_volume']}/{progress['total']} ({progress['series_volume']/progress['total']*100:.1f}%)")
            print()
            print(f"🕒 Last processed: {format_time(progress['last_time'])}")
            
            # Progress bar
            bar_width = 30
            filled = int(bar_width * progress['pct_complete'] / 100)
            bar = "█" * filled + "░" * (bar_width - filled)
            print(f"[{bar}] {progress['pct_complete']:.1f}%")
            
            if progress['processed'] >= progress['total']:
                print("\n🎉 PROCESSING COMPLETE!")
                break
                
            time.sleep(5)
            
    except KeyboardInterrupt:
        print("\n🛑 Monitoring stopped")
    except Exception as e:
        print(f"\n❌ Error: {e}")

if __name__ == "__main__":
    main()