#!/usr/bin/env python3
"""
Comprehensive Vertex AI Batch Processing Monitor
Shows ALL MARC fields with completion percentages
"""

import sqlite3
import time
from datetime import datetime

def get_comprehensive_progress():
    """Get comprehensive progress for ALL MARC fields"""
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
    
    # ALL MARC field completion - exclude empty strings and common placeholder values
    field_queries = {
        'publisher': "publisher != '' AND publisher != 'None' AND publisher NOT LIKE 'Unable to%'",
        'publication_date': "publication_date != '' AND publication_date != 'None' AND publication_date NOT LIKE 'Unable to%'",
        'physical_description': "physical_description != '' AND physical_description != 'None' AND physical_description NOT LIKE 'Unable to%'",
        'subjects': "subjects != '' AND subjects != 'None' AND subjects NOT LIKE 'Unable to%'",
        'genre': "genre != '' AND genre != 'None' AND genre NOT LIKE 'Unable to%'",
        'series': "series != '' AND series != 'None' AND series NOT LIKE 'Unable to%'",
        'series_volume': "series_volume != '' AND series_volume != 'None' AND series_volume NOT LIKE 'Unable to%'",
        'language': "language != '' AND language != 'None' AND language NOT LIKE 'Unable to%'",
        'edition': "edition != '' AND edition != 'None' AND edition NOT LIKE 'Unable to%'",
        'lccn': "lccn != '' AND lccn != 'None' AND lccn NOT LIKE 'Unable to%'",
        'dewey_decimal': "dewey_decimal != '' AND dewey_decimal != 'None' AND dewey_decimal NOT LIKE 'Unable to%'",
        'description': "description != '' AND description != 'None' AND description NOT LIKE 'Unable to%'"
    }
    
    field_completion = {}
    for field, condition in field_queries.items():
        cursor.execute(f'SELECT COUNT(*) FROM records WHERE {condition}')
        count = cursor.fetchone()[0]
        field_completion[field] = {
            'complete': count,
            'percentage': (count / total * 100) if total > 0 else 0
        }
    
    # Get processing status
    cursor.execute('''
        SELECT COUNT(*) FROM records 
        WHERE enhanced_description LIKE '%VERTEX AI RESEARCH%'
    ''')
    vertex_processed = cursor.fetchone()[0]
    
    # Get last record number processed
    cursor.execute('''
        SELECT MAX(record_number) FROM records 
        WHERE enhanced_description LIKE '%VERTEX AI RESEARCH%'
    ''')
    last_record = cursor.fetchone()[0]
    
    conn.close()
    
    return {
        'total': total,
        'processed': processed,
        'pending': total - processed,
        'pct_complete': (processed / total * 100) if total > 0 else 0,
        'fields': field_completion,
        'vertex_processed': vertex_processed,
        'last_record': last_record
    }

def format_percentage(value):
    """Format percentage with color coding"""
    if value >= 90:
        return f"\033[92m{value:.1f}%\033[0m"  # Green
    elif value >= 50:
        return f"\033[93m{value:.1f}%\033[0m"  # Yellow
    else:
        return f"\033[91m{value:.1f}%\033[0m"  # Red

def main():
    """Main monitoring loop"""
    print("🔍 Comprehensive Vertex AI Batch Processing Monitor")
    print("Press Ctrl+C to exit")
    print("-" * 70)
    
    try:
        while True:
            progress = get_comprehensive_progress()
            
            # Clear screen and display progress
            print("\033[2J\033[H", end="")  # Clear screen and move to top
            
            print(f"📊 Overall Progress: {progress['processed']}/{progress['total']} records")
            print(f"   Vertex AI Processed: {progress['vertex_processed']}")
            print(f"   Last Record Processed: #{progress['last_record'] if progress['last_record'] else 'None'}")
            print()
            
            print("📚 MARC Field Completion:")
            print("-" * 40)
            
            # Group fields for better display
            critical_fields = ['publisher', 'description', 'series_volume']
            core_fields = ['publication_date', 'physical_description', 'subjects', 'genre']
            additional_fields = ['series', 'language', 'edition', 'lccn', 'dewey_decimal']
            
            print("🚨 CRITICAL FIELDS:")
            for field in critical_fields:
                data = progress['fields'][field]
                print(f"   {field:20} {data['complete']:3d}/{progress['total']} {format_percentage(data['percentage'])}")
            
            print("\n📖 CORE FIELDS:")
            for field in core_fields:
                data = progress['fields'][field]
                print(f"   {field:20} {data['complete']:3d}/{progress['total']} {format_percentage(data['percentage'])}")
            
            print("\n📋 ADDITIONAL FIELDS:")
            for field in additional_fields:
                data = progress['fields'][field]
                print(f"   {field:20} {data['complete']:3d}/{progress['total']} {format_percentage(data['percentage'])}")
            
            print()
            print("🔄 Processing Status:")
            print(f"   Records awaiting Vertex AI: {progress['pending']}")
            
            # Progress bar
            bar_width = 40
            filled = int(bar_width * progress['pct_complete'] / 100)
            bar = "█" * filled + "░" * (bar_width - filled)
            print(f"   [{bar}] {progress['pct_complete']:.1f}%")
            
            print(f"\n⏰ Last update: {datetime.now().strftime('%H:%M:%S')}")
            
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