#!/usr/bin/env python3
"""
Debug script to check Vertex AI processor status
"""

import sqlite3
from caching import load_cache
from vertex_grounded_research import perform_grounded_research

def debug_processing():
    """Debug the processing status"""
    
    # Load cache
    cache = load_cache()
    print(f"Cache entries: {len(cache)}")
    
    # Count Vertex AI entries
    vertex_entries = [k for k in cache.keys() if 'vertex_grounded' in k]
    print(f"Vertex AI cached entries: {len(vertex_entries)}")
    
    # Check database
    conn = sqlite3.connect('review_app/data/reviews.db')
    cursor = conn.cursor()
    
    # Get processed records
    cursor.execute('''
        SELECT COUNT(*) FROM records 
        WHERE enhanced_description LIKE '%VERTEX AI RESEARCH%'
    ''')
    processed = cursor.fetchone()[0]
    print(f"Database processed records: {processed}")
    
    # Check next record to process
    cursor.execute('''
        SELECT id, record_number, title FROM records 
        WHERE enhanced_description NOT LIKE '%VERTEX AI RESEARCH%'
        ORDER BY record_number
        LIMIT 1
    ''')
    next_record = cursor.fetchone()
    if next_record:
        id, record_number, title = next_record
        print(f"Next record to process: #{record_number} - {title}")
        
        # Test research on this record
        record_data = {
            'id': id,
            'record_number': record_number,
            'title': title,
            'author': '',
            'isbn': '',
            'publisher': '',
            'description': ''
        }
        
        print("Testing Vertex AI research...")
        try:
            results, cached = perform_grounded_research(record_data, cache)
            if 'error' in results:
                print(f"Research error: {results['error']}")
            else:
                print(f"Research successful! Cached: {cached}")
                if 'verified_data' in results:
                    print(f"Verified data: {results['verified_data']}")
        except Exception as e:
            print(f"Research exception: {e}")
    
    conn.close()

if __name__ == "__main__":
    debug_processing()