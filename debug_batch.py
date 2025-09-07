#!/usr/bin/env python3
"""
Debug version of batch processor with detailed logging
"""

import sqlite3
import json
import time
from datetime import datetime
from caching import load_cache, save_cache
from vertex_grounded_research import perform_grounded_research, apply_research_to_record

def debug_process_batch():
    """Debug process with detailed logging"""
    
    cache = load_cache()
    conn = sqlite3.connect('review_app/data/reviews.db')
    
    # Get next record to process
    cursor = conn.cursor()
    cursor.execute('''
        SELECT id, record_number, title, author, isbn, 
               publisher, physical_description, description,
               series_volume, edition, lccn, dewey_decimal
        FROM records 
        WHERE enhanced_description NOT LIKE '%VERTEX AI RESEARCH%'
        ORDER BY record_number
        LIMIT 1
    ''')
    
    record = cursor.fetchone()
    if not record:
        print("No more records to process")
        return
    
    (
        id, record_number, title, author, isbn,
        publisher, physical_desc, description,
        series_volume, edition, lccn, dewey_decimal
    ) = record
    
    record_data = {
        'id': id,
        'record_number': record_number,
        'title': title,
        'author': author,
        'isbn': isbn,
        'publisher': publisher,
        'physical_description': physical_desc,
        'description': description,
        'series_volume': series_volume,
        'edition': edition,
        'lccn': lccn,
        'dewey_decimal': dewey_decimal
    }
    
    print(f"🔍 Processing Record #{record_number}: {title}")
    print(f"   ISBN: {isbn}")
    
    try:
        # Perform grounded research
        print("   🤖 Performing research...")
        research_results, cached = perform_grounded_research(record_data, cache)
        
        if 'error' in research_results:
            print(f"   ❌ Research failed: {research_results['error']}")
            return
        
        print(f"   ✅ Research completed ({'cached' if cached else 'new'})")
        
        # Apply research to database
        print("   💾 Applying to database...")
        updates_applied = apply_research_to_record(id, research_results, conn)
        
        print(f"   ✅ Updates applied: {updates_applied}")
        
        # Verify the update
        cursor.execute('SELECT enhanced_description FROM records WHERE id = ?', (id,))
        result = cursor.fetchone()
        
        if result and 'VERTEX AI RESEARCH' in result[0]:
            print(f"   🎯 Successfully saved to database")
        else:
            print(f"   ❗ Database save may have failed")
        
        # Save cache
        save_cache(cache)
        
    except Exception as e:
        print(f"   💥 Exception: {e}")
        import traceback
        traceback.print_exc()
    
    conn.close()

if __name__ == "__main__":
    debug_process_batch()