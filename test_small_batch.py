#!/usr/bin/env python3
"""
Test small batch processing for Vertex AI research
"""

import sqlite3
import json
import time
from datetime import datetime
from caching import load_cache, save_cache
from vertex_grounded_research import perform_grounded_research, apply_research_to_record

def test_small_batch():
    """Test with a small batch of 5 records"""
    
    cache = load_cache()
    conn = sqlite3.connect('review_app/data/reviews.db')
    cursor = conn.cursor()
    
    # Get a small test batch (records with most missing critical fields)
    cursor.execute('''
        SELECT id, record_number, title, author, isbn, 
               publisher, physical_description, description,
               series_volume, edition, lccn, dewey_decimal
        FROM records 
        WHERE (series_volume = '' OR series_volume = 'None' OR series_volume IS NULL)
           OR (description = '' OR description = 'None' OR description IS NULL)
           OR (publisher = '' OR publisher = 'None' OR publisher IS NULL)
        ORDER BY record_number
        LIMIT 5
    ''')
    
    test_records = []
    for record in cursor.fetchall():
        (
            id, record_number, title, author, isbn,
            publisher, physical_desc, description,
            series_volume, edition, lccn, dewey_decimal
        ) = record
        
        test_records.append({
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
        })
    
    conn.close()
    
    print("🧪 Testing Vertex AI Batch Processing with 5 records")
    print("=" * 60)
    print("Records with missing critical fields (series_volume, description, publisher)")
    print("=" * 60)
    
    results = {
        'processed': 0,
        'cached': 0,
        'new_research': 0,
        'errors': 0,
        'updates_applied': 0
    }
    
    for i, record_data in enumerate(test_records):
        print(f"\n--- Processing Record #{record_data['record_number']}: {record_data['title']} ---")
        print(f"Missing: series_volume='{record_data['series_volume']}', description='{record_data['description']}', publisher='{record_data['publisher']}'")
        
        try:
            # Perform grounded research
            research_results, cached = perform_grounded_research(record_data, cache)
            
            if 'error' in research_results:
                print(f"❌ Research failed: {research_results['error']}")
                results['errors'] += 1
                continue
            
            # Apply research to database
            conn = sqlite3.connect('review_app/data/reviews.db')
            updates_applied = apply_research_to_record(record_data['id'], research_results, conn)
            conn.close()
            
            if cached:
                results['cached'] += 1
                print(f"✅ Used cached research: {updates_applied} fields updated")
            else:
                results['new_research'] += 1
                print(f"✅ New research completed: {updates_applied} fields updated")
            
            results['processed'] += 1
            results['updates_applied'] += updates_applied
            
            # Show some research results
            if 'verified_data' in research_results:
                verified = research_results['verified_data']
                if verified.get('publisher'):
                    print(f"   Publisher: {verified['publisher']}")
            
            if 'enriched_data' in research_results:
                enriched = research_results['enriched_data']
                if enriched.get('series_info'):
                    print(f"   Series: {enriched['series_info']}")
            
            # Respectful delay for new research
            if not cached:
                time.sleep(3)
                
        except Exception as e:
            print(f"❌ Error: {e}")
            results['errors'] += 1
            continue
    
    # Save cache
    save_cache(cache)
    
    print(f"\n📊 Test Batch Results:")
    print("=" * 40)
    print(f"Processed: {results['processed']}/5")
    print(f"Cached: {results['cached']}")
    print(f"New Research: {results['new_research']}")
    print(f"Updates Applied: {results['updates_applied']}")
    print(f"Errors: {results['errors']}")
    print(f"Success Rate: {(results['processed']/5)*100:.1f}%")
    
    return results

if __name__ == "__main__":
    test_small_batch()