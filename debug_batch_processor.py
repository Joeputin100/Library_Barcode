#!/usr/bin/env python3
"""
Debug version of Vertex AI batch processor
"""

import sqlite3
import json
import time
from datetime import datetime
from caching import load_cache, save_cache
from vertex_grounded_research import perform_grounded_research, apply_research_to_record

def debug_process_batch(records_batch, batch_name="", batch_size=10):
    """Process a batch of records with detailed debugging"""
    
    cache = load_cache()
    conn = sqlite3.connect('review_app/data/reviews.db')
    
    results = {
        'processed': 0,
        'cached': 0,
        'new_research': 0,
        'errors': 0,
        'updates_applied': 0,
        'start_time': datetime.now().isoformat()
    }
    
    print(f"\n🚀 Processing {batch_name} batch: {len(records_batch)} records")
    print("=" * 60)
    
    for i, record_data in enumerate(records_batch):
        print(f"\n--- Processing Record #{record_data['record_number']}: {record_data['title']} ---")
        print(f"   Missing: series_volume='{record_data['series_volume']}', description='{record_data['description']}', publisher='{record_data['publisher']}'")
        
        if i % batch_size == 0 and i > 0:
            print(f"Processed {i}/{len(records_batch)} records...")
            # Auto-save progress every batch_size records
            conn.commit()
            save_cache(cache)
        
        try:
            # Perform grounded research
            print("   🤖 Performing research...")
            research_results, cached = perform_grounded_research(record_data, cache)
            
            if 'error' in research_results:
                print(f"   ❌ Research failed: {research_results['error']}")
                results['errors'] += 1
                continue
            
            print(f"   ✅ Research completed ({'cached' if cached else 'new'})")
            
            # Apply research to database
            print("   💾 Applying to database...")
            updates_applied = apply_research_to_record(record_data['id'], research_results, conn)
            
            if cached:
                results['cached'] += 1
            else:
                results['new_research'] += 1
            
            results['processed'] += 1
            results['updates_applied'] += updates_applied
            
            print(f"   ✅ Record {record_data['record_number']}: {updates_applied} fields updated ({'cached' if cached else 'new'})")
            
            # Show some research results
            if 'verified_data' in research_results:
                verified = research_results['verified_data']
                if verified.get('publisher'):
                    print(f"   Publisher: {verified['publisher']}")
            
            # Respectful delay between API calls to avoid rate limiting
            if not cached:
                time.sleep(2)
                
        except Exception as e:
            print(f"   💥 Error processing record {record_data['record_number']}: {e}")
            import traceback
            traceback.print_exc()
            results['errors'] += 1
            continue
    
    # Final commit and cache save
    conn.commit()
    conn.close()
    save_cache(cache)
    
    results['end_time'] = datetime.now().isoformat()
    results['success_rate'] = (results['processed'] / len(records_batch)) * 100 if records_batch else 0
    
    return results

def debug_run():
    """Debug run with small batch"""
    
    # Connect to database and get a small batch
    conn = sqlite3.connect('review_app/data/reviews.db')
    cursor = conn.cursor()
    
    # Get next 5 records
    cursor.execute('''
        SELECT id, record_number, title, author, isbn, 
               publisher, physical_description, description,
               series_volume, edition, lccn, dewey_decimal
        FROM records 
        WHERE enhanced_description NOT LIKE '%VERTEX AI RESEARCH%'
        ORDER BY record_number
        LIMIT 5
    ''')
    
    records_batch = []
    for record in cursor.fetchall():
        (
            id, record_number, title, author, isbn,
            publisher, physical_desc, description,
            series_volume, edition, lccn, dewey_decimal
        ) = record
        
        records_batch.append({
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
    
    if not records_batch:
        print("No records to process")
        return
    
    print(f"🎯 Debug processing {len(records_batch)} records")
    
    # Process the batch
    results = debug_process_batch(records_batch, "debug", 3)
    
    print(f"\n📊 Debug Results:")
    print("=" * 40)
    print(f"Processed: {results['processed']}/{len(records_batch)}")
    print(f"Cached: {results['cached']}")
    print(f"New Research: {results['new_research']}")
    print(f"Updates Applied: {results['updates_applied']}")
    print(f"Errors: {results['errors']}")
    print(f"Success Rate: {results['success_rate']:.1f}%")

if __name__ == "__main__":
    debug_run()