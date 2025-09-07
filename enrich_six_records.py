#!/usr/bin/env python3
"""
Enrich the 6 specific records with corrected ISBNs using Vertex AI
Records: 1, 2, 3, 4, 5, 7
"""

import sqlite3
import time
from caching import load_cache, save_cache
from vertex_grounded_research import perform_grounded_research, apply_research_to_record

def enrich_specific_records():
    """Enrich the 6 specific records with corrected ISBNs"""
    
    # Load cache
    cache = load_cache()
    print(f"Loaded cache with {len(cache)} entries")
    
    # Connect to database
    conn = sqlite3.connect('review_app/data/reviews.db')
    cursor = conn.cursor()
    
    # The 6 specific records we need to re-enrich
    target_records = [1, 2, 3, 4, 5, 7]
    
    print("🚀 Starting Vertex AI enrichment for 6 specific records with corrected ISBNs")
    print("=" * 70)
    print("Target records:", target_records)
    print("=" * 70)
    
    total_updates = 0
    
    for record_number in target_records:
        cursor.execute('''
            SELECT * FROM records WHERE record_number = ?
        ''', (record_number,))
        
        record = cursor.fetchone()
        if not record:
            print(f"❌ Record {record_number} not found")
            continue
        
        # Get column names and create dictionary
        columns = [description[0] for description in cursor.description]
        record_data = dict(zip(columns, record))
        print(f"\n--- Researching Record #{record_number}: {record_data.get('title', 'Unknown')} ---")
        print(f"ISBN: {record_data.get('isbn', 'None')}")
        
        # Perform grounded research
        research_results, cached = perform_grounded_research(record_data, cache)
        
        if 'error' in research_results:
            print(f"❌ Research failed: {research_results['error']}")
            continue
        
        # Apply research to database
        updates_applied = apply_research_to_record(record_data['id'], research_results, conn)
        total_updates += updates_applied
        
        print(f"✅ Research completed ({'cached' if cached else 'new'})")
        print(f"📊 Updates applied: {updates_applied} fields")
        
        # Show sample of research results
        if 'verified_data' in research_results:
            verified = research_results['verified_data']
            print(f"Verified: {verified.get('title', 'No title')} by {verified.get('author', 'Unknown')}")
        
        if 'enriched_data' in research_results:
            enriched = research_results['enriched_data']
            if enriched.get('dewey_decimal'):
                print(f"Classification: {enriched['dewey_decimal']}")
        
        # Brief delay between researches
        time.sleep(2)
    
    # Commit changes and close
    conn.commit()
    conn.close()
    
    # Save cache
    save_cache(cache)
    
    print(f"\n🎉 Vertex AI enrichment completed! {total_updates} total fields updated across 6 records.")
    return total_updates

if __name__ == "__main__":
    enrich_specific_records()