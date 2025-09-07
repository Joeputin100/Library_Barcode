#!/usr/bin/env python3
"""
Test Vertex AI research with Unknown Title but valid ISBN
"""

import sqlite3
from caching import load_cache
from vertex_grounded_research import perform_grounded_research

def test_unknown_title_research():
    """Test research on record with Unknown Title but valid ISBN"""
    
    # Load cache
    cache = load_cache()
    
    # Get the test record
    conn = sqlite3.connect('review_app/data/reviews.db')
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT id, record_number, title, author, isbn, publisher, 
               publication_date, description
        FROM records 
        WHERE record_number = 23
    ''')
    
    record = cursor.fetchone()
    if not record:
        print("Record not found")
        return
    
    id, record_number, title, author, isbn, publisher, publication_date, description = record
    
    record_data = {
        'id': id,
        'record_number': record_number,
        'title': title,
        'author': author,
        'isbn': isbn,
        'publisher': publisher,
        'publication_date': publication_date,
        'description': description
    }
    
    print(f"Testing research on Record #{record_number}")
    print(f"Title: '{title}'")
    print(f"ISBN: {isbn}")
    print("-" * 50)
    
    # Perform research
    try:
        results, cached = perform_grounded_research(record_data, cache)
        
        if 'error' in results:
            print(f"❌ Research failed: {results['error']}")
        else:
            print(f"✅ Research successful! Cached: {cached}")
            
            # Show verified data
            if 'verified_data' in results:
                verified = results['verified_data']
                print("\n📋 Verified Data:")
                for key, value in verified.items():
                    if value and 'Unable to verify' not in value:
                        print(f"   {key}: {value}")
            
            # Show enriched data
            if 'enriched_data' in results:
                enriched = results['enriched_data']
                print("\n🎯 Enriched Data:")
                for key, value in enriched.items():
                    if value and value != "Unable to determine":
                        if isinstance(value, list):
                            for item in value:
                                print(f"   {key}: {item}")
                        else:
                            print(f"   {key}: {value}")
    
    except Exception as e:
        print(f"❌ Research exception: {e}")

if __name__ == "__main__":
    test_unknown_title_research()