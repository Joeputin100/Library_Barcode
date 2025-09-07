#!/usr/bin/env python3
"""
Test applying research results to database
"""

import sqlite3
import json
from caching import load_cache
from vertex_grounded_research import perform_grounded_research, apply_research_to_record

def test_apply_research():
    """Test applying research to database"""
    
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
    
    print(f"Testing research application on Record #{record_number}")
    
    # Perform research
    results, cached = perform_grounded_research(record_data, cache)
    
    if 'error' in results:
        print(f"❌ Research failed: {results['error']}")
        return
    
    print(f"✅ Research successful! Applying to database...")
    
    # Apply research
    updates_applied = apply_research_to_record(id, results, conn)
    
    print(f"✅ Updates applied: {updates_applied}")
    
    # Verify the update
    cursor.execute('SELECT title, publisher, enhanced_description FROM records WHERE id = ?', (id,))
    updated_record = cursor.fetchone()
    
    if updated_record:
        new_title, new_publisher, enhanced_desc = updated_record
        print(f"\n📊 After update:")
        print(f"   Title: '{new_title}' (was: '{title}')")
        print(f"   Publisher: '{new_publisher}' (was: '{publisher}')")
        print(f"   Enhanced description: {'Yes' if enhanced_desc and 'VERTEX AI RESEARCH' in enhanced_desc else 'No'}")
    
    conn.close()

if __name__ == "__main__":
    test_apply_research()