#!/usr/bin/env python3
"""
Test Vertex AI research with ISBN/LCCN priority
"""

import sqlite3
from caching import load_cache
from vertex_grounded_research import create_grounded_research_prompt

def test_priority_research():
    """Test research priority with ISBN and LCCN"""
    
    # Get the test record
    conn = sqlite3.connect('review_app/data/reviews.db')
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT id, record_number, title, author, isbn, lccn, publisher, 
               publication_date, description
        FROM records 
        WHERE record_number = 1
    ''')
    
    record = cursor.fetchone()
    if not record:
        print("Record not found")
        return
    
    id, record_number, title, author, isbn, lccn, publisher, publication_date, description = record
    
    record_data = {
        'id': id,
        'record_number': record_number,
        'title': title,
        'author': author,
        'isbn': isbn,
        'lccn': lccn,
        'publisher': publisher,
        'publication_date': publication_date,
        'description': description
    }
    
    print(f"Testing research priority on Record #{record_number}")
    print(f"Title: '{title}'")
    print(f"ISBN: {isbn}")
    print(f"LCCN: {lccn}")
    print("-" * 50)
    
    # Generate research prompt
    research_prompt = create_grounded_research_prompt(record_data)
    
    print("Generated Research Prompt:")
    print("=" * 50)
    print(research_prompt)
    print("=" * 50)

if __name__ == "__main__":
    test_priority_research()