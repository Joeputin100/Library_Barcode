#!/usr/bin/env python3
"""
Re-run free APIs (Google Books, Open Library) for problematic records
Avoids expensive Vertex AI and rate-limited LOC calls
"""

import json
import sqlite3
from api_calls import get_book_metadata_google_books, get_book_metadata_open_library
from caching import load_cache, save_cache

def rerun_free_apis_for_problematic_records():
    """Re-run free APIs for records with unknown titles/authors"""
    
    # Load cache
    cache = load_cache()
    
    # Connect to database
    conn = sqlite3.connect('review_app/data/reviews.db')
    conn.row_factory = sqlite3.Row
    
    # Find problematic records (unknown titles/authors)
    problematic_records = conn.execute('''
        SELECT id, record_number, title, author, isbn, data_sources_used 
        FROM records 
        WHERE title LIKE '%Unknown%' OR author LIKE '%Unknown%' OR title = '' OR author = ''
        ORDER BY record_number
    ''').fetchall()
    
    print(f"Found {len(problematic_records)} problematic records")
    
    updated_count = 0
    
    for record in problematic_records:
        record_id = record['id']
        record_number = record['record_number']
        original_title = record['title']
        original_author = record['author']
        isbn = record['isbn']
        
        print(f"\n--- Processing Record #{record_number} (ID: {record_id}) ---")
        print(f"Original: Title='{original_title}', Author='{original_author}', ISBN='{isbn}'")
        
        # Re-run Google Books API (free & fast)
        print("Re-running Google Books API...")
        google_meta, google_cached, google_success = get_book_metadata_google_books(
            original_title, original_author, isbn, cache
        )
        
        # Re-run Open Library API (free)
        print("Re-running Open Library API...")
        openlibrary_meta, openlibrary_cached, openlibrary_success = get_book_metadata_open_library(
            original_title, original_author, isbn, cache
        )
        
        # Check if we got better data
        new_title = google_meta.get('title') or openlibrary_meta.get('title') or original_title
        new_author = google_meta.get('author') or openlibrary_meta.get('author') or original_author
        
        if new_title != original_title or new_author != original_author:
            print(f"✅ IMPROVEMENT: Title='{new_title}', Author='{new_author}'")
            
            # Update database
            conn.execute('''
                UPDATE records 
                SET title = ?, author = ?, data_quality_score = data_quality_score + 0.1
                WHERE id = ?
            ''', (new_title, new_author, record_id))
            
            updated_count += 1
        else:
            print("❌ No improvement found")
    
    # Commit changes
    conn.commit()
    conn.close()
    
    # Save updated cache
    save_cache(cache)
    
    print(f"\n🎯 Completed: Updated {updated_count} out of {len(problematic_records)} problematic records")
    return updated_count

if __name__ == "__main__":
    print("Re-running free APIs for problematic records...")
    print("This will update Google Books and Open Library data without touching Vertex AI or LOC")
    
    updated = rerun_free_apis_for_problematic_records()
    print(f"\nTotal records improved: {updated}")