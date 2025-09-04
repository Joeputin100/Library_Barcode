#!/usr/bin/env python3
"""
Extract cached API data to complete missing MARC fields
MLE-Star: Use existing cached data to enhance records without new API calls
"""

import json
import sqlite3
from caching import load_cache

def extract_cached_marc_data():
    """Extract MARC field data from cache and update database"""
    
    # Load cache
    cache = load_cache()
    print(f"Loaded cache with {len(cache)} entries")
    
    # Connect to database
    conn = sqlite3.connect('review_app/data/reviews.db')
    cursor = conn.cursor()
    
    updated_count = 0
    
    # Find records with missing MARC fields
    problematic_records = cursor.execute('''
        SELECT id, record_number, title, author, isbn 
        FROM records 
        WHERE publisher = '' OR physical_description = '' OR publication_date = ''
        ORDER BY record_number
    ''').fetchall()
    
    print(f"Found {len(problematic_records)} records with missing MARC fields")
    
    for record in problematic_records:
        record_id, record_number, title, author, isbn = record
        
        # Try to find cached data using multiple key patterns
        cache_keys_to_try = []
        
        if isbn and isbn != 'None':
            # Try multiple cache key patterns
            cache_keys_to_try.append(f"google_unknown title|unknown author|{isbn}".lower())
            cache_keys_to_try.append(f"google_{isbn}".lower())
            cache_keys_to_try.append(f"openlibrary_.*{isbn}".lower())
            cache_keys_to_try.append(f"google_.*{isbn}".lower())
        
        if title and title != 'Unknown Title':
            safe_title = "".join(c for c in title if c.isalnum() or c in ' .:').strip()
            safe_author = "".join(c for c in author if c.isalnum() or c in ' ,').strip() if author else ""
            
            if safe_title and safe_author:
                cache_keys_to_try.append(f"google_{safe_title}|{safe_author}".lower())
            if safe_title:
                cache_keys_to_try.append(f"google_{safe_title}|".lower())
        
        # Try each cache key pattern
        cached_data = None
        used_key = None
        
        for pattern in cache_keys_to_try:
            # Handle exact matches
            if pattern in cache:
                cached_data = cache[pattern]
                used_key = pattern
                break
            # Handle pattern matches (for wildcards)
            elif '*' in pattern or '.*' in pattern:
                import re
                regex_pattern = pattern.replace('*', '.*').replace('.', '\.')
                for cache_key in cache.keys():
                    if re.match(regex_pattern, cache_key):
                        cached_data = cache[cache_key]
                        used_key = cache_key
                        break
                if cached_data:
                    break
        
        if cached_data:
            print(f"Found cached data for record {record_number} with key: {used_key}")
        
        if not cached_data:
            continue
        
        # Extract MARC fields from cached data
        updates = {}
        
        # Publisher
        if not updates.get('publisher') and cached_data.get('publisher'):
            updates['publisher'] = cached_data['publisher']
        
        # Publication date
        if not updates.get('publication_date') and cached_data.get('publication_date'):
            updates['publication_date'] = cached_data['publication_date']
        elif not updates.get('publication_date') and cached_data.get('publication_year'):
            updates['publication_date'] = cached_data['publication_year']
        
        # Physical description
        if not updates.get('physical_description') and cached_data.get('physical_description'):
            updates['physical_description'] = cached_data['physical_description']
        elif not updates.get('physical_description') and cached_data.get('page_count'):
            page_count = cached_data['page_count']
            if page_count > 0:
                updates['physical_description'] = f"{page_count} pages ; 24 cm"
        
        # Language
        if not updates.get('language') and cached_data.get('language'):
            updates['language'] = cached_data['language']
        
        if updates:
            print(f"Updating record {record_number} with: {updates}")
            
            # Build SQL update
            set_clauses = []
            params = []
            
            for field, value in updates.items():
                set_clauses.append(f"{field} = ?")
                params.append(value)
            
            params.append(record_id)
            
            cursor.execute(f'''
                UPDATE records SET {', '.join(set_clauses)}
                WHERE id = ?
            ''', params)
            
            updated_count += 1
    
    # Commit changes
    conn.commit()
    conn.close()
    
    print(f"✅ Updated {updated_count} records with cached MARC field data")
    return updated_count

def update_specific_isbn_records():
    """Update specific ISBN records with known good data from cache"""
    
    # Known problematic ISBNs that should have good cached data
    known_isbns = [
        '978-1-7896-6308-2',  # Confident Coding
        '978-0-06-287274-9',  # Known good record
        '978-1-328-91124-7',  # Friday Black
        '978-1-328-50568-2',  # The Accidental President
    ]
    
    cache = load_cache()
    conn = sqlite3.connect('review_app/data/reviews.db')
    cursor = conn.cursor()
    
    updated_count = 0
    
    for isbn in known_isbns:
        # Try multiple cache key patterns
        cache_keys_to_try = [
            f"google_unknown title|unknown author|{isbn}".lower(),
            f"google_{isbn}".lower(),
            f"openlibrary_.*{isbn}".lower()
        ]
        
        cached_data = None
        for pattern in cache_keys_to_try:
            if pattern in cache:
                cached_data = cache[pattern]
                break
            elif '*' in pattern or '.*' in pattern:
                import re
                regex_pattern = pattern.replace('*', '.*').replace('.', '\.')
                for cache_key in cache.keys():
                    if re.match(regex_pattern, cache_key):
                        cached_data = cache[cache_key]
                        break
                if cached_data:
                    break
        
        if not cached_data:
            print(f"No cache found for ISBN: {isbn}")
            continue
        
        # Update the record
        cursor.execute('''
            UPDATE records SET
                publisher = COALESCE(?, publisher),
                publication_date = COALESCE(?, publication_date),
                physical_description = COALESCE(?, physical_description),
                language = COALESCE(?, language)
            WHERE isbn = ?
        ''', (
            cached_data.get('publisher', ''),
            cached_data.get('publication_date', cached_data.get('publication_year', '')),
            cached_data.get('physical_description', f"{cached_data.get('page_count', 0)} pages ; 24 cm" if cached_data.get('page_count', 0) > 0 else ''),
            cached_data.get('language', 'en'),
            isbn
        ))
        
        if cursor.rowcount > 0:
            print(f"✅ Updated record with ISBN: {isbn}")
            updated_count += 1
    
    conn.commit()
    conn.close()
    return updated_count

if __name__ == "__main__":
    print("Extracting cached MARC field data...")
    
    # Update specific ISBN records first (targeted approach)
    specific_updates = update_specific_isbn_records()
    print(f"Updated {specific_updates} specific ISBN records")
    
    # Then update all records with missing fields
    all_updates = extract_cached_marc_data()
    print(f"Total records updated: {specific_updates + all_updates}")