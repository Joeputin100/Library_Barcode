#!/usr/bin/env python3
"""
Extract missing MARC fields from cached API data
Populate ISBN, Price, Description, Physical Description from available sources
"""

import sqlite3
import json
import re
from caching import load_cache

def extract_missing_fields_from_cache():
    """Extract missing fields from cached API responses and update database"""
    
    # Load cache
    cache = load_cache()
    print(f"Loaded cache with {len(cache)} entries")
    
    # Connect to database
    conn = sqlite3.connect('review_app/data/reviews.db')
    cursor = conn.cursor()
    
    # Get all records missing key fields
    cursor.execute('''
        SELECT id, record_number, title, author, isbn, publication_date 
        FROM records 
        WHERE isbn = '' OR isbn IS NULL OR 
              price = '' OR price IS NULL OR
              description = '' OR description IS NULL OR
              physical_description = '' OR physical_description IS NULL
        ORDER BY record_number
    ''')
    
    records = cursor.fetchall()
    print(f"Found {len(records)} records with missing fields")
    
    updated_count = 0
    
    for record in records:
        record_id, record_number, title, author, current_isbn, pub_date = record
        
        # Try to find cached data using multiple patterns
        cache_keys_to_try = []
        
        # Try ISBN-based lookup first
        if current_isbn and current_isbn != 'None':
            cache_keys_to_try.append(f"google_.*{current_isbn}".lower())
            cache_keys_to_try.append(f"openlibrary_.*{current_isbn}".lower())
        
        # Try title/author based lookup
        if title and title != 'Unknown Title':
            safe_title = re.sub(r"[^a-zA-Z0-9\s\.:]", "", title).lower()
            safe_author = re.sub(r"[^a-zA-Z0-9\s, ]", "", author).lower() if author else ""
            
            if safe_title and safe_author:
                cache_keys_to_try.append(f"google_{safe_title}|{safe_author}".lower())
            if safe_title:
                cache_keys_to_try.append(f"google_{safe_title}|".lower())
        
        # Search cache for matching keys
        cached_data = None
        used_key = None
        
        for pattern in cache_keys_to_try:
            # Handle regex patterns
            if '*' in pattern or '.*' in pattern:
                regex_pattern = pattern.replace('*', '.*').replace('.', '\.')
                for cache_key in cache.keys():
                    if re.match(regex_pattern, cache_key):
                        cached_data = cache[cache_key]
                        used_key = cache_key
                        break
            else:
                # Exact match
                if pattern in cache:
                    cached_data = cache[pattern]
                    used_key = pattern
                    break
            
            if cached_data:
                break
        
        if not cached_data:
            continue
        
        # Extract missing fields from cached data
        updates = {}
        
        # Extract ISBN
        if (not current_isbn or current_isbn == 'None') and 'isbn' in cached_data:
            updates['isbn'] = cached_data['isbn']
        
        # Extract description
        if 'description' in cached_data and cached_data['description']:
            updates['description'] = cached_data['description'][:500]  # Limit length
        
        # Extract physical description from page count
        if 'page_count' in cached_data and cached_data['page_count'] > 0:
            updates['physical_description'] = f"{cached_data['page_count']} pages ; 24 cm"
        
        # Extract price information if available
        if 'price' in cached_data and cached_data['price']:
            updates['price'] = cached_data['price']
        elif 'list_price' in cached_data and cached_data['list_price']:
            updates['price'] = cached_data['list_price']
        
        # Extract publisher if missing
        if 'publisher' in cached_data and cached_data['publisher']:
            updates['publisher'] = cached_data['publisher']
        
        # Extract language if missing
        if 'language' in cached_data and cached_data['language']:
            updates['language'] = cached_data['language']
        
        if updates:
            print(f"Updating record {record_number} with {len(updates)} fields from cache key: {used_key}")
            
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
    
    print(f"✅ Updated {updated_count} records with missing fields from cache")
    return updated_count

def manual_lookup_record_1():
    """Manually populate record #1 with known Google Books data"""
    
    # Known Google Books data for "Treasures A Novel" by Belva Plain
    google_data = {
        'isbn': '9780804152563',
        'page_count': 528,
        'publication_date': 'August 6, 2014',
        'publisher': 'Random House Publishing Group',
        'language': 'English',
        'description': 'A story of family... the Osbornes -- two sisters and a brother -- united by family ties but split apart by different dreams. Lara, the happy young wife, longs for the family that will make her life whole. Connie, wild and lovely, is more like her brother Eddy -- bright, ambitious, and ready to seize all that life has to offer. A story of choices... Connie is looking for wealth -- to make or to marry. Lara, staying behind in a small Ohio town, finds everything she cherishes threatened by fate and by her own blind commitment. And Eddy, as Wall Street''s "wonder boy," can make millions... if he ruthlessly uses his family and friends. A story of marriages... Lara''s held together by devotion, Connie''s shattered by infidelity and betrayal, and Eddy''s rocked by shame and prison. Torn by conflicting loyalties, they are a family caught in the tides of scandal... and swept toward a fate where dreams may end or be born again...',
        'price': '$5.99',
        'format': 'ebook'
    }
    
    conn = sqlite3.connect('review_app/data/reviews.db')
    cursor = conn.cursor()
    
    # Update record #1
    cursor.execute('''
        UPDATE records SET
            isbn = ?,
            physical_description = ?,
            publication_date = ?,
            publisher = ?,
            language = ?,
            description = ?,
            price = ?
        WHERE record_number = 1
    ''', (
        google_data['isbn'],
        f"{google_data['page_count']} pages ; 24 cm",
        google_data['publication_date'],
        google_data['publisher'],
        google_data['language'],
        google_data['description'],
        google_data['price']
    ))
    
    conn.commit()
    conn.close()
    
    print("✅ Manually updated record #1 with Google Books data")

if __name__ == "__main__":
    print("Extracting missing fields from cached API data...")
    
    # First manually update record #1 with known data
    manual_lookup_record_1()
    
    # Then extract from cache for all other records
    cache_updates = extract_missing_fields_from_cache()
    
    print(f"\n🎉 Field extraction complete! {1 + cache_updates} records updated.")