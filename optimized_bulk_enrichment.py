#!/usr/bin/env python3
"""
Optimized Bulk MARC Field Enrichment
Target: Complete Publisher, Physical Description, Description fields efficiently
"""

import sqlite3
import requests
import re
import time
import os
from caching import load_cache, save_cache

def optimized_bulk_enrichment():
    """Optimized bulk enrichment focusing on most missing fields"""
    
    # Load cache
    cache = load_cache()
    print(f"Loaded cache with {len(cache)} entries")
    
    # Connect to database
    conn = sqlite3.connect('review_app/data/reviews.db')
    cursor = conn.cursor()
    
    # Get records missing critical fields
    cursor.execute('''
        SELECT record_number, title, author, isbn, publisher, physical_description, description
        FROM records 
        WHERE (publisher = '' OR publisher = 'None' OR publisher IS NULL) OR
              (physical_description = '' OR physical_description = 'None' OR physical_description IS NULL) OR
              (description = '' OR description = 'None' OR description IS NULL)
        ORDER BY record_number
    ''')
    
    records_to_enrich = cursor.fetchall()
    print(f"Found {len(records_to_enrich)} records needing enrichment")
    
    total_updated = 0
    
    for i, record in enumerate(records_to_enrich):
        record_number, title, author, isbn, current_publisher, current_physical_desc, current_description = record
        
        if i % 20 == 0:
            print(f"Processing record {i+1}/{len(records_to_enrich)} (Record #{record_number})")
        
        # Generate cache key
        cache_key = f"google_opt_{isbn}".lower() if isbn and isbn != 'None' else f"google_opt_{title}|{author}".lower()
        
        # Check cache first
        if cache_key in cache:
            cached_data = cache[cache_key]
            
            # Update database from cache using parameterized query
            cursor.execute('''
                UPDATE records SET
                    publisher = COALESCE(?, publisher),
                    physical_description = COALESCE(?, physical_description),
                    description = COALESCE(?, description)
                WHERE record_number = ?
            ''', (
                cached_data.get('publisher'),
                cached_data.get('physical_description'),
                cached_data.get('description'),
                record_number
            ))
            
            if cursor.rowcount > 0:
                total_updated += 1
            
            continue
        
        # Query Google Books API
        try:
            if isbn and isbn != 'None':
                query = f"isbn:{isbn}"
            else:
                safe_title = re.sub(r"[^a-zA-Z0-9\s\.:]", "", title)
                safe_author = re.sub(r"[^a-zA-Z0-9\s, ]", "", author) if author else ""
                query = f'intitle:"{safe_title}"'
                if safe_author:
                    query += f' inauthor:"{safe_author}"'
            
            api_key = os.environ.get("GOOGLE_API_KEY", "")
            url = f"https://www.googleapis.com/books/v1/volumes?q={query}&maxResults=1&key={api_key}"
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            
            enrichment_data = {}
            
            if "items" in data and data["items"]:
                item = data["items"][0]
                volume_info = item.get("volumeInfo", {})
                
                # Extract fields
                if "publisher" in volume_info:
                    enrichment_data["publisher"] = volume_info["publisher"]
                
                if "pageCount" in volume_info and volume_info["pageCount"] > 0:
                    enrichment_data["physical_description"] = f"{volume_info['pageCount']} pages ; 24 cm"
                
                if "description" in volume_info:
                    enrichment_data["description"] = volume_info["description"][:1000]
                
                # Save to cache
                cache[cache_key] = enrichment_data
                
                # Update database with parameterized query
                cursor.execute('''
                    UPDATE records SET
                        publisher = COALESCE(?, publisher),
                        physical_description = COALESCE(?, physical_description),
                        description = COALESCE(?, description)
                    WHERE record_number = ?
                ''', (
                    enrichment_data.get('publisher'),
                    enrichment_data.get('physical_description'),
                    enrichment_data.get('description'),
                    record_number
                ))
                
                if cursor.rowcount > 0:
                    total_updated += 1
            
            # Respectful delay
            time.sleep(0.5)
            
        except Exception as e:
            if "429" in str(e) or "rate" in str(e).lower():
                print(f"Rate limit detected, waiting 5 seconds...")
                time.sleep(5)
            else:
                print(f"Error processing record {record_number}: {e}")
            continue
    
    # Commit changes and save cache
    conn.commit()
    conn.close()
    save_cache(cache)
    
    print(f"✅ Optimized bulk enrichment complete! {total_updated} records updated.")
    return total_updated

def check_enrichment_progress():
    """Check current enrichment progress"""
    
    conn = sqlite3.connect('review_app/data/reviews.db')
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT 
            COUNT(*) as total,
            SUM(CASE WHEN publisher != '' AND publisher != 'None' THEN 1 ELSE 0 END) as publisher_complete,
            SUM(CASE WHEN physical_description != '' AND physical_description != 'None' THEN 1 ELSE 0 END) as physical_desc_complete,
            SUM(CASE WHEN description != '' AND description != 'None' THEN 1 ELSE 0 END) as description_complete
        FROM records
    ''')
    
    total, publisher, physical_desc, description = cursor.fetchone()
    conn.close()
    
    print(f"\n📊 Current Completion Status:")
    print(f"Total Records: {total}")
    print(f"Publisher: {publisher}/{total} ({publisher/total*100:.1f}%)")
    print(f"Physical Description: {physical_desc}/{total} ({physical_desc/total*100:.1f}%)")
    print(f"Description: {description}/{total} ({description/total*100:.1f}%)")
    
    return publisher, physical_desc, description

if __name__ == "__main__":
    print("🚀 Starting Optimized Bulk MARC Field Enrichment")
    print("=" * 60)
    print("Target: Publisher, Physical Description, Description")
    print("Method: Google Books API with caching and rate limiting")
    print("=" * 60)
    
    # Check pre-enrichment status
    print("📈 Pre-Enrichment:")
    check_enrichment_progress()
    
    # Run optimized enrichment
    updated = optimized_bulk_enrichment()
    
    # Check post-enrichment status
    print("\n📊 Post-Enrichment:")
    check_enrichment_progress()
    
    print(f"\n🎉 Optimized enrichment completed! {updated} records enhanced.")