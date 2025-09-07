#!/usr/bin/env python3
"""
Focused MARC Field Enrichment - Target most critical missing fields first
"""

import sqlite3
import requests
import json
import time
import re
from caching import load_cache, save_cache

def enrich_critical_fields():
    """Enrich the most critically missing fields: Publisher, Physical Description, Description"""
    
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
        WHERE publisher = '' OR publisher = 'None' OR
              physical_description = '' OR physical_description = 'None' OR
              description = '' OR description = 'None'
        ORDER BY record_number
    ''')
    
    records_to_enrich = cursor.fetchall()
    print(f"Found {len(records_to_enrich)} records needing critical field enrichment")
    
    updated_count = 0
    
    for i, record in enumerate(records_to_enrich):
        record_number, title, author, isbn, current_publisher, current_physical_desc, current_description = record
        
        if i % 10 == 0:
            print(f"Processing record {i+1}/{len(records_to_enrich)} (Record #{record_number})")
        
        # Generate cache key
        cache_key = f"google_critical_{isbn}".lower() if isbn and isbn != 'None' else f"google_critical_{title}|{author}".lower()
        
        # Check cache first
        if cache_key in cache:
            cached_data = cache[cache_key]
            
            # Update database from cache
            cursor.execute('''
                UPDATE records SET
                    publisher = COALESCE(?, publisher),
                    physical_description = COALESCE(?, physical_description),
                    description = COALESCE(?, description)
                WHERE record_number = ?
            ''', (
                cached_data.get('publisher', current_publisher),
                cached_data.get('physical_description', current_physical_desc),
                cached_data.get('description', current_description),
                record_number
            ))
            
            if cursor.rowcount > 0:
                updated_count += 1
            
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
            
            url = f"https://www.googleapis.com/books/v1/volumes?q={query}&maxResults=1"
            response = requests.get(url, timeout=15)
            response.raise_for_status()
            
            data = response.json()
            
            enrichment_data = {}
            
            if "items" in data and data["items"]:
                item = data["items"][0]
                volume_info = item.get("volumeInfo", {})
                
                # Extract critical fields
                if "publisher" in volume_info:
                    enrichment_data["publisher"] = volume_info["publisher"]
                
                if "pageCount" in volume_info and volume_info["pageCount"] > 0:
                    enrichment_data["physical_description"] = f"{volume_info['pageCount']} pages ; 24 cm"
                
                if "description" in volume_info:
                    enrichment_data["description"] = volume_info["description"][:1000]
                
                # Save to cache
                cache[cache_key] = enrichment_data
                
                # Update database
                cursor.execute('''
                    UPDATE records SET
                        publisher = COALESCE(?, publisher),
                        physical_description = COALESCE(?, physical_description),
                        description = COALESCE(?, description)
                    WHERE record_number = ?
                ''', (
                    enrichment_data.get('publisher', current_publisher),
                    enrichment_data.get('physical_description', current_physical_desc),
                    enrichment_data.get('description', current_description),
                    record_number
                ))
                
                if cursor.rowcount > 0:
                    updated_count += 1
            
            # Respectful delay
            time.sleep(1)
            
        except Exception as e:
            print(f"Error processing record {record_number}: {e}")
            continue
    
    # Commit changes and save cache
    conn.commit()
    conn.close()
    save_cache(cache)
    
    print(f"✅ Critical field enrichment complete! {updated_count} records updated.")
    return updated_count

def check_current_status():
    """Check current status of critical fields"""
    
    conn = sqlite3.connect('review_app/data/reviews.db')
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT 
            SUM(CASE WHEN publisher != '' AND publisher != 'None' THEN 1 ELSE 0 END) as publisher_complete,
            SUM(CASE WHEN physical_description != '' AND physical_description != 'None' THEN 1 ELSE 0 END) as physical_desc_complete,
            SUM(CASE WHEN description != '' AND description != 'None' THEN 1 ELSE 0 END) as description_complete
        FROM records
    ''')
    
    results = cursor.fetchone()
    conn.close()
    
    publisher, physical_desc, description = results
    
    print(f"\n📊 Current Status:")
    print(f"Publisher: {publisher}/808 ({publisher/808*100:.1f}%)")
    print(f"Physical Description: {physical_desc}/808 ({physical_desc/808*100:.1f}%)")
    print(f"Description: {description}/808 ({description/808*100:.1f}%)")
    
    return results

if __name__ == "__main__":
    print("🎯 Starting Focused MARC Field Enrichment")
    print("=" * 50)
    print("Target: Publisher, Physical Description, Description")
    print("=" * 50)
    
    # Check pre-enrichment status
    print("📈 Pre-Enrichment:")
    check_current_status()
    print("=" * 50)
    
    # Run focused enrichment
    updated = enrich_critical_fields()
    
    # Check post-enrichment status
    print("\n📊 Post-Enrichment:")
    check_current_status()
    
    print(f"\n🎉 Focused enrichment completed! {updated} records enhanced.")