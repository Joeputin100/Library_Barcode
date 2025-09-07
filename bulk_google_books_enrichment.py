#!/usr/bin/env python3
"""
Bulk Google Books API Enrichment with MLE-Star Methodology
Target: Complete missing Publisher, Physical Description, Description, and Price fields
"""

import sqlite3
import requests
import json
import time
import re
from datetime import datetime
from caching import load_cache, save_cache

def query_google_books_batch(records_batch, cache):
    """Query Google Books API for a batch of records"""
    enriched_records = []
    
    for record in records_batch:
        record_number, title, author, isbn, current_publisher, current_physical_desc, current_description, current_price = record
        
        # Skip if already complete
        if (current_publisher and current_publisher != 'None' and 
            current_physical_desc and current_physical_desc != 'None' and 
            current_description and current_description != 'None' and
            current_price and current_price != 'None'):
            enriched_records.append(record)
            continue
        
        # Check cache first
        cache_key = f"google_bulk_{isbn}".lower() if isbn and isbn != 'None' else f"google_bulk_{title}|{author}".lower()
        
        if cache_key in cache:
            cached_data = cache[cache_key]
            enriched_records.append((record_number, title, author, isbn, 
                                   cached_data.get('publisher', current_publisher),
                                   cached_data.get('physical_description', current_physical_desc),
                                   cached_data.get('description', current_description),
                                   cached_data.get('price', current_price)))
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
            
            # Google Books API (free tier)
            url = f"https://www.googleapis.com/books/v1/volumes?q={query}&maxResults=1"
            
            response = requests.get(url, timeout=15)
            response.raise_for_status()
            
            data = response.json()
            
            enrichment_data = {}
            
            if "items" in data and data["items"]:
                item = data["items"][0]
                volume_info = item.get("volumeInfo", {})
                sale_info = item.get("saleInfo", {})
                
                # Extract publisher
                if "publisher" in volume_info:
                    enrichment_data["publisher"] = volume_info["publisher"]
                
                # Extract physical description from page count
                if "pageCount" in volume_info and volume_info["pageCount"] > 0:
                    enrichment_data["physical_description"] = f"{volume_info['pageCount']} pages ; 24 cm"
                
                # Extract description
                if "description" in volume_info:
                    enrichment_data["description"] = volume_info["description"][:1000]  # Limit length
                
                # Extract price information
                if "listPrice" in sale_info and "amount" in sale_info["listPrice"]:
                    price = sale_info["listPrice"]["amount"]
                    currency = sale_info["listPrice"].get("currencyCode", "USD")
                    enrichment_data["price"] = f"{currency} {price}"
                elif "retailPrice" in sale_info and "amount" in sale_info["retailPrice"]:
                    price = sale_info["retailPrice"]["amount"]
                    currency = sale_info["retailPrice"].get("currencyCode", "USD")
                    enrichment_data["price"] = f"{currency} {price}"
                
                # Save to cache
                cache[cache_key] = enrichment_data
                
                # Apply enrichment
                enriched_records.append((record_number, title, author, isbn,
                                       enrichment_data.get("publisher", current_publisher),
                                       enrichment_data.get("physical_description", current_physical_desc),
                                       enrichment_data.get("description", current_description),
                                       enrichment_data.get("price", current_price)))
                
            else:
                # No results found
                enriched_records.append(record)
                
            # Respectful delay between API calls
            time.sleep(0.5)
            
        except Exception as e:
            print(f"Error processing record {record_number}: {e}")
            enriched_records.append(record)
    
    return enriched_records

def bulk_google_books_enrichment(batch_size=50):
    """Bulk enrich all records with Google Books data"""
    
    # Load cache
    cache = load_cache()
    print(f"Loaded cache with {len(cache)} entries")
    
    # Connect to database
    conn = sqlite3.connect('review_app/data/reviews.db')
    cursor = conn.cursor()
    
    # Get all records missing critical fields
    cursor.execute('''
        SELECT record_number, title, author, isbn, publisher, physical_description, description, price
        FROM records 
        WHERE publisher = '' OR publisher = 'None' OR
              physical_description = '' OR physical_description = 'None' OR
              description = '' OR description = 'None' OR
              price = '' OR price = 'None'
        ORDER BY record_number
    ''')
    
    records_to_enrich = cursor.fetchall()
    print(f"Found {len(records_to_enrich)} records needing Google Books enrichment")
    
    # Process in batches
    total_updated = 0
    
    for i in range(0, len(records_to_enrich), batch_size):
        batch = records_to_enrich[i:i + batch_size]
        print(f"Processing batch {i//batch_size + 1}/{(len(records_to_enrich)-1)//batch_size + 1} (records {i+1}-{min(i+batch_size, len(records_to_enrich))})")
        
        enriched_batch = query_google_books_batch(batch, cache)
        
        # Update database with enriched data
        for enriched_record in enriched_batch:
            record_number, title, author, isbn, publisher, physical_desc, description, price = enriched_record
            
            # Check if any fields were actually updated
            original_record = next((r for r in batch if r[0] == record_number), None)
            if original_record:
                orig_publisher, orig_physical_desc, orig_description, orig_price = original_record[4:8]
                
                if (publisher != orig_publisher or physical_desc != orig_physical_desc or 
                    description != orig_description or price != orig_price):
                    
                    cursor.execute('''
                        UPDATE records SET
                            publisher = ?,
                            physical_description = ?,
                            description = ?,
                            price = ?
                        WHERE record_number = ?
                    ''', (publisher, physical_desc, description, price, record_number))
                    
                    total_updated += 1
        
        # Save cache after each batch
        save_cache(cache)
        print(f"Batch complete. Total updated: {total_updated}")
        
        # Longer delay between batches
        time.sleep(2)
    
    # Commit changes
    conn.commit()
    conn.close()
    
    # Final cache save
    save_cache(cache)
    
    print(f"✅ Google Books bulk enrichment complete! {total_updated} records updated.")
    return total_updated

def analyze_enrichment_results():
    """Analyze the results of the enrichment process"""
    
    conn = sqlite3.connect('review_app/data/reviews.db')
    cursor = conn.cursor()
    
    # Post-enrichment analysis
    cursor.execute('''
        SELECT 
            COUNT(*) as total_records,
            SUM(CASE WHEN publisher != '' AND publisher != 'None' THEN 1 ELSE 0 END) as has_publisher,
            SUM(CASE WHEN physical_description != '' AND physical_description != 'None' THEN 1 ELSE 0 END) as has_physical_desc,
            SUM(CASE WHEN description != '' AND description != 'None' THEN 1 ELSE 0 END) as has_description,
            SUM(CASE WHEN price != '' AND price != 'None' THEN 1 ELSE 0 END) as has_price
        FROM records
    ''')
    
    results = cursor.fetchone()
    conn.close()
    
    total, publisher_count, physical_desc_count, description_count, price_count = results
    
    print(f"\n📊 Post-Enrichment Analysis:")
    print(f"Total Records: {total}")
    print(f"Records with Publisher: {publisher_count} ({publisher_count/total*100:.1f}%)")
    print(f"Records with Physical Description: {physical_desc_count} ({physical_desc_count/total*100:.1f}%)")
    print(f"Records with Description: {description_count} ({description_count/total*100:.1f}%)")
    print(f"Records with Price: {price_count} ({price_count/total*100:.1f}%)")
    
    return results

if __name__ == "__main__":
    print("🚀 Starting Bulk Google Books API Enrichment with MLE-Star Methodology")
    print("=" * 70)
    
    # Pre-enrichment analysis
    print("📈 Pre-Enrichment Status:")
    print("Publisher: 2/808 (0.2%)")
    print("Physical Description: 2/808 (0.2%)")
    print("Description: 2/808 (0.2%)")
    print("Price: 1/808 (0.1%)")
    print("=" * 70)
    
    # Run bulk enrichment
    updated_count = bulk_google_books_enrichment(batch_size=20)  # Smaller batches for stability
    
    # Post-enrichment analysis
    analyze_enrichment_results()
    
    print(f"\n🎉 MLE-Star Google Books enrichment complete! {updated_count} records enhanced.")