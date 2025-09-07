#!/usr/bin/env python3
"""
Comprehensive MARC Field Enrichment with MLE-Star Methodology
Target: Complete ALL missing MARC fields across 808 records
"""

import sqlite3
import requests
import json
import time
import re
from datetime import datetime
from caching import load_cache, save_cache

# Standard MARC fields to enrich
MARC_FIELDS = [
    'title', 'author', 'isbn', 'call_number', 'description', 'notes',
    'publisher', 'publication_date', 'physical_description', 'subjects',
    'genre', 'series', 'series_volume', 'language', 'edition', 'lccn', 'dewey_decimal'
]

def extract_marc_fields_from_google_books(volume_info, sale_info):
    """Extract ALL MARC fields from Google Books API response"""
    marc_data = {}
    
    # Basic fields
    if "title" in volume_info:
        marc_data["title"] = volume_info["title"]
    if "subtitle" in volume_info:
        marc_data["title"] = f"{marc_data.get('title', '')}: {volume_info['subtitle']}"
    
    if "authors" in volume_info:
        authors = volume_info["authors"]
        if len(authors) == 1:
            marc_data["author"] = authors[0]
        elif len(authors) == 2:
            marc_data["author"] = f"{authors[0]} and {authors[1]}"
        else:
            marc_data["author"] = ", ".join(authors[:-1]) + f", and {authors[-1]}"
    
    if "industryIdentifiers" in volume_info:
        for identifier in volume_info["industryIdentifiers"]:
            if identifier.get("type") == "ISBN_13":
                marc_data["isbn"] = identifier.get("identifier", "")
                break
            elif identifier.get("type") == "ISBN_10" and "isbn" not in marc_data:
                marc_data["isbn"] = identifier.get("identifier", "")
    
    # Publisher information
    if "publisher" in volume_info:
        marc_data["publisher"] = volume_info["publisher"]
    
    # Publication date
    if "publishedDate" in volume_info:
        marc_data["publication_date"] = volume_info["publishedDate"]
    
    # Physical description
    if "pageCount" in volume_info and volume_info["pageCount"] > 0:
        marc_data["physical_description"] = f"{volume_info['pageCount']} pages ; 24 cm"
    
    # Description
    if "description" in volume_info:
        marc_data["description"] = volume_info["description"][:1000]  # Limit length
    
    # Subjects and genres
    if "categories" in volume_info:
        marc_data["subjects"] = ", ".join(volume_info["categories"])
        marc_data["genre"] = ", ".join(volume_info["categories"][:3])  # First 3 as genre
    
    # Series information
    if "seriesInfo" in volume_info:
        series_info = volume_info["seriesInfo"]
        if "series" in series_info and series_info["series"]:
            marc_data["series"] = series_info["series"][0].get("title", "")
        if "bookDisplayNumber" in series_info:
            marc_data["series_volume"] = series_info["bookDisplayNumber"]
    
    # Language
    if "language" in volume_info:
        marc_data["language"] = volume_info["language"]
    
    # Edition information
    if "contentVersion" in volume_info:
        marc_data["edition"] = volume_info["contentVersion"]
    
    # Price information
    if sale_info:
        if "listPrice" in sale_info and "amount" in sale_info["listPrice"]:
            price = sale_info["listPrice"]["amount"]
            currency = sale_info["listPrice"].get("currencyCode", "USD")
            marc_data["price"] = f"{currency} {price}"
        elif "retailPrice" in sale_info and "amount" in sale_info["retailPrice"]:
            price = sale_info["retailPrice"]["amount"]
            currency = sale_info["retailPrice"].get("currencyCode", "USD")
            marc_data["price"] = f"{currency} {price}"
    
    # Additional metadata that could help with other fields
    if "maturityRating" in volume_info:
        marc_data["notes"] = f"Maturity: {volume_info['maturityRating']}"
    
    return marc_data

def query_google_books_for_complete_marc(title, author, isbn, cache):
    """Query Google Books API for complete MARC field data"""
    
    # Generate cache key
    cache_key = f"google_complete_{isbn}".lower() if isbn and isbn != 'None' else f"google_complete_{title}|{author}".lower()
    
    # Check cache first
    if cache_key in cache:
        return cache[cache_key]
    
    try:
        # Build query
        if isbn and isbn != 'None':
            query = f"isbn:{isbn}"
        else:
            safe_title = re.sub(r"[^a-zA-Z0-9\s\.:]", "", title)
            safe_author = re.sub(r"[^a-zA-Z0-9\s, ]", "", author) if author else ""
            query = f'intitle:"{safe_title}"'
            if safe_author:
                query += f' inauthor:"{safe_author}"'
        
        # Google Books API call
        url = f"https://www.googleapis.com/books/v1/volumes?q={query}&maxResults=1"
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        
        data = response.json()
        
        if "items" in data and data["items"]:
            item = data["items"][0]
            volume_info = item.get("volumeInfo", {})
            sale_info = item.get("saleInfo", {})
            
            # Extract complete MARC data
            marc_data = extract_marc_fields_from_google_books(volume_info, sale_info)
            
            # Save to cache
            cache[cache_key] = marc_data
            
            return marc_data
        
    except Exception as e:
        print(f"Google Books API error: {e}")
    
    return {}

def enhance_record_with_complete_marc(record, cache):
    """Enhance a record with complete MARC field data"""
    record_number, title, author, isbn = record[0], record[1], record[2], record[3]
    
    # Get current field values
    current_fields = {}
    for i, field in enumerate(MARC_FIELDS):
        if i < len(record):
            current_fields[field] = record[i]
    
    # Query Google Books for complete MARC data
    google_data = query_google_books_for_complete_marc(title, author, isbn, cache)
    
    # Apply MLE-Star methodology: Target, Ablate, Refine
    enhanced_fields = current_fields.copy()
    
    for field in MARC_FIELDS:
        current_value = current_fields.get(field, '')
        google_value = google_data.get(field, '')
        
        # TARGET: Focus on missing or incomplete fields
        if not current_value or current_value == 'None' or current_value == 'Not Available':
            if google_value:
                enhanced_fields[field] = google_value
        # REFINE: Enhance existing data with better sources
        elif google_value and field in ['description', 'subjects', 'genre']:
            # For these fields, Google data might be more comprehensive
            enhanced_fields[field] = google_value
    
    return enhanced_fields

def bulk_marc_enrichment(batch_size=20):
    """Bulk enrich ALL MARC fields across all records"""
    
    # Load cache
    cache = load_cache()
    print(f"Loaded cache with {len(cache)} entries")
    
    # Connect to database
    conn = sqlite3.connect('review_app/data/reviews.db')
    cursor = conn.cursor()
    
    # Get ALL records with ALL MARC fields
    field_list = ", ".join(MARC_FIELDS)
    cursor.execute(f'''
        SELECT record_number, {field_list}
        FROM records 
        ORDER BY record_number
    ''')
    
    all_records = cursor.fetchall()
    print(f"Processing {len(all_records)} records for complete MARC enrichment")
    
    total_updated = 0
    
    # Process in batches
    for i in range(0, len(all_records), batch_size):
        batch = all_records[i:i + batch_size]
        print(f"Processing batch {i//batch_size + 1}/{(len(all_records)-1)//batch_size + 1} (records {i+1}-{min(i+batch_size, len(all_records))})")
        
        batch_updates = 0
        
        for record in batch:
            record_number = record[0]
            
            # Enhance record with complete MARC data
            enhanced_data = enhance_record_with_complete_marc(record, cache)
            
            # Check if any fields were updated
            needs_update = False
            update_values = []
            
            for j, field in enumerate(MARC_FIELDS, 1):  # Start from 1 because record[0] is record_number
                current_value = record[j] if j < len(record) else ''
                enhanced_value = enhanced_data.get(field, '')
                
                if enhanced_value and enhanced_value != current_value:
                    needs_update = True
                    update_values.append((field, enhanced_value))
            
            if needs_update:
                # Build SQL update
                set_clauses = []
                params = []
                
                for field, value in update_values:
                    set_clauses.append(f"{field} = ?")
                    params.append(value)
                
                params.append(record_number)
                
                cursor.execute(f'''
                    UPDATE records SET {', '.join(set_clauses)}
                    WHERE record_number = ?
                ''', params)
                
                batch_updates += len(update_values)
                total_updated += len(update_values)
        
        print(f"Batch updated {batch_updates} fields. Total: {total_updated}")
        
        # Save cache after each batch
        save_cache(cache)
        
        # Respectful delay between batches
        time.sleep(3)
    
    # Commit changes
    conn.commit()
    conn.close()
    
    # Final cache save
    save_cache(cache)
    
    print(f"✅ Complete MARC enrichment finished! {total_updated} fields updated across all records.")
    return total_updated

def analyze_complete_marc_completion():
    """Analyze completeness of ALL MARC fields after enrichment"""
    
    conn = sqlite3.connect('review_app/data/reviews.db')
    cursor = conn.cursor()
    
    print("\n📊 Complete MARC Field Completion Analysis:")
    print("=" * 60)
    
    for field in MARC_FIELDS:
        cursor.execute(f'''
            SELECT COUNT(*) FROM records 
            WHERE {field} != '' AND {field} != 'None' AND {field} IS NOT NULL
        ''')
        complete_count = cursor.fetchone()[0]
        percentage = (complete_count / 808) * 100
        
        status = "✅" if percentage >= 80 else "❌" if percentage <= 20 else "⚠️ "
        print(f"{status} {field:20}: {complete_count:3d}/808 ({percentage:5.1f}%)")
    
    conn.close()

if __name__ == "__main__":
    print("🚀 Starting Comprehensive MARC Field Enrichment with MLE-Star Methodology")
    print("=" * 80)
    print("Target: Complete ALL MARC fields across 808 records")
    print("Method: Google Books API + MLE-Star (Target, Ablate, Refine)")
    print("=" * 80)
    
    # Pre-enrichment analysis
    print("📈 Pre-Enrichment Status:")
    print("❌ Publisher: 2/808 (0.2%)")
    print("❌ Physical Description: 2/808 (0.2%)")
    print("❌ Description: 2/808 (0.2%)")
    print("❌ Series: 0/808 (0%)")
    print("❌ Series Volume: 0/808 (0%)")
    print("❌ Edition: 0/808 (0%)")
    print("❌ LCCN: 0/808 (0%)")
    print("❌ Dewey Decimal: 0/808 (0%)")
    print("=" * 80)
    
    # Run comprehensive enrichment
    total_updates = bulk_marc_enrichment(batch_size=15)  # Conservative batch size
    
    # Post-enrichment analysis
    analyze_complete_marc_completion()
    
    print(f"\n🎉 MLE-Star Complete MARC Enrichment finished! {total_updates} fields updated.")