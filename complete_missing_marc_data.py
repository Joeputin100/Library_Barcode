#!/usr/bin/env python3
"""
Complete missing MARC data using free API queries
MLE-Star: Design targeted API queries for missing publisher and physical description fields
"""

import json
import sqlite3
import requests
import re
import time
from datetime import datetime
from caching import load_cache, save_cache

def query_google_books_for_marc_fields(isbn, title, author):
    """Query Google Books API specifically for missing MARC fields"""
    try:
        # Use ISBN-based query for most accurate results
        query = f"isbn:{isbn}" if isbn else f'intitle:"{title}"+inauthor:"{author}"'
        
        # Google Books API (free tier)
        api_key = ""  # Can be empty for basic queries
        url = f"https://www.googleapis.com/books/v1/volumes?q={query}&maxResults=1"
        
        if api_key:
            url += f"&key={api_key}"
            
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        
        data = response.json()
        
        if "items" in data and data["items"]:
            item = data["items"][0]
            volume_info = item.get("volumeInfo", {})
            
            marc_fields = {}
            
            # Extract publisher
            if "publisher" in volume_info:
                marc_fields["publisher"] = volume_info["publisher"]
            
            # Extract physical description from page count
            if "pageCount" in volume_info and volume_info["pageCount"] > 0:
                page_count = volume_info["pageCount"]
                marc_fields["physical_description"] = f"{page_count} pages ; 24 cm"
            
            # Extract language
            if "language" in volume_info:
                marc_fields["language"] = volume_info["language"]
            
            # Extract publication date (full date)
            if "publishedDate" in volume_info:
                marc_fields["publication_date"] = volume_info["publishedDate"]
            
            return marc_fields
            
    except Exception as e:
        print(f"Google Books query failed: {e}")
        
    return {}

def query_open_library_for_marc_fields(isbn):
    """Query Open Library API for missing MARC fields"""
    try:
        if isbn:
            url = f"https://openlibrary.org/isbn/{isbn}.json"
            response = requests.get(url, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                marc_fields = {}
                
                # Extract publisher information
                if "publishers" in data and data["publishers"]:
                    marc_fields["publisher"] = data["publishers"][0]
                
                # Extract number of pages
                if "number_of_pages" in data and data["number_of_pages"] > 0:
                    marc_fields["physical_description"] = f"{data['number_of_pages']} pages ; 24 cm"
                
                # Extract publish date
                if "publish_date" in data:
                    marc_fields["publication_date"] = data["publish_date"]
                
                return marc_fields
                
    except Exception as e:
        print(f"Open Library query failed: {e}")
        
    return {}

def complete_missing_marc_data():
    """Complete missing MARC fields using free API queries"""
    
    # Connect to database
    conn = sqlite3.connect('review_app/data/reviews.db')
    cursor = conn.cursor()
    
    # Find records still missing critical MARC fields
    problematic_records = cursor.execute('''
        SELECT id, record_number, title, author, isbn 
        FROM records 
        WHERE publisher = '' OR physical_description = '' OR publication_date = ''
        ORDER BY record_number
    ''').fetchall()
    
    print(f"Found {len(problematic_records)} records still missing MARC fields")
    
    updated_count = 0
    
    for record in problematic_records:
        record_id, record_number, title, author, isbn = record
        
        print(f"\nProcessing record {record_number}: {title} by {author} (ISBN: {isbn})")
        
        # Query APIs for missing fields
        marc_fields = {}
        
        # Try Google Books first
        if isbn and isbn != 'None':
            marc_fields.update(query_google_books_for_marc_fields(isbn, title, author))
        
        # If still missing fields, try Open Library
        if (not marc_fields.get('publisher') or not marc_fields.get('physical_description')) and isbn:
            open_lib_fields = query_open_library_for_marc_fields(isbn)
            marc_fields.update(open_lib_fields)
        
        # Filter only the fields that are actually missing
        current_missing = cursor.execute('''
            SELECT 
                CASE WHEN publisher = '' THEN 'publisher' ELSE NULL END,
                CASE WHEN physical_description = '' THEN 'physical_description' ELSE NULL END,
                CASE WHEN publication_date = '' THEN 'publication_date' ELSE NULL END
            FROM records WHERE id = ?
        ''', (record_id,)).fetchone()
        
        missing_fields = [field for field in current_missing if field]
        
        # Only update fields that are actually missing
        updates_to_apply = {}
        for field in missing_fields:
            if field in marc_fields and marc_fields[field]:
                updates_to_apply[field] = marc_fields[field]
        
        if updates_to_apply:
            print(f"Updating record {record_number} with: {updates_to_apply}")
            
            # Build SQL update
            set_clauses = []
            params = []
            
            for field, value in updates_to_apply.items():
                set_clauses.append(f"{field} = ?")
                params.append(value)
            
            params.append(record_id)
            
            cursor.execute(f'''
                UPDATE records SET {', '.join(set_clauses)}
                WHERE id = ?
            ''', params)
            
            updated_count += 1
            
            # Add small delay to be respectful of API rate limits
            time.sleep(0.5)
        else:
            print(f"No additional MARC fields found for record {record_number}")
    
    # Commit changes
    conn.commit()
    conn.close()
    
    print(f"✅ Completed missing MARC data for {updated_count} records")
    return updated_count

def target_specific_problematic_records():
    """Target specific known problematic records"""
    
    # Known problematic records that need completion
    target_records = [
        {
            'record_number': 22,
            'isbn': '978-1-7896-6308-2',
            'title': 'Confident Coding',
            'author': 'Rob Percival, Darren Woods'
        },
        # Add more problematic records here
    ]
    
    conn = sqlite3.connect('review_app/data/reviews.db')
    cursor = conn.cursor()
    
    updated_count = 0
    
    for target in target_records:
        print(f"\nTargeting record #{target['record_number']}: {target['title']}")
        
        # Query APIs for complete MARC data
        marc_fields = {}
        
        # Google Books query
        google_fields = query_google_books_for_marc_fields(
            target['isbn'], target['title'], target['author']
        )
        marc_fields.update(google_fields)
        
        # Open Library query
        open_lib_fields = query_open_library_for_marc_fields(target['isbn'])
        marc_fields.update(open_lib_fields)
        
        if marc_fields:
            print(f"Found MARC fields: {marc_fields}")
            
            # Update database
            set_clauses = []
            params = []
            
            for field, value in marc_fields.items():
                if field in ['publisher', 'physical_description', 'publication_date', 'language']:
                    set_clauses.append(f"{field} = ?")
                    params.append(value)
            
            params.append(target['record_number'])
            
            cursor.execute(f'''
                UPDATE records SET {', '.join(set_clauses)}
                WHERE record_number = ?
            ''', params)
            
            if cursor.rowcount > 0:
                updated_count += 1
                print(f"✅ Updated record #{target['record_number']}")
            
            time.sleep(1)  # Respectful delay
        
    conn.commit()
    conn.close()
    
    return updated_count

if __name__ == "__main__":
    print("Completing missing MARC data using free API queries...")
    
    # First target specific problematic records
    specific_updates = target_specific_problematic_records()
    print(f"Updated {specific_updates} specific problematic records")
    
    # Then complete all remaining missing fields
    all_updates = complete_missing_marc_data()
    print(f"Total records updated with complete MARC data: {specific_updates + all_updates}")