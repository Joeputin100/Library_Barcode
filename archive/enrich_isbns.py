#!/usr/bin/env python3
"""
Script to enrich ISBN records from the ISBNs file.
This processes both ISBN numbers and NO ISBN entries.
"""

import json
import re
import requests
import time
import os
from api_calls import get_book_metadata_google_books

def load_extracted_data():
    """Load existing extracted data"""
    try:
        with open("extracted_data.json", "r") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}

def save_extracted_data(data):
    """Save extracted data back to file"""
    with open("extracted_data.json", "w") as f:
        json.dump(data, f, indent=4)

def process_isbn_line(line):
    """Process a single line from the ISBN file"""
    line = line.strip()
    if not line:
        return None
    
    # Handle "NO ISBN" entries
    if line.startswith("NO ISBN:"):
        # Extract title and author from "NO ISBN: Title by Author" format
        parts = line.replace("NO ISBN:", "").strip().split(" by ")
        if len(parts) >= 2:
            title = parts[0].strip()
            author = " by ".join(parts[1:]).strip()
            return {
                "type": "no_isbn",
                "title": title,
                "author": author,
                "isbn": None
            }
        else:
            # Just title, no author
            return {
                "type": "no_isbn",
                "title": parts[0].strip(),
                "author": "",
                "isbn": None
            }
    
    # Handle regular ISBN entries
    elif re.match(r'^[0-9X\-]+$', line):
        # Clean ISBN (remove hyphens)
        clean_isbn = line.replace("-", "")
        return {
            "type": "isbn",
            "isbn": clean_isbn,
            "title": "",
            "author": ""
        }
    
    return None

def enrich_book_data(book_info, loc_cache={}):
    """Enrich book data using Google Books API"""
    enriched_data = {
        "title": book_info.get("title", ""),
        "author": book_info.get("author", ""),
        "isbn": book_info.get("isbn", ""),
        "lccn": None
    }
    
    # Use Google Books API for enrichment
    google_meta, from_cache, success = get_book_metadata_google_books(
        book_info.get("title", ""),
        book_info.get("author", ""),
        book_info.get("isbn", ""),
        loc_cache
    )
    
    if success:
        # Add Google Books data
        if google_meta.get("google_genres"):
            enriched_data["google_genres"] = google_meta["google_genres"]
        
        if google_meta.get("publication_year"):
            enriched_data["publication_year"] = google_meta["publication_year"]
        
        if google_meta.get("series_name"):
            enriched_data["series_name"] = google_meta["series_name"]
        
        if google_meta.get("volume_number"):
            enriched_data["volume_number"] = google_meta["volume_number"]
    
    return enriched_data

def main():
    """Main function to process ISBN file"""
    print("Starting ISBN enrichment process...")
    
    # Load existing data
    extracted_data = load_extracted_data()
    
    # Read ISBN file
    with open("isbns_to_be_entered_2025088.txt", "r") as f:
        lines = f.readlines()
    
    # Skip header line
    isbn_lines = lines[1:]  # Skip "Books to be Entered into Atriuum:"
    
    print(f"Processing {len(isbn_lines)} ISBN entries...")
    
    # Load LOC cache
    loc_cache = {}
    if os.path.exists("loc_cache.json"):
        try:
            with open("loc_cache.json", "r") as f:
                loc_cache = json.load(f)
        except (json.JSONDecodeError, FileNotFoundError):
            pass
    
    processed_count = 0
    enriched_count = 0
    
    for i, line in enumerate(isbn_lines):
        book_info = process_isbn_line(line)
        if not book_info:
            continue
        
        # Generate a unique key for this entry
        if book_info["type"] == "isbn":
            key = f"ISBN_{book_info['isbn']}"
        else:
            # For NO ISBN entries, use a hash of title+author
            key = f"NOISBN_{hash(book_info['title'] + book_info['author']) & 0xFFFFFFFF}"
        
        # Skip if already processed
        if key in extracted_data:
            processed_count += 1
            continue
        
        print(f"Enriching {i+1}/{len(isbn_lines)}: {line.strip()}")
        
        # Enrich the book data
        enriched_data = enrich_book_data(book_info, loc_cache)
        
        # Add to extracted data
        extracted_data[key] = enriched_data
        enriched_count += 1
        
        # Save progress every 10 records
        if enriched_count % 10 == 0:
            save_extracted_data(extracted_data)
            print(f"Saved progress after {enriched_count} records")
        
        # Rate limiting
        time.sleep(0.5)
    
    # Final save
    save_extracted_data(extracted_data)
    
    print(f"\nISBN enrichment completed!")
    print(f"Processed: {len(isbn_lines)} entries")
    print(f"Enriched: {enriched_count} new records")
    print(f"Already processed: {processed_count} records")
    print(f"Total records in database: {len(extracted_data)}")

if __name__ == "__main__":
    main()