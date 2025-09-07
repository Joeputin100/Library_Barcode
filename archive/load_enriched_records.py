#!/usr/bin/env python3
"""
Load enriched B00 records into Flask review app database
"""

import json
import sqlite3
import os
from datetime import datetime

# Database configuration
DATABASE_PATH = "/data/data/com.termux/files/home/projects/barcode/review_app/data/reviews.db"
EXTRACTED_DATA_PATH = "/data/data/com.termux/files/home/projects/barcode/extracted_data.json"

def load_enriched_records():
    """Load enriched B00 records into the database"""
    
    # Load extracted data
    with open(EXTRACTED_DATA_PATH, 'r') as f:
        extracted_data = json.load(f)
    
    # Connect to database
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()
    
    # Get the highest existing record number to continue from
    cursor.execute("SELECT MAX(record_number) FROM records")
    max_record_number = cursor.fetchone()[0] or 0
    
    # Filter for B00 records
    b00_records = []
    for barcode, data in extracted_data.items():
        if barcode.startswith('B00') and isinstance(data, dict):
            b00_records.append({
                'barcode': barcode,
                'title': data.get('title', 'Unknown Title'),
                'author': data.get('author', 'Unknown Author'),
                'isbn': data.get('isbn', ''),
                'call_number': data.get('call_number', ''),
                'publication_year': data.get('publication_year', ''),
                'google_genres': data.get('google_genres', []),
                'series_name': data.get('series_name', ''),
                'volume_number': data.get('volume_number', '')
            })
    
    print(f"Found {len(b00_records)} B00 records to load")
    
    # Insert records into database
    for i, record in enumerate(b00_records, start=1):
        record_number = max_record_number + i
        
        # Create description from available data
        description_parts = []
        if record.get('publication_year'):
            description_parts.append(f"Published: {record['publication_year']}")
        if record.get('series_name'):
            series_info = f"Series: {record['series_name']}"
            if record.get('volume_number'):
                series_info += f" (Volume {record['volume_number']})"
            description_parts.append(series_info)
        if record.get('google_genres'):
            description_parts.append(f"Genres: {', '.join(record['google_genres'])}")
        
        description = "; ".join(description_parts) if description_parts else "No additional information available"
        
        # Insert record
        cursor.execute('''
            INSERT OR REPLACE INTO records 
            (record_number, title, author, isbn, status, price, call_number, description, notes, cover_url)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            record_number,
            record['title'],
            record['author'],
            record['isbn'],
            'pending',  # status
            0.0,        # price
            record['call_number'],
            description,
            f"Original barcode: {record['barcode']}",
            None        # cover_url
        ))
        
        if i % 50 == 0:
            print(f"Loaded {i} records...")
    
    conn.commit()
    conn.close()
    
    print(f"Successfully loaded {len(b00_records)} B00 records into database")
    print(f"Records now range from {max_record_number + 1} to {max_record_number + len(b00_records)}")

if __name__ == "__main__":
    load_enriched_records()