#!/usr/bin/env python3
"""
Update database with complete MARC field data from enriched records
"""

import json
import sqlite3
from datetime import datetime

def update_marc_fields():
    """Update database with all available MARC field data"""
    
    # Load enriched data
    try:
        with open('enriched_data_full.json', 'r') as f:
            enriched_data = json.load(f)
        print(f"Loaded {len(enriched_data.get('enriched_records', []))} enriched records")
    except FileNotFoundError:
        print("❌ Enriched data file not found")
        return
    except json.JSONDecodeError:
        print("❌ Error parsing enriched data JSON")
        return
    
    # Connect to database
    conn = sqlite3.connect('review_app/data/reviews.db')
    cursor = conn.cursor()
    
    updated_count = 0
    
    for record in enriched_data.get('enriched_records', []):
        record_number = record.get('record_number')
        isbn = record.get('isbn', '')
        
        if not record_number:
            continue
        
        # Extract MARC field data
        marc_data = {
            'publisher': '',
            'publication_date': record.get('publication_year', ''),
            'physical_description': '',
            'subjects': ', '.join(record.get('google_genres', [])),
            'genre': ', '.join(record.get('google_genres', [])),
            'series': '',
            'series_volume': '',
            'language': 'en',  # Default to English since most books are in English
            'edition': '',
            'lccn': '',
            'dewey_decimal': ''
        }
        
        # Update database
        try:
            cursor.execute('''
                UPDATE records SET
                    publisher = COALESCE(?, publisher),
                    publication_date = COALESCE(?, publication_date), 
                    physical_description = COALESCE(?, physical_description),
                    subjects = COALESCE(?, subjects),
                    genre = COALESCE(?, genre),
                    series = COALESCE(?, series),
                    series_volume = COALESCE(?, series_volume),
                    language = COALESCE(?, language),
                    edition = COALESCE(?, edition),
                    lccn = COALESCE(?, lccn),
                    dewey_decimal = COALESCE(?, dewey_decimal)
                WHERE record_number = ?
            ''', (
                marc_data['publisher'],
                marc_data['publication_date'],
                marc_data['physical_description'], 
                marc_data['subjects'],
                marc_data['genre'],
                marc_data['series'],
                marc_data['series_volume'],
                marc_data['language'],
                marc_data['edition'],
                marc_data['lccn'],
                marc_data['dewey_decimal'],
                record_number
            ))
            
            if cursor.rowcount > 0:
                updated_count += 1
                
        except sqlite3.Error as e:
            print(f"❌ Error updating record {record_number}: {e}")
    
    # Commit changes
    conn.commit()
    conn.close()
    
    print(f"✅ Updated {updated_count} records with MARC field data")
    print("\nNote: Some MARC fields may still be missing because:")
    print("1. The enriched data doesn't contain all fields")
    print("2. We need to improve Mangle rules to extract more data from APIs")
    print("3. Some APIs may not provide certain fields")

if __name__ == "__main__":
    print("Updating database with complete MARC field data...")
    update_marc_fields()