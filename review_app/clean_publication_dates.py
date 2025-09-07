#!/usr/bin/env python3
"""
Clean publication date field to extract only 4-digit year (YYYY)
"""

import sqlite3
import re

def clean_publication_dates():
    """Clean publication dates to extract only 4-digit years"""
    
    conn = sqlite3.connect('data/reviews.db')
    cursor = conn.cursor()
    
    # Get all records with publication dates
    cursor.execute("SELECT record_number, publication_date FROM records WHERE publication_date IS NOT NULL")
    records = cursor.fetchall()
    
    updated_count = 0
    
    for record_number, pub_date in records:
        if not pub_date:
            continue
            
        # Extract 4-digit year using regex
        year_match = re.search(r'\b(19\d{2}|20\d{2})\b', str(pub_date))
        
        if year_match:
            clean_year = year_match.group(1)
            
            # Update the record with clean year
            cursor.execute(
                "UPDATE records SET publication_date = ? WHERE record_number = ?",
                (clean_year, record_number)
            )
            
            if cursor.rowcount > 0:
                print(f"✅ Record #{record_number}: '{pub_date}' → '{clean_year}'")
                updated_count += 1
        else:
            print(f"❌ Record #{record_number}: Could not extract year from '{pub_date}'")
    
    conn.commit()
    conn.close()
    
    print(f"✅ Cleaned {updated_count} publication dates to 4-digit years")
    return updated_count

if __name__ == "__main__":
    clean_publication_dates()