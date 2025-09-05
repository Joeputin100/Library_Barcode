#!/usr/bin/env python3
"""
Test fixed Google Books enrichment with small batch
"""

import sqlite3
import requests
import re
import time
import os

def test_fixed_enrichment():
    """Test the fixed Google Books enrichment with a small batch"""
    
    # Connect to database
    conn = sqlite3.connect('review_app/data/reviews.db')
    cursor = conn.cursor()
    
    # Get first 5 records needing enrichment
    cursor.execute('''
        SELECT record_number, title, author, isbn, publisher, physical_description, description
        FROM records 
        WHERE (publisher = '' OR publisher = 'None' OR publisher IS NULL) OR
              (physical_description = '' OR physical_description = 'None' OR physical_description IS NULL) OR
              (description = '' OR description = 'None' OR description IS NULL)
        ORDER BY record_number
        LIMIT 5
    ''')
    
    test_records = cursor.fetchall()
    print(f"Testing fixed enrichment on {len(test_records)} records")
    
    for i, record in enumerate(test_records):
        record_number, title, author, isbn, current_publisher, current_physical_desc, current_description = record
        
        print(f"\n--- Testing Record #{record_number}: {title} ---")
        print(f"Current - Publisher: '{current_publisher}', Physical Desc: '{current_physical_desc}', Description: '{current_description}'")
        
        # Query Google Books API (fixed version)
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
            
            print(f"API Call: {query}")
            
            response = requests.get(url, timeout=15)
            response.raise_for_status()
            
            data = response.json()
            
            if "items" in data and data["items"]:
                item = data["items"][0]
                volume_info = item.get("volumeInfo", {})
                
                # Extract fields
                new_publisher = volume_info.get("publisher", "")
                new_physical_desc = f"{volume_info.get('pageCount', '')} pages ; 24 cm" if volume_info.get("pageCount") else ""
                new_description = volume_info.get("description", "")[:500] if volume_info.get("description") else ""
                
                print(f"✅ Google Books Results:")
                print(f"   Publisher: '{new_publisher}'")
                print(f"   Physical Desc: '{new_physical_desc}'")
                print(f"   Description: '{new_description[:100]}...'")
                
                # Update database
                updates = []
                params = []
                
                if new_publisher and (not current_publisher or current_publisher == 'None'):
                    updates.append("publisher = ?")
                    params.append(new_publisher)
                if new_physical_desc and (not current_physical_desc or current_physical_desc == 'None'):
                    updates.append("physical_description = ?")
                    params.append(new_physical_desc)
                if new_description and (not current_description or current_description == 'None'):
                    updates.append("description = ?")
                    params.append(new_description)
                
                if updates:
                    params.append(record_number)
                    cursor.execute(f"""
                        UPDATE records SET {', '.join(updates)}
                        WHERE record_number = ?
                    """, params)
                    conn.commit()
                    print(f"   ✅ Updated {len(updates)} fields")
                else:
                    print("   ℹ️  No updates needed")
                    
            else:
                print("❌ No results found in Google Books")
                
        except Exception as e:
            print(f"❌ Error: {e}")
            
        # Brief delay between requests
        time.sleep(1)
    
    conn.close()
    print("\n🎉 Fixed enrichment test completed!")

if __name__ == "__main__":
    print("🧪 Testing Fixed Google Books Enrichment")
    print("=" * 50)
    test_fixed_enrichment()