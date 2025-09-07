#!/usr/bin/env python3
"""
Minimal test enrichment for a few records
"""

import sqlite3
import requests
import re
import time

def test_single_record_enrichment(record_number):
    """Test enrichment for a single record"""
    
    conn = sqlite3.connect('review_app/data/reviews.db')
    cursor = conn.cursor()
    
    # Get the record
    cursor.execute('''
        SELECT record_number, title, author, isbn, publisher, physical_description, description
        FROM records WHERE record_number = ?
    ''', (record_number,))
    
    record = cursor.fetchone()
    if not record:
        print(f"Record {record_number} not found")
        return
    
    record_number, title, author, isbn, current_publisher, current_physical_desc, current_description = record
    
    print(f"Testing enrichment for Record #{record_number}: {title} by {author}")
    print(f"Current - Publisher: '{current_publisher}', Physical Desc: '{current_physical_desc}', Description: '{current_description}'")
    
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
        
        print(f"Query: {query}")
        
        url = f"https://www.googleapis.com/books/v1/volumes?q={query}&maxResults=1"
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        
        data = response.json()
        print(f"API Response: {data.get('totalItems', 0)} items found")
        
        if "items" in data and data["items"]:
            item = data["items"][0]
            volume_info = item.get("volumeInfo", {})
            
            # Extract fields
            new_publisher = volume_info.get("publisher", "")
            new_physical_desc = f"{volume_info.get('pageCount', '')} pages ; 24 cm" if volume_info.get("pageCount") else ""
            new_description = volume_info.get("description", "")[:200] + "..." if volume_info.get("description") else ""
            
            print(f"Google Books - Publisher: '{new_publisher}'")
            print(f"Google Books - Physical Desc: '{new_physical_desc}'")
            print(f"Google Books - Description: '{new_description}'")
            
            # Update if we found better data
            updates = []
            if new_publisher and (not current_publisher or current_publisher == 'None'):
                safe_publisher = new_publisher.replace("'", "''")
                updates.append(f"publisher = '{safe_publisher}'")
            if new_physical_desc and (not current_physical_desc or current_physical_desc == 'None'):
                safe_physical_desc = new_physical_desc.replace("'", "''")
                updates.append(f"physical_description = '{safe_physical_desc}'")
            if new_description and (not current_description or current_description == 'None'):
                safe_description = new_description.replace("'", "''")
                updates.append(f"description = '{safe_description}'")
            
            if updates:
                update_sql = f"UPDATE records SET {', '.join(updates)} WHERE record_number = {record_number}"
                cursor.execute(update_sql)
                conn.commit()
                print(f"✅ Updated record {record_number} with {len(updates)} fields")
            else:
                print("ℹ️  No updates needed")
                
        else:
            print("❌ No results found in Google Books")
            
    except Exception as e:
        print(f"❌ Error: {e}")
    
    conn.close()

if __name__ == "__main__":
    print("🧪 Testing minimal enrichment")
    print("=" * 40)
    
    # Test with a few records
    test_records = [1, 2, 3, 22]  # Record #1 and a few others
    
    for record_number in test_records:
        print(f"\n--- Testing Record #{record_number} ---")
        test_single_record_enrichment(record_number)
        print("-" * 30)
        time.sleep(1)  # Brief delay between requests
    
    print("\n🎉 Test enrichment complete!")