#!/usr/bin/env python3
"""
Find missing ISBNs using Google Books API search
"""

import sqlite3
import requests
import time

def search_google_books(title, author):
    """Search Google Books API for ISBN"""
    base_url = "https://www.googleapis.com/books/v1/volumes"
    
    # Clean up title for better search matching
    # Remove common subtitle indicators, special characters, and normalize
    clean_title = title
    if ':' in clean_title:
        clean_title = clean_title.split(':')[0].strip()  # Use main title before colon
    clean_title = clean_title.replace('"', '').replace("'", "").replace('(', '').replace(')', '')
    
    # Build search query with cleaned title
    query = f'intitle:"{clean_title}"'
    if author:
        query += f'+inauthor:"{author}"'
    
    params = {
        'q': query,
        'maxResults': 5,
        'printType': 'books'
    }
    
    try:
        response = requests.get(base_url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        if 'items' in data and data['items']:
            for item in data['items']:
                volume_info = item.get('volumeInfo', {})
                
                # Check industry identifiers for ISBN
                identifiers = volume_info.get('industryIdentifiers', [])
                for identifier in identifiers:
                    if identifier.get('type') == 'ISBN_13':
                        return identifier.get('identifier')
                    elif identifier.get('type') == 'ISBN_10':
                        return identifier.get('identifier')
        
        return None
        
    except Exception as e:
        print(f"Google Books search error for '{title}': {e}")
        return None

def find_missing_isbns():
    """Find and add missing ISBNs for records"""
    
    conn = sqlite3.connect('review_app/data/reviews.db')
    cursor = conn.cursor()
    
    # Get records with missing ISBNs
    cursor.execute("""
        SELECT record_number, title, author, isbn 
        FROM records 
        WHERE (isbn IS NULL OR isbn = '' OR isbn = 'Unable to verify') 
        AND record_number BETWEEN 1 AND 7
    """)
    
    records = cursor.fetchall()
    
    for record_number, title, author, current_isbn in records:
        print(f"Searching for ISBN: Record #{record_number} - {title} by {author}")
        
        # Search Google Books
        isbn = search_google_books(title, author)
        
        if isbn:
            print(f"Found ISBN {isbn} for Record #{record_number}")
            cursor.execute("UPDATE records SET isbn = ? WHERE record_number = ?", (isbn, record_number))
        else:
            print(f"No ISBN found for Record #{record_number}")
        
        # Be respectful of API rate limits
        time.sleep(1)
    
    conn.commit()
    conn.close()
    
    print("Completed ISBN search for missing records")

if __name__ == "__main__":
    find_missing_isbns()