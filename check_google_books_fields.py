#!/usr/bin/env python3
"""
Check what MARC fields Google Books API provides for a specific ISBN
"""

import requests
import os
import json

def check_google_books_fields(isbn):
    """Check what fields Google Books API returns for a given ISBN"""
    
    api_key = os.environ.get("GOOGLE_API_KEY", "")
    if not api_key:
        print("❌ Google API key not found")
        return
    
    url = f"https://www.googleapis.com/books/v1/volumes?q=isbn:{isbn}&maxResults=1&key={api_key}"
    
    try:
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        data = response.json()
        
        print(f"Google Books API response for ISBN {isbn}:")
        
        if "items" in data and data["items"]:
            item = data["items"][0]
            volume_info = item.get("volumeInfo", {})
            
            print("\n📚 Available MARC fields:")
            print(f"Title: {volume_info.get('title', 'MISSING')}")
            print(f"Subtitle: {volume_info.get('subtitle', 'MISSING')}")
            print(f"Authors: {volume_info.get('authors', 'MISSING')}")
            print(f"Publisher: {volume_info.get('publisher', 'MISSING')}")
            print(f"Published Date: {volume_info.get('publishedDate', 'MISSING')}")
            print(f"Description: {volume_info.get('description', 'MISSING')[:100]}..." if volume_info.get('description') else "Description: MISSING")
            print(f"Page Count: {volume_info.get('pageCount', 'MISSING')}")
            print(f"Categories: {volume_info.get('categories', 'MISSING')}")
            print(f"Language: {volume_info.get('language', 'MISSING')}")
            print(f"ISBNs: {[id['identifier'] for id in volume_info.get('industryIdentifiers', [])] if volume_info.get('industryIdentifiers') else 'MISSING'}")
            
            # Check for series information
            if "seriesInfo" in volume_info:
                series_info = volume_info["seriesInfo"]
                print(f"Series: {series_info.get('series', [{}])[0].get('title', 'MISSING') if series_info.get('series') else 'MISSING'}")
                print(f"Volume: {series_info.get('bookDisplayNumber', 'MISSING')}")
            else:
                print("Series: MISSING")
                print("Volume: MISSING")
                
        else:
            print("❌ No items found in response")
            
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    isbn = "978-1-7896-6308-2"  # Confident Coding
    print(f"Checking Google Books fields for ISBN: {isbn}")
    check_google_books_fields(isbn)