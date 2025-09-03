#!/usr/bin/env python3
"""
Debug LOC API with a known working book from cache
"""
import requests
import json
import logging
from loc_integration import LibraryOfCongressAPI

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def debug_loc_with_known_book():
    """Debug LOC with a book we know works from cache"""
    
    # Use the book that's already in cache
    known_record = {
        "barcode": "B12345",
        "title": "To Your Eternity 12",
        "author": "Yoshitoki Oima",
        "call_number": "",
        "lccn": "",
        "isbn": "9781632367990"  # ISBN from the cache
    }
    
    api = LibraryOfCongressAPI()
    
    logger.info(f"Testing known record: {known_record}")
    
    # Test ISBN search
    logger.info("Testing ISBN search...")
    result = api.search_by_isbn(known_record['isbn'])
    logger.info(f"ISBN search result: {result}")
    
    # Test title/author search
    logger.info("Testing title/author search...")
    result = api.search_by_title_author(known_record['title'], known_record['author'])
    logger.info(f"Title/author search result: {result}")
    
    # Let's also try a direct API call
    logger.info("Making direct LOC API call...")
    
    try:
        # Try both ISBN and title/author searches
        urls = [
            f"https://www.loc.gov/books/?fo=json&at=results&q=isbn:{known_record['isbn']}",
            f"https://www.loc.gov/books/?fo=json&at=results&q=title:{known_record['title']}",
            f"https://www.loc.gov/books/?fo=json&at=results&q=title:{known_record['title']}+author:{known_record['author']}"
        ]
        
        for i, url in enumerate(urls):
            logger.info(f"Calling URL {i+1}: {url}")
            
            response = requests.get(url, timeout=10, headers={
                'User-Agent': 'MangleEnrichment/1.0',
                'Accept': 'application/json'
            })
            
            logger.info(f"Response status: {response.status_code}")
            
            if response.status_code == 200:
                data = response.json()
                logger.info(f"Results found: {len(data.get('results', []))}")
                
                if data.get('results') and len(data['results']) > 0:
                    for j, result in enumerate(data['results'][:2]):
                        title = result.get('item', {}).get('title', 'No title')
                        logger.info(f"Result {j+1}: {title}")
                else:
                    logger.info("No results found")
            else:
                logger.info(f"Error response: {response.text}")
            
            print()  # Blank line between attempts
            
    except Exception as e:
        logger.error(f"Direct API call failed: {e}")

if __name__ == "__main__":
    debug_loc_with_known_book()