#!/usr/bin/env python3
"""
Debug LOC API issue with test record
"""
import requests
import json
import logging
from loc_integration import LibraryOfCongressAPI

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def debug_loc_issue():
    """Debug why LOC didn't find data for test record"""
    
    test_record = {
        "barcode": "B000001",
        "title": "To Kill a Mockingbird",
        "author": "Harper Lee",
        "call_number": "FIC LEE",
        "lccn": "lc123456",  # This is probably fake
        "isbn": "9780061120084"  # Real ISBN for To Kill a Mockingbird
    }
    
    api = LibraryOfCongressAPI()
    
    logger.info(f"Testing record: {test_record}")
    
    # Test ISBN search
    logger.info("Testing ISBN search...")
    result = api.search_by_isbn(test_record['isbn'])
    logger.info(f"ISBN search result: {result}")
    
    # Test title/author search
    logger.info("Testing title/author search...")
    result = api.search_by_title_author(test_record['title'], test_record['author'])
    logger.info(f"Title/author search result: {result}")
    
    # Test LCCN search (this will likely fail since it's fake)
    logger.info("Testing LCCN search...")
    result = api.search_by_lccn(test_record['lccn'])
    logger.info(f"LCCN search result: {result}")
    
    # Let's also try a direct API call to see what's happening
    logger.info("Making direct LOC API call...")
    
    try:
        # Try the exact URL that should work
        url = f"https://www.loc.gov/books/?fo=json&at=results&q=isbn:{test_record['isbn']}"
        logger.info(f"Calling URL: {url}")
        
        response = requests.get(url, timeout=10, headers={
            'User-Agent': 'MangleEnrichment/1.0',
            'Accept': 'application/json'
        })
        
        logger.info(f"Response status: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            logger.info(f"Response data: {json.dumps(data, indent=2)}")
            
            if data.get('results') and len(data['results']) > 0:
                logger.info("Found results!")
                for i, result in enumerate(data['results'][:3]):
                    logger.info(f"Result {i}: {result.get('item', {}).get('title', 'No title')}")
            else:
                logger.info("No results found in response")
        else:
            logger.info(f"Error response: {response.text}")
            
    except Exception as e:
        logger.error(f"Direct API call failed: {e}")

if __name__ == "__main__":
    debug_loc_issue()