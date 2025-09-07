#!/usr/bin/env python3
"""
Test new LOC SRU integration
"""
import logging
from loc_integration import LibraryOfCongressAPI

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_loc_sru():
    """Test the new LOC SRU integration"""
    
    api = LibraryOfCongressAPI()
    
    # Test with a known book that should be in LOC
    test_cases = [
        {
            "name": "To Kill a Mockingbird",
            "isbn": "9780061120084",
            "title": "To Kill a Mockingbird",
            "author": "Harper Lee"
        },
        {
            "name": "Known manga from cache", 
            "isbn": "9781632367990",
            "title": "To Your Eternity 12",
            "author": "Yoshitoki Oima"
        }
    ]
    
    for test_case in test_cases:
        logger.info(f"\n=== Testing: {test_case['name']} ===")
        
        # Test ISBN search
        logger.info("Testing ISBN search...")
        result = api.search_by_isbn(test_case['isbn'])
        logger.info(f"ISBN search result: {result is not None}")
        if result:
            logger.info(f"Title: {result.get('title', 'No title')}")
            logger.info(f"Author: {result.get('author', 'No author')}")
            logger.info(f"Classification: {result.get('classification', 'No classification')}")
        
        # Test title/author search
        logger.info("Testing title/author search...")
        result = api.search_by_title_author(test_case['title'], test_case['author'])
        logger.info(f"Title/author search result: {result is not None}")
        if result:
            logger.info(f"Title: {result.get('title', 'No title')}")
            logger.info(f"Author: {result.get('author', 'No author')}")
            logger.info(f"Classification: {result.get('classification', 'No classification')}")
        
        print("-" * 50)

if __name__ == "__main__":
    test_loc_sru()