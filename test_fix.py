#!/usr/bin/env python3
"""
Test script to verify the title/author fix works correctly
"""

import sys
sys.path.append('.')

from api_calls import get_book_metadata_initial_pass
from caching import load_cache

def test_unknown_title_fix():
    """Test that unknown titles/authors are properly corrected"""
    
    # Load cache
    cache = load_cache()
    
    # Test case: ISBN 978-1-7896-6308-2 with unknown title/author
    print("Testing ISBN 978-1-7896-6308-2 with unknown title/author...")
    
    metadata, google_cached, loc_cached, openlibrary_success, loc_success, vertex_ai_success = \
        get_book_metadata_initial_pass(
            "Unknown Title",  # Original title
            "Unknown Author", # Original author  
            "978-1-7896-6308-2", # ISBN
            "", # LCCN
            cache,
            is_blank=False,
            is_problematic=False
        )
    
    print(f"Original: Title='Unknown Title', Author='Unknown Author'")
    print(f"Corrected: Title='{metadata.get('title', 'MISSING')}', Author='{metadata.get('author', 'MISSING')}'")
    print(f"Google cached: {google_cached}")
    print(f"Google success: {openlibrary_success}")
    
    # Check if correction worked
    expected_title = "Confident Coding: How to Write Code and Futureproof Your Career"
    expected_author = "Rob Percival and Darren Woods"
    
    if metadata.get('title') == expected_title and metadata.get('author') == expected_author:
        print("✅ SUCCESS: Title and author correctly fixed!")
        return True
    else:
        print("❌ FAILED: Title/author not corrected properly")
        print(f"Expected: Title='{expected_title}', Author='{expected_author}'")
        return False

if __name__ == "__main__":
    success = test_unknown_title_fix()
    sys.exit(0 if success else 1)