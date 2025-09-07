#!/usr/bin/env python3
"""
Debug specific price extraction cases
"""

import re

def debug_price_extraction():
    """Debug why range prices aren't being extracted"""
    
    test_text = "Mass Market Paperback (ISBN 9780804152563): New copies typically range from $7.99 - $8.99 (list price). Used copies are widely available, often priced from $0.01 (plus shipping) to $5, depending on condition and seller."
    
    print("Debugging price extraction:")
    print(f"Text: {test_text}")
    print()
    
    # Test each pattern
    patterns = [
        r'\$(\d+\.?\d*)\s*-\s*\$(\d+\.?\d*)',  # $7.99 - $8.99
        r'\$(\d+\.?\d*)\s*to\s*\$(\d+\.?\d*)',  # $10 to $50
        r'range.*?\$(\d+\.?\d*)\s*-\s*\$(\d+\.?\d*)',  # ranges from $10 to $50
        r'\$(\d+\.?\d*)',  # $12.99, $5
    ]
    
    for i, pattern in enumerate(patterns, 1):
        matches = re.findall(pattern, test_text, re.IGNORECASE)
        print(f"Pattern {i}: {pattern}")
        print(f"Matches: {matches}")
        print()

if __name__ == "__main__":
    debug_price_extraction()