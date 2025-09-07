#!/usr/bin/env python3
"""
Test price extraction with dictionary format current_value
"""

from price_extraction import extract_price_from_research

def test_dict_price():
    """Test price extraction with dictionary current_value"""
    
    # Simulate the problematic record's market_data
    market_data = {
        'current_value': {
            'ebook_price_approx': '£35.99 / $49.46',
            'hardback_price': '£120.00 / $160.00', 
            'paperback_price': '£39.99 / $54.95'
        },
        'availability': 'The book is widely available...',
        'editions': []
    }
    
    price = extract_price_from_research(market_data)
    print(f"Extracted price: ${price:.2f}")
    print(f"Market data: {market_data}")

if __name__ == "__main__":
    test_dict_price()