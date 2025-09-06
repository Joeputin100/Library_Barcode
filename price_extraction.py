#!/usr/bin/env python3
"""
Price extraction and normalization functions
"""

import re

def extract_price_from_research(market_data):
    """
    Extract and normalize price from market_data research results
    Returns replacement cost for a Very Good quality used copy, minimum $10
    ASSUMPTION: Our copy is NOT collectible, NOT signed, NOT limited edition
    We want the cost to buy a new or Very Good condition used copy of same title
    """
    if not market_data:
        return 10.0  # Default minimum price
    
    current_value = market_data.get('current_value', '')
    
    # Handle dictionary format current_value
    if isinstance(current_value, dict):
        # Convert dictionary values to a string for processing
        current_value = ' '.join([f"{k}: {v}" for k, v in current_value.items()])
    
    # If unable to determine or free distribution, use educated guess
    if 'Unable to determine' in current_value or 'No verifiable market value' in current_value:
        return 10.0  # Minimum price for any book
    
    # Check for free distribution
    if 'free' in current_value.lower() or 'distributed free' in current_value.lower():
        return 10.0  # Minimum price even for free books
    
    # Extract prices with context awareness
    prices_found = []
    typical_prices = []  # Prices from "typical", "general", "used" contexts
    collector_prices = []  # Prices from "first edition", "collector", "rare" contexts
    
    # Patterns for different contexts - prioritize replacement cost pricing
    replacement_patterns = [
        r'used.*?\$(\d+\.?\d*)',
        r'general.*?\$(\d+\.?\d*)',
        r'typically.*?\$(\d+\.?\d*)',
        r'generally.*?\$(\d+\.?\d*)',
        r'standard.*?\$(\d+\.?\d*)',
        r'paperback.*?\$(\d+\.?\d*)',
        r'new.*?\$(\d+\.?\d*)',
        r'replacement.*?\$(\d+\.?\d*)',
        r'very good.*?\$(\d+\.?\d*)',
        r'good condition.*?\$(\d+\.?\d*)',
    ]
    
    # Patterns to AVOID - collector/rare/special editions
    collector_patterns = [
        r'first edition.*?\$(\d+\.?\d*)',
        r'collector.*?\$(\d+\.?\d*)',
        r'rare.*?\$(\d+\.?\d*)',
        r'signed.*?\$(\d+\.?\d*)',
        r'fine condition.*?\$(\d+\.?\d*)',
        r'mint condition.*?\$(\d+\.?\d*)',
        r'exceeding.*?\$(\d+\.?\d*)',
        r'command.*?\$(\d+\.?\d*)',
        r'thousands.*?\$(\d+\.?\d*)',
        r'special edition.*?\$(\d+\.?\d*)',
        r'limited edition.*?\$(\d+\.?\d*)',
    ]
    
    # Extract prices using string methods instead of regex (due to regex issues)
    # Look for dollar amounts in the text
    text = str(current_value)
    
    # Simple string-based price extraction
    i = 0
    while i < len(text):
        if text[i] == '$':
            # Found dollar sign, extract the number
            j = i + 1
            while j < len(text) and (text[j].isdigit() or text[j] == '.'):
                j += 1
            
            if j > i + 1:  # Found at least one digit after $
                price_str = text[i+1:j]
                try:
                    price = float(price_str)
                    prices_found.append(price)
                except ValueError:
                    pass
            i = j
        else:
            i += 1
    
    # Classify prices based on context - prioritize replacement cost patterns
    for pattern in replacement_patterns:
        matches = re.findall(pattern, current_value, re.IGNORECASE)
        for match in matches:
            try:
                price = float(match)
                typical_prices.append(price)
            except ValueError:
                continue
    
    for pattern in collector_patterns:
        matches = re.findall(pattern, current_value, re.IGNORECASE)
        for match in matches:
            try:
                price = float(match)
                collector_prices.append(price)
            except ValueError:
                continue
    
    # Also check editions for pricing
    editions = market_data.get('editions', [])
    for edition in editions:
        if isinstance(edition, dict):
            price_str = edition.get('price', '')
            if price_str:
                price_matches = re.findall(r'\$(\d+\.?\d*)', price_str)
                for match in price_matches:
                    try:
                        price = float(match)
                        prices_found.append(price)
                        # Assume edition prices are typical unless specified otherwise
                        typical_prices.append(price)
                    except ValueError:
                        continue
    
    # Prioritization logic: Use reasonable prices for replacement cost
    # Focus on prices that represent typical retail copies (not collector items)
    
    # First, filter out obviously collector prices (over $100)
    reasonable_prices = [p for p in prices_found if p <= 100]
    
    if reasonable_prices:
        # Use the lower end of reasonable prices for replacement cost
        reasonable_prices.sort()
        
        # Prefer prices in the $10-50 range for typical books
        normal_range_prices = [p for p in reasonable_prices if 10 <= p <= 50]
        
        if normal_range_prices:
            # Use the lower end of normal range prices
            return max(min(normal_range_prices), 10.0)
        else:
            # Use the minimum reasonable price found
            return max(min(reasonable_prices), 10.0)
    
    elif prices_found:
        # If only collector prices found, use the minimum but flag as potentially high
        prices_found.sort()
        min_price = min(prices_found)
        
        # For collector items, cap at $100 maximum for replacement cost
        return max(min(min_price, 100.0), 10.0)
    
    # If no prices found but book exists, use educated guess
    availability = market_data.get('availability', '')
    if availability:
        # Handle both string and dictionary availability data
        if isinstance(availability, str) and 'available' in availability.lower():
            # Book is available but no specific price - use $15 as educated guess
            return 15.0
        elif isinstance(availability, dict):
            # If availability is a dictionary, check if it indicates availability
            availability_str = str(availability).lower()
            if 'available' in availability_str:
                return 15.0
    
    # Default minimum price
    return 10.0

def test_price_extraction():
    """Test price extraction with sample market data"""
    
    test_cases = [
        # Record #1 - Has explicit pricing
        {
            'current_value': 'Mass Market Paperback (ISBN 9780804152563): New copies typically range from $7.99 - $8.99 (list price). Used copies are widely available, often priced from $0.01 (plus shipping) to $5, depending on condition and seller.',
            'availability': 'Widely available...',
            'editions': []
        },
        # Record #2 - Unable to determine
        {
            'current_value': 'Unable to determine. No record of the book\'s existence or market presence found.',
            'availability': 'Unavailable...',
            'editions': []
        },
        # Record #3 - Free distribution but should have minimum price
        {
            'current_value': 'The book is primarily distributed as a free digital PDF/eBook... Physical copies typically range from under $5 to $20...',
            'availability': 'Highly available as a free PDF/eBook...',
            'editions': []
        },
        # Record #8 - Explicit pricing
        {
            'current_value': 'Paperback: $12.99 - $13.99 USD (Amazon.com, High); Ebook: $4.99 USD (Amazon.com, High)',
            'availability': 'Currently available for purchase...',
            'editions': []
        },
        # Record with edition pricing
        {
            'current_value': 'Various prices',
            'availability': 'Available',
            'editions': [
                {'price': 'From $12.99 USD', 'format': 'Paperback'},
                {'price': '$4.99 USD', 'format': 'eBook'}
            ]
        }
    ]
    
    print("Testing Price Extraction:")
    print("=" * 50)
    
    for i, market_data in enumerate(test_cases, 1):
        price = extract_price_from_research(market_data)
        print(f"Test Case {i}: ${price:.2f}")
        print(f"   Current Value: {market_data['current_value'][:100]}...")
        if market_data['editions']:
            print(f"   Editions with pricing: {len(market_data['editions'])}")
        print()

if __name__ == "__main__":
    test_price_extraction()