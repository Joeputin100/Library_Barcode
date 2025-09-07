#!/usr/bin/env python3
"""
Check if price data exists in Vertex AI research results
"""

import sqlite3
import json

def check_price_data():
    """Check for price data in research results"""
    
    conn = sqlite3.connect('review_app/data/reviews.db')
    cursor = conn.cursor()
    
    # Check multiple records to see price data patterns
    cursor.execute('''
        SELECT record_number, title, enhanced_description FROM records 
        WHERE enhanced_description LIKE '%VERTEX AI RESEARCH%'
        LIMIT 10
    ''')
    
    records = cursor.fetchall()
    
    for record_number, title, enhanced_description in records:
        print(f"\n📚 Record #{record_number}: {title}")
        print("-" * 50)
        
        if 'VERTEX AI RESEARCH: ' in enhanced_description:
            research_json = enhanced_description.replace('VERTEX AI RESEARCH: ', '')
            try:
                research_results = json.loads(research_json)
                
                # Check market_data section for pricing
                market_data = research_results.get('market_data', {})
                print(f"Market data keys: {list(market_data.keys())}")
                
                if 'current_value' in market_data:
                    print(f"Current value: {market_data['current_value']}")
                
                # Check if there's any pricing information
                if market_data:
                    for key, value in market_data.items():
                        if value and value != "Unable to determine":
                            print(f"   {key}: {value}")
                else:
                    print("   No market data found")
                
            except json.JSONDecodeError as e:
                print(f"JSON Error: {e}")
    
    conn.close()

if __name__ == "__main__":
    check_price_data()