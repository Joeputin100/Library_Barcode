#!/usr/bin/env python3
"""
Check where description data is located in research results
"""

import sqlite3
import json

def check_description_location():
    """Check where description data is stored in research results"""
    
    conn = sqlite3.connect('review_app/data/reviews.db')
    cursor = conn.cursor()
    
    # Check multiple records to see description location patterns
    for record_number in [1, 3, 22]:
        cursor.execute('''
            SELECT enhanced_description FROM records 
            WHERE record_number = ? AND enhanced_description LIKE '%VERTEX AI RESEARCH%'
        ''', (record_number,))
        
        result = cursor.fetchone()
        if result:
            enhanced_description = result[0]
            if 'VERTEX AI RESEARCH: ' in enhanced_description:
                research_json = enhanced_description.replace('VERTEX AI RESEARCH: ', '')
                try:
                    research_results = json.loads(research_json)
                    
                    print(f"\nRecord #{record_number}:")
                    print("=" * 30)
                    
                    # Check verified_data
                    verified = research_results.get('verified_data', {})
                    print(f"Verified data keys: {list(verified.keys())}")
                    if 'description' in verified:
                        print(f"Description in verified_data: {verified['description']}")
                    
                    # Check enriched_data  
                    enriched = research_results.get('enriched_data', {})
                    print(f"Enriched data keys: {list(enriched.keys())}")
                    
                    # Check contextual_data
                    contextual = research_results.get('contextual_data', {})
                    print(f"Contextual data keys: {list(contextual.keys())}")
                    
                    # Check if description might be in a different format
                    print(f"Research keys: {list(research_results.keys())}")
                    
                except json.JSONDecodeError as e:
                    print(f"JSON decode error for record #{record_number}: {e}")
    
    conn.close()

if __name__ == "__main__":
    check_description_location()