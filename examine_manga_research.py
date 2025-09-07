#!/usr/bin/env python3
"""
Examine raw research results for manga records to find volume/series information
"""

import sqlite3
import json

def examine_manga_research():
    """Examine research results for manga records"""
    
    conn = sqlite3.connect('review_app/data/reviews.db')
    cursor = conn.cursor()
    
    # Get manga records with research data
    cursor.execute('''
        SELECT record_number, title, publisher, series, series_volume, enhanced_description 
        FROM records 
        WHERE (publisher LIKE '%Viz%' OR publisher LIKE '%Yen Press%') 
        AND enhanced_description LIKE '%VERTEX AI RESEARCH%'
        LIMIT 10
    ''')
    
    records = cursor.fetchall()
    
    for record_number, title, publisher, series, series_volume, enhanced_description in records:
        print(f"\n📚 Record #{record_number}: {title}")
        print(f"   Publisher: {publisher}")
        print(f"   Series: {series}")
        print(f"   Series Volume: {series_volume}")
        print("-" * 60)
        
        if 'VERTEX AI RESEARCH: ' in enhanced_description:
            research_json = enhanced_description.replace('VERTEX AI RESEARCH: ', '')
            try:
                research_results = json.loads(research_json)
                
                # Check where volume information might be
                verified = research_results.get('verified_data', {})
                enriched = research_results.get('enriched_data', {})
                contextual = research_results.get('contextual_data', {})
                
                print("Research Structure:")
                print(f"   Verified keys: {list(verified.keys())}")
                print(f"   Enriched keys: {list(enriched.keys())}")
                
                # Check for series info specifically
                if enriched.get('series_info'):
                    print(f"   Series Info: {enriched['series_info']}")
                
                # Check physical description which might contain volume info
                if enriched.get('physical_description'):
                    print(f"   Physical Desc: {enriched['physical_description']}")
                
                # Check if volume info is in the title itself
                if verified.get('title'):
                    print(f"   Verified Title: {verified['title']}")
                
            except json.JSONDecodeError as e:
                print(f"   JSON Error: {e}")
        else:
            print("   No research data found")
    
    conn.close()

if __name__ == "__main__":
    examine_manga_research()