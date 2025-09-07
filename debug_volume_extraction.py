#!/usr/bin/env python3
"""
Debug volume extraction with actual research data
"""

import sqlite3
import json
from vertex_grounded_research import extract_volume_number

def debug_volume_extraction():
    """Debug volume extraction with real research data"""
    
    conn = sqlite3.connect('review_app/data/reviews.db')
    cursor = conn.cursor()
    
    # Test with manga records
    cursor.execute('''
        SELECT record_number, title, enhanced_description 
        FROM records 
        WHERE publisher LIKE '%Viz%' 
        AND enhanced_description LIKE '%VERTEX AI RESEARCH%'
        LIMIT 5
    ''')
    
    records = cursor.fetchall()
    
    for record_number, title, enhanced_description in records:
        print(f"\n📚 Record #{record_number}: {title}")
        print("-" * 50)
        
        if 'VERTEX AI RESEARCH: ' in enhanced_description:
            research_json = enhanced_description.replace('VERTEX AI RESEARCH: ', '')
            try:
                research_results = json.loads(research_json)
                
                # Check series_info
                enriched = research_results.get('enriched_data', {})
                series_info = enriched.get('series_info', '')
                print(f"Series Info: {series_info}")
                
                # Extract volume from series_info
                volume_from_series = extract_volume_number(series_info)
                print(f"Volume from series_info: {volume_from_series}")
                
                # Check title
                verified = research_results.get('verified_data', {})
                verified_title = verified.get('title', '')
                print(f"Verified Title: {verified_title}")
                
                # Extract volume from title
                volume_from_title = extract_volume_number(verified_title)
                print(f"Volume from title: {volume_from_title}")
                
                # Check if we found any volume
                if volume_from_series or volume_from_title:
                    final_volume = volume_from_series or volume_from_title
                    print(f"✅ Would set series_volume to: {final_volume}")
                else:
                    print("❌ No volume found")
                
            except json.JSONDecodeError as e:
                print(f"JSON Error: {e}")
    
    conn.close()

if __name__ == "__main__":
    debug_volume_extraction()