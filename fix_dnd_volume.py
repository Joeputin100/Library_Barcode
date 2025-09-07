#!/usr/bin/env python3
"""
Fix the D&D 3.5 volume number to use mixed number format
"""

import sqlite3
import json
from vertex_grounded_research import apply_research_to_record, extract_volume_number

def fix_dnd_volume():
    """Fix the D&D 3.5 volume number"""
    
    conn = sqlite3.connect('review_app/data/reviews.db')
    cursor = conn.cursor()
    
    # Get the D&D record
    cursor.execute('''
        SELECT id, enhanced_description FROM records 
        WHERE record_number = 400 AND enhanced_description LIKE '%VERTEX AI RESEARCH%'
    ''')
    
    record = cursor.fetchone()
    if not record:
        print("D&D record not found or no research data")
        return
    
    id, enhanced_description = record
    
    if 'VERTEX AI RESEARCH: ' in enhanced_description:
        research_json = enhanced_description.replace('VERTEX AI RESEARCH: ', '')
        try:
            research_results = json.loads(research_json)
            
            print("Fixing D&D 3.5 volume number...")
            print(f"Current series_volume: {research_results.get('enriched_data', {}).get('series_info', '')}")
            
            # Apply research with updated volume extraction
            updates_applied = apply_research_to_record(id, research_results, conn)
            
            print(f"✅ Updates applied: {updates_applied}")
            
            # Check the fixed volume
            cursor.execute('SELECT series_volume FROM records WHERE id = ?', (id,))
            new_volume = cursor.fetchone()[0]
            print(f"New series_volume: '{new_volume}'")
            
        except json.JSONDecodeError as e:
            print(f"JSON Error: {e}")
    
    conn.close()

if __name__ == "__main__":
    fix_dnd_volume()