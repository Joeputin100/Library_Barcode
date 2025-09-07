#!/usr/bin/env python3
"""
Fix subjects and genre fields for records with good research data
"""

import sqlite3
import json
from vertex_grounded_research import apply_research_to_record

def fix_record_fields(record_number):
    """Fix fields for a specific record"""
    
    conn = sqlite3.connect('review_app/data/reviews.db')
    cursor = conn.cursor()
    
    # Get the record with research data
    cursor.execute('''
        SELECT id, enhanced_description FROM records 
        WHERE record_number = ? AND enhanced_description LIKE '%VERTEX AI RESEARCH%'
    ''', (record_number,))
    
    record = cursor.fetchone()
    if not record:
        print(f"Record #{record_number} not found or no research data")
        return
    
    id, enhanced_description = record
    
    # Extract research results from enhanced_description
    if 'VERTEX AI RESEARCH: ' in enhanced_description:
        research_json = enhanced_description.replace('VERTEX AI RESEARCH: ', '')
        try:
            research_results = json.loads(research_json)
            
            print(f"Fixing Record #{record_number}")
            print(f"Research data found, applying updates...")
            
            # Apply research to update subjects and genre fields
            updates_applied = apply_research_to_record(id, research_results, conn)
            
            print(f"✅ Updates applied: {updates_applied}")
            
            # Verify the update
            cursor.execute('SELECT subjects, genre FROM records WHERE id = ?', (id,))
            subjects, genre = cursor.fetchone()
            
            print(f"After fix:")
            print(f"   Subjects: '{subjects}'")
            print(f"   Genre: '{genre}'")
            
        except json.JSONDecodeError as e:
            print(f"JSON decode error: {e}")
    
    conn.close()

if __name__ == "__main__":
    # Fix records with known good research data
    fix_record_fields(1)  # Treasures
    print()
    fix_record_fields(3)  # Mindfulness