#!/usr/bin/env python3
"""
Mass apply Vertex AI research results to all database fields
"""

import sqlite3
import json
from vertex_grounded_research import apply_research_to_record

def mass_apply_research():
    """Apply research results to all processed records"""
    
    conn = sqlite3.connect('review_app/data/reviews.db')
    cursor = conn.cursor()
    
    # Get all records with Vertex AI research
    cursor.execute('''
        SELECT id, record_number, enhanced_description FROM records 
        WHERE enhanced_description LIKE '%VERTEX AI RESEARCH%'
    ''')
    
    records = cursor.fetchall()
    total = len(records)
    
    print(f"🔧 Mass applying research to {total} records")
    print("=" * 50)
    
    updated_count = 0
    error_count = 0
    
    for i, (id, record_number, enhanced_description) in enumerate(records):
        if i % 50 == 0:
            print(f"Processing {i}/{total} records...")
        
        try:
            # Extract research results
            if 'VERTEX AI RESEARCH: ' in enhanced_description:
                research_json = enhanced_description.replace('VERTEX AI RESEARCH: ', '')
                research_results = json.loads(research_json)
                
                # Apply research to database fields
                updates_applied = apply_research_to_record(id, research_results, conn)
                
                if updates_applied > 0:
                    updated_count += 1
                
        except json.JSONDecodeError as e:
            print(f"❌ JSON error for record #{record_number}: {e}")
            error_count += 1
        except Exception as e:
            print(f"❌ Error for record #{record_number}: {e}")
            error_count += 1
    
    conn.commit()
    conn.close()
    
    print(f"\n🎉 Mass apply completed!")
    print(f"   Records updated: {updated_count}/{total}")
    print(f"   Errors: {error_count}")
    print(f"   Success rate: {(updated_count/total)*100:.1f}%")

if __name__ == "__main__":
    mass_apply_research()