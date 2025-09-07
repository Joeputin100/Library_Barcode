#!/usr/bin/env python3
"""
Process Needs Info instructions for records 1-150
"""

import sqlite3
import re

def process_needs_info():
    """Process all Needs Info instructions"""
    
    conn = sqlite3.connect('data/reviews.db')
    cursor = conn.cursor()
    
    # Get all records with Needs Info status in range 1-150
    cursor.execute("""
        SELECT id, record_number, user_instructions, title, author, call_number, 
               notes, genre, series, series_volume, language
        FROM records 
        WHERE status = 'needs_info' AND record_number BETWEEN 1 AND 150
        ORDER BY record_number
    """)
    
    records = cursor.fetchall()
    print(f"Found {len(records)} records with Needs Info status to process")
    
    processed_count = 0
    
    for record in records:
        record_id, record_number, instructions, title, author, call_number, \
        notes, genre, series, series_volume, language = record
        
        if not instructions:
            print(f"❌ Record #{record_number}: No instructions found")
            continue
            
        print(f"\n📋 Record #{record_number}: {instructions}")
        
        # Parse instructions
        updates = {}
        
        # Extract call number updates
        call_number_match = re.search(r'Update call number to "([^"]+)"', instructions, re.IGNORECASE)
        if call_number_match:
            updates['call_number'] = call_number_match.group(1)
            print(f"   📍 Call Number: {call_number} → {updates['call_number']}")
        
        # Extract author updates
        author_match = re.search(r'Update author to "([^"]+)"', instructions, re.IGNORECASE)
        if author_match:
            updates['author'] = author_match.group(1)
            print(f"   👤 Author: {author} → {updates['author']}")
        
        # Extract title updates
        title_match = re.search(r'Update title to "([^"]+)"', instructions, re.IGNORECASE)
        if title_match:
            updates['title'] = title_match.group(1)
            print(f"   📖 Title: {title} → {updates['title']}")
        
        # Extract series updates
        series_match = re.search(r'Update series title to "([^"]+)"', instructions, re.IGNORECASE)
        if series_match:
            updates['series'] = series_match.group(1)
            print(f"   📚 Series: {series} → {updates['series']}")
        
        # Extract series number updates
        series_num_match = re.search(r'Update series number to "([^"]+)"', instructions, re.IGNORECASE)
        if series_num_match:
            updates['series_volume'] = series_num_match.group(1)
            print(f"   🔢 Series Number: {series_volume} → {updates['series_volume']}")
        
        # Extract notes updates (MARC 500)
        notes_match = re.search(r'Update Notes(?:\(500\))? (?:field )?to "([^"]+)"', instructions, re.IGNORECASE)
        if notes_match:
            updates['notes'] = notes_match.group(1)
            print(f"   📝 Notes: {notes} → {updates['notes']}")
        
        # Build SQL update
        if updates:
            set_clause = ', '.join([f"{key} = ?" for key in updates.keys()])
            values = list(updates.values())
            values.extend([record_id])  # Add record_id for WHERE clause
            
            cursor.execute(f"""
                UPDATE records 
                SET {set_clause}, status = NULL, user_instructions = NULL
                WHERE id = ?
            """, values)
            
            if cursor.rowcount > 0:
                print(f"   ✅ Updated successfully")
                processed_count += 1
            else:
                print(f"   ❌ Update failed")
        else:
            print(f"   ⚠️  No valid updates found in instructions")
    
    conn.commit()
    conn.close()
    
    print(f"\n✅ Processed {processed_count} records successfully")
    return processed_count

if __name__ == "__main__":
    process_needs_info()