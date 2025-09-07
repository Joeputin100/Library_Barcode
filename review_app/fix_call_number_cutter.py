#!/usr/bin/env python3
"""
Fix call number cutter to use first 3 letters of author's LAST name instead of first name
"""

import sqlite3
import re

def extract_last_name(author):
    """Extract last name from author field"""
    if not author:
        return None
    
    # Handle 'Last, First' format
    if ',' in author:
        return author.split(',')[0].strip()
    
    # Handle 'First Last' format - take last word
    parts = author.split()
    if parts:
        return parts[-1].strip()
    
    return None

def fix_call_number_cutter(call_number, author):
    """Fix call number to use proper author cutter (last name)"""
    if not call_number or not author:
        return call_number
    
    last_name = extract_last_name(author)
    if not last_name or len(last_name) < 2:
        return call_number
    
    # Extract current cutter (assumed to be 3 letters)
    parts = call_number.split()
    if len(parts) < 3:
        return call_number  # Not enough parts to fix
    
    # Replace the cutter (second part) with proper last name abbreviation
    proper_cutter = last_name[:3].upper()
    parts[1] = proper_cutter
    
    return ' '.join(parts)

def fix_all_call_number_cutters():
    """Fix call number cutters across all records"""
    
    conn = sqlite3.connect('data/reviews.db')
    cursor = conn.cursor()
    
    # Get all records with call numbers and authors
    cursor.execute("""
        SELECT id, record_number, call_number, author 
        FROM records 
        WHERE call_number IS NOT NULL AND call_number != ''
        AND author IS NOT NULL AND author != ''
    """)
    records = cursor.fetchall()
    
    updated_count = 0
    
    for record_id, record_number, current_call_number, author in records:
        new_call_number = fix_call_number_cutter(current_call_number, author)
        
        if new_call_number != current_call_number:
            cursor.execute("UPDATE records SET call_number = ? WHERE id = ?", (new_call_number, record_id))
            updated_count += 1
            
            if record_number <= 30:  # Show first 30 changes for verification
                print(f"Record #{record_number}: '{current_call_number}' -> '{new_call_number}' (Author: {author})")
    
    conn.commit()
    conn.close()
    
    print(f"✅ Fixed call number cutters for {updated_count} records")
    return updated_count

if __name__ == "__main__":
    fix_all_call_number_cutters()