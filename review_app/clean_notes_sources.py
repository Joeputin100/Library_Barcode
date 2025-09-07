#!/usr/bin/env python3
"""
Clean source information from Notes fields
- Remove English language identifiers (only show non-English languages)
- Remove source information in parentheses or angle brackets
"""

import sqlite3
import re

def clean_notes(notes):
    """Clean source information from notes field"""
    if not notes:
        return notes
    
    # Remove English language identifiers
    cleaned = re.sub(r'\bEnglish\b', '', notes, flags=re.IGNORECASE)
    
    # Remove source information in parentheses
    cleaned = re.sub(r'\([^)]*(source|library of congress|worldcat|amazon|goodreads)[^)]*\)', '', cleaned, flags=re.IGNORECASE)
    
    # Remove source information in angle brackets
    cleaned = re.sub(r'\[[^]]*(source|library of congress|worldcat|amazon|goodreads)[^]]*\]', '', cleaned, flags=re.IGNORECASE)
    
    # Clean up extra spaces and punctuation
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()
    cleaned = re.sub(r'^\s*\|\s*', '', cleaned)
    cleaned = re.sub(r'\s*\|\s*$', '', cleaned)
    cleaned = re.sub(r'\s*,\s*$', '', cleaned)
    cleaned = re.sub(r'\s*\.\s*$', '', cleaned)
    
    return cleaned

def clean_all_notes_sources():
    """Clean source information from all notes fields"""
    
    conn = sqlite3.connect('data/reviews.db')
    cursor = conn.cursor()
    
    # Get all records with notes
    cursor.execute("SELECT id, record_number, notes FROM records WHERE notes IS NOT NULL AND notes != ''")
    records = cursor.fetchall()
    
    updated_count = 0
    
    for record_id, record_number, current_notes in records:
        new_notes = clean_notes(current_notes)
        
        if new_notes != current_notes:
            cursor.execute("UPDATE records SET notes = ? WHERE id = ?", (new_notes, record_id))
            updated_count += 1
            
            if record_number <= 30:  # Show first 30 changes for verification
                print(f"Record #{record_number}: '{current_notes}' -> '{new_notes}'")
    
    conn.commit()
    conn.close()
    
    print(f"✅ Cleaned notes sources for {updated_count} records")
    return updated_count

if __name__ == "__main__":
    clean_all_notes_sources()