#!/usr/bin/env python3
"""
Clean attribution comments from Title fields
- Remove (WorldCat) and other source attributions
- Remove [Source: ...] in angle brackets
"""

import sqlite3
import re

def clean_title(title):
    """Remove attribution comments from title"""
    if not title:
        return title
    
    # Remove (WorldCat) and similar parenthetical attributions
    cleaned = re.sub(r'\([^)]*(WorldCat|Library of Congress|Amazon|Goodreads|Source)[^)]*\)', '', title)
    
    # Remove [Source: ...] in angle brackets
    cleaned = re.sub(r'\[[^]]*Source[^]]*\]', '', cleaned)
    
    # Clean up extra spaces and punctuation
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()
    cleaned = re.sub(r'\s*,\s*$', '', cleaned)
    cleaned = re.sub(r'\s*\.\s*$', '', cleaned)
    
    return cleaned

def clean_all_title_attributions():
    """Clean attribution comments from all title fields"""
    
    conn = sqlite3.connect('data/reviews.db')
    cursor = conn.cursor()
    
    # Get all records with titles
    cursor.execute("SELECT id, record_number, title FROM records WHERE title IS NOT NULL AND title != ''")
    records = cursor.fetchall()
    
    updated_count = 0
    
    for record_id, record_number, current_title in records:
        new_title = clean_title(current_title)
        
        if new_title != current_title:
            cursor.execute("UPDATE records SET title = ? WHERE id = ?", (new_title, record_id))
            updated_count += 1
            
            if record_number <= 30:  # Show first 30 changes for verification
                print(f"Record #{record_number}: '{current_title}' -> '{new_title}'")
    
    conn.commit()
    conn.close()
    
    print(f"✅ Cleaned title attributions for {updated_count} records")
    return updated_count

if __name__ == "__main__":
    clean_all_title_attributions()