#!/usr/bin/env python3
"""
Update NF-only notes field with proper genre/language/format information
"""

import sqlite3
import re

def update_nf_notes():
    """Update records with only 'NF' in notes to include genre/language/format"""
    
    conn = sqlite3.connect('data/reviews.db')
    cursor = conn.cursor()
    
    # Get all records with only 'NF' in notes
    cursor.execute("SELECT record_number, title, genre, language FROM records WHERE notes = 'NF'")
    records = cursor.fetchall()
    
    updated_count = 0
    
    for record_number, title, genre, language in records:
        if not genre or genre in ('', 'None', 'Not Available'):
            print(f"❌ Record #{record_number}: No genre information available")
            continue
            
        # Extract first 1-2 words from genre for the notes
        genre_words = genre.split()
        if len(genre_words) > 2:
            # Take first 2 words if available
            genre_part = ' '.join(genre_words[:2])
        else:
            genre_part = genre
        
        # Clean up genre part (remove special characters, etc.)
        genre_part = re.sub(r'[^a-zA-Z\s]', '', genre_part).strip()
        
        # Determine if language is not English
        language_part = ""
        if language and language.lower() not in ['en', 'english', 'eng', '']:
            language_part = f"{language.capitalize()}"
        
        # Determine special formats
        title_lower = title.lower() if title else ""
        format_part = ""
        
        format_keywords = {
            'manga': ['manga', 'graphic novel', 'comic'],
            'large print': ['large print', 'large type'],
            'reference': ['reference', 'directory', 'handbook', 'manual'],
            'easy reading': ['easy reading', 'beginner', 'introductory', 'juvenile', 'children']
        }
        
        for format_name, keywords in format_keywords.items():
            if any(keyword in title_lower for keyword in keywords):
                format_part = format_name.capitalize()
                break
        
        # Build the new notes field
        new_notes_parts = ["NF", genre_part]
        
        if language_part:
            new_notes_parts.append(language_part)
        
        if format_part:
            new_notes_parts.append(format_part)
        
        new_notes = ' '.join(new_notes_parts)
        
        # Update the record
        cursor.execute(
            "UPDATE records SET notes = ? WHERE record_number = ?",
            (new_notes, record_number)
        )
        
        if cursor.rowcount > 0:
            print(f"✅ Record #{record_number}: 'NF' → '{new_notes}'")
            updated_count += 1
    
    conn.commit()
    conn.close()
    
    print(f"✅ Updated {updated_count} NF-only notes with proper genre/language/format information")
    return updated_count

if __name__ == "__main__":
    update_nf_notes()