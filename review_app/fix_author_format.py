#!/usr/bin/env python3
"""
Fix author names from 'First Last' format to 'Last, First' format
"""

import sqlite3
import re

def format_author_name(author):
    """Convert 'First Last' format to 'Last, First' format"""
    if not author or ',' in author or '(' in author or ')' in author:
        return author  # Already formatted or contains special characters
    
    # Handle corporate authors, organizations, etc. - DON'T change these
    corporate_keywords = [
        'department', 'corporation', 'inc', 'llc', 'association', 
        'foundation', 'committee', 'center', 'institute', 'press',
        'entertainment', 'calligraphy', 'university', 'college', 'school',
        'agency', 'bureau', 'division', 'group', 'team', 'studio'
    ]
    
    if any(keyword in author.lower() for keyword in corporate_keywords):
        return author
    
    # Handle multi-author cases with "and" - DON'T change these
    if ' and ' in author.lower():
        return author
    
    # Handle initials like J.D. Davies, R.L. Roldan
    if re.match(r'^[A-Z]\.\s*[A-Z]\.', author):
        parts = author.split()
        if len(parts) >= 2:
            # Format like "Davies, J.D."
            return f"{parts[-1]}, {' '.join(parts[:-1])}"
    
    # Handle regular First Last names
    parts = author.split()
    if len(parts) == 2:
        # Simple case: First Last -> Last, First
        # But only if both parts look like names (not numbers, not corporate)
        if (parts[0][0].isalpha() and parts[1][0].isalpha() and
            not any(keyword in parts[0].lower() for keyword in corporate_keywords) and
            not any(keyword in parts[1].lower() for keyword in corporate_keywords)):
            return f"{parts[1]}, {parts[0]}"
    elif len(parts) > 2:
        # Multiple names: take last word as last name only if it looks like a surname
        # Avoid changing things like "Hustle 2.0" or corporate names
        last_word = parts[-1]
        if (last_word[0].isalpha() and 
            not last_word.replace('.', '').isdigit() and 
            not any(keyword in last_word.lower() for keyword in corporate_keywords) and
            not any(keyword in author.lower() for keyword in corporate_keywords)):
            return f"{last_word}, {' '.join(parts[:-1])}"
    
    return author  # Single word or couldn't parse

def fix_all_author_formats():
    """Fix author name formats across all records"""
    
    conn = sqlite3.connect('data/reviews.db')
    cursor = conn.cursor()
    
    # Get all records with authors
    cursor.execute("SELECT id, record_number, author FROM records WHERE author IS NOT NULL AND author != ''")
    records = cursor.fetchall()
    
    updated_count = 0
    
    for record_id, record_number, current_author in records:
        new_author = format_author_name(current_author)
        
        if new_author != current_author:
            cursor.execute("UPDATE records SET author = ? WHERE id = ?", (new_author, record_id))
            updated_count += 1
            
            if record_number <= 30:  # Show first 30 changes for verification
                print(f"Record #{record_number}: '{current_author}' -> '{new_author}'")
    
    conn.commit()
    conn.close()
    
    print(f"✅ Fixed author formats for {updated_count} records")
    return updated_count

if __name__ == "__main__":
    fix_all_author_formats()