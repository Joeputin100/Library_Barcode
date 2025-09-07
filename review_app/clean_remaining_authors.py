#!/usr/bin/env python3
"""
Clean remaining author fields with source information
"""

import sqlite3
import re

def clean_author_final(author):
    """Final cleanup of author names"""
    if not author:
        return None
    
    # Remove all source information patterns
    patterns = [
        r'(?i)\s*\([^)]*(worldcat|library of congress|source|reliability|verified|catalog|amazon|goodreads|publisher)[^)]*\)',
        r'(?i)\s*\[[^\]]*(worldcat|library of congress|source|reliability|verified|catalog|amazon|goodreads|publisher)[^\]]*\]',
        r'(?i)\s*-\s*[Ss]ource:[^\]]*',
        r'(?i)\s*verified by.*',
        r'(?i)\s*from.*',
        r'(?i)\s*with.*',
        r'(?i)\s*and.*[Ss]ource.*',
    ]
    
    for pattern in patterns:
        author = re.sub(pattern, '', author)
    
    # Clean up extra spaces and commas
    author = re.sub(r'\s+', ' ', author)
    author = re.sub(r',\s*,', ',', author)
    author = author.strip().strip(',').strip()
    
    # Format basic author names
    if ',' in author and ' and ' not in author:
        # Already in Last, First format
        parts = author.split(',', 1)
        last_name = parts[0].strip()
        first_name = parts[1].strip()
        return f"{last_name}, {first_name}"
    elif ' and ' in author:
        # Multiple authors
        authors = []
        parts = re.split(r'\s+and\s+', author)
        for part in parts:
            if ',' in part:
                authors.append(part.strip())
            else:
                # Convert First Last to Last, First
                name_parts = part.split()
                if len(name_parts) >= 2:
                    last_name = name_parts[-1]
                    first_name = ' '.join(name_parts[:-1])
                    authors.append(f"{last_name}, {first_name}")
                else:
                    authors.append(part.strip())
        return ' and '.join(authors)
    else:
        # Single author in First Last format
        parts = author.split()
        if len(parts) >= 2:
            last_name = parts[-1]
            first_name = ' '.join(parts[:-1])
            return f"{last_name}, {first_name}"
        else:
            return author

def clean_remaining_authors():
    """Clean remaining author fields with source information"""
    
    conn = sqlite3.connect('data/reviews.db')
    cursor = conn.cursor()
    
    # Get records with remaining source information
    cursor.execute("""
        SELECT id, record_number, author 
        FROM records 
        WHERE record_number BETWEEN 151 AND 808 
          AND author IS NOT NULL 
          AND (author LIKE '%WorldCat%' 
               OR author LIKE '%Library of Congress%' 
               OR author LIKE '%Source:%' 
               OR author LIKE '%reliability:%'
               OR author LIKE '%Amazon%'
               OR author LIKE '%Goodreads%')
        ORDER BY record_number
    """)
    
    records = cursor.fetchall()
    print(f"Found {len(records)} records with remaining source information in authors")
    
    processed_count = 0
    
    for record_id, record_number, author in records:
        original_author = author
        cleaned_author = clean_author_final(author)
        
        if cleaned_author != original_author:
            print(f"\n📋 Record #{record_number}")
            print(f"   👤 Original: '{original_author}'")
            print(f"   👤 Cleaned:  '{cleaned_author}'")
            
            # Update the record
            cursor.execute("""
                UPDATE records 
                SET author = ?
                WHERE id = ?
            """, (cleaned_author, record_id))
            
            if cursor.rowcount > 0:
                print(f"   ✅ Updated successfully")
                processed_count += 1
            else:
                print(f"   ❌ Update failed")
        else:
            print(f"📋 Record #{record_number}: No changes needed - '{author}'")
    
    conn.commit()
    conn.close()
    
    print(f"\n✅ Processed {processed_count} records with remaining source information")
    return processed_count

if __name__ == "__main__":
    clean_remaining_authors()