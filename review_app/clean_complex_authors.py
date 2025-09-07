#!/usr/bin/env python3
"""
Clean complex author fields in records 151-808
"""

import sqlite3
import re

def clean_author_name(author):
    """Clean and format author names"""
    if not author:
        return None
    
    original = author
    
    # Remove source information, reliability notes, and other metadata
    author = re.sub(r'(?i)\s*\([^)]*(worldcat|library of congress|source|reliability|verified|catalog)[^)]*\)', '', author)
    author = re.sub(r'(?i)\s*\[[^\]]*(worldcat|library of congress|source|reliability|verified|catalog)[^\]]*\]', '', author)
    author = re.sub(r'(?i)\s*with source.*', '', author)
    author = re.sub(r'(?i)\s*verified by.*', '', author)
    
    # Remove years and dates
    author = re.sub(r',\s*\d{4}-\d{4}', '', author)
    author = re.sub(r',\s*\d{4}-present', '', author)
    
    # Remove honorifics and titles
    author = re.sub(r'(?i)^(dr\.?|prof\.?|mr\.?|mrs\.?|ms\.?|rev\.?)\s+', '', author)
    author = re.sub(r'(?i)\s+(dr\.?|prof\.?|mr\.?|mrs\.?|ms\.?|rev\.?)\s+', ' ', author)
    
    # Clean up compiled by, edited by, etc.
    author = re.sub(r'(?i),\s*compiled by', '', author)
    author = re.sub(r'(?i),\s*edited by', '', author)
    author = re.sub(r'(?i),\s*translated by', '', author)
    
    # Remove brackets and parentheses content
    author = re.sub(r'\s*\[[^\]]*\]', '', author)
    author = re.sub(r'\s*\([^)]*\)', '', author)
    
    # Clean up extra spaces and commas
    author = re.sub(r'\s+', ' ', author)
    author = re.sub(r',\s*,', ',', author)
    author = author.strip().strip(',').strip()
    
    # Handle organization names with articles
    if re.search(r'(?i)^the\s+[a-z]', author):
        org_name = re.sub(r'(?i)^the\s+', '', author)
        author = f"{org_name}, The"
    elif re.search(r'(?i)^a\s+[a-z]', author):
        org_name = re.sub(r'(?i)^a\s+', '', author)
        author = f"{org_name}, A"
    elif re.search(r'(?i)^an\s+[a-z]', author):
        org_name = re.sub(r'(?i)^an\s+', '', author)
        author = f"{org_name}, An"
    
    # Format multi-author lists
    if ' and ' in author.lower() or ',' in author:
        authors = []
        # Split by ' and ' or commas
        if ' and ' in author.lower():
            parts = re.split(r'\s+and\s+', author, flags=re.IGNORECASE)
        else:
            parts = [p.strip() for p in author.split(',')]
        
        for part in parts:
            if part and not re.search(r'(?i)^and$', part.strip()):
                # Format individual author names
                cleaned_part = format_individual_author(part.strip())
                if cleaned_part:
                    authors.append(cleaned_part)
        
        if len(authors) > 1:
            author = ' and '.join(authors)
        elif authors:
            author = authors[0]
    else:
        # Format single author
        author = format_individual_author(author)
    
    return author if author != original and author else original

def format_individual_author(name):
    """Format individual author name to Last, First Middle format"""
    if not name:
        return None
    
    # If already in Last, First format, clean it up
    if ',' in name:
        parts = [p.strip() for p in name.split(',', 1)]
        if len(parts) == 2:
            last_name = parts[0]
            first_middle = parts[1]
            # Remove any remaining honorifics from first/middle
            first_middle = re.sub(r'(?i)^(dr\.?|prof\.?|mr\.?|mrs\.?|ms\.?|rev\.?)\s+', '', first_middle)
            return f"{last_name}, {first_middle}"
    
    # If in First Last format, convert to Last, First
    parts = name.split()
    if len(parts) >= 2:
        last_name = parts[-1]
        first_middle = ' '.join(parts[:-1])
        # Remove honorifics from first/middle
        first_middle = re.sub(r'(?i)^(dr\.?|prof\.?|mr\.?|mrs\.?|ms\.?|rev\.?)\s+', '', first_middle)
        return f"{last_name}, {first_middle}"
    
    return name

def extract_cutter(author):
    """Extract first 3 letters for call number cutter"""
    if not author:
        return "XXX"
    
    # For organization names with articles at end
    if author.endswith(', The') or author.endswith(', A') or author.endswith(', An'):
        base_name = author.split(',')[0].strip()
        if len(base_name) >= 3:
            return base_name[:3].upper()
    
    # For regular author names
    if ',' in author:
        # Last, First format - use last name
        last_name = author.split(',')[0].strip()
        if last_name:
            return last_name[:3].upper()
    else:
        # First Last format - use last word
        parts = author.split()
        if parts:
            return parts[-1][:3].upper()
    
    return "XXX"

def clean_complex_authors():
    """Clean complex author fields in records 151-808"""
    
    conn = sqlite3.connect('data/reviews.db')
    cursor = conn.cursor()
    
    # Get records with complex author fields
    cursor.execute("""
        SELECT id, record_number, author, call_number 
        FROM records 
        WHERE record_number BETWEEN 151 AND 808 
          AND author IS NOT NULL 
          AND (author LIKE '%,%' OR author LIKE '%;%' 
               OR author LIKE '%(%' OR author LIKE '%)%' 
               OR LENGTH(author) > 30)
        ORDER BY record_number
    """)
    
    records = cursor.fetchall()
    print(f"Found {len(records)} records with complex author fields to process")
    
    processed_count = 0
    
    for record_id, record_number, author, call_number in records:
        original_author = author
        cleaned_author = clean_author_name(author)
        
        if cleaned_author != original_author:
            print(f"\n📋 Record #{record_number}")
            print(f"   👤 Original: '{original_author}'")
            print(f"   👤 Cleaned:  '{cleaned_author}'")
            
            # Extract cutter for call number update
            cutter = extract_cutter(cleaned_author)
            print(f"   🔠 Cutter: {cutter}")
            
            # Update call number if it follows standard pattern
            new_call_number = None
            if call_number and ' ' in call_number:
                parts = call_number.split()
                if len(parts) >= 2:
                    # Replace the second part (cutter) with new cutter
                    parts[1] = cutter
                    new_call_number = ' '.join(parts)
                    print(f"   📍 Call Number: {call_number} → {new_call_number}")
            
            # Update the record
            if new_call_number:
                cursor.execute("""
                    UPDATE records 
                    SET author = ?, call_number = ?
                    WHERE id = ?
                """, (cleaned_author, new_call_number, record_id))
            else:
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
    
    print(f"\n✅ Processed {processed_count} records with complex author fields")
    return processed_count

if __name__ == "__main__":
    clean_complex_authors()