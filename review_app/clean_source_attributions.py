#!/usr/bin/env python3
"""
Clean source attributions from field values and move them to separate annotation fields
"""

import sqlite3
import re

def extract_source_attribution(text):
    """Extract source attribution from field text and return cleaned text + sources"""
    if not text or not isinstance(text, str):
        return text, []
    
    # Pattern to match source attributions like "(Source: Amazon.com, Library of Congress)"
    source_pattern = r'\(Source:\s*([^)]+)\)'
    
    # Find all source attributions
    sources = []
    cleaned_text = text
    
    # Extract and remove source attributions
    matches = list(re.finditer(source_pattern, text))
    for match in reversed(matches):  # Process from end to avoid index issues
        source_info = match.group(1).strip()
        sources.extend([s.strip() for s in source_info.split(',')])
        cleaned_text = cleaned_text[:match.start()] + cleaned_text[match.end():]
    
    # Clean up any extra spaces or punctuation
    cleaned_text = re.sub(r'\s+', ' ', cleaned_text).strip()
    cleaned_text = re.sub(r'\s*,\s*$', '', cleaned_text)  # Remove trailing comma
    cleaned_text = re.sub(r'\s*\.\s*$', '', cleaned_text)  # Remove trailing period
    
    return cleaned_text, list(set(sources))  # Return unique sources

def clean_all_records():
    """Clean source attributions from all records"""
    
    conn = sqlite3.connect('data/reviews.db')
    cursor = conn.cursor()
    
    # Get all records
    cursor.execute("SELECT id, record_number, title, author, description, publisher, subjects, genre FROM records")
    records = cursor.fetchall()
    
    updated_count = 0
    
    for record in records:
        record_id, record_number, title, author, description, publisher, subjects, genre = record
        
        # Clean each field and collect sources
        all_sources = []
        
        # Clean title
        clean_title, title_sources = extract_source_attribution(title)
        all_sources.extend(title_sources)
        
        # Clean author
        clean_author, author_sources = extract_source_attribution(author)
        all_sources.extend(author_sources)
        
        # Clean publisher
        clean_publisher, publisher_sources = extract_source_attribution(publisher or '')
        all_sources.extend(publisher_sources)
        
        # Clean subjects
        clean_subjects, subjects_sources = extract_source_attribution(subjects or '')
        all_sources.extend(subjects_sources)
        
        # Clean genre
        clean_genre, genre_sources = extract_source_attribution(genre or '')
        all_sources.extend(genre_sources)
        
        # For description, we keep sources inline as requested
        clean_description = description  # Leave description as-is
        
        # Update record with cleaned fields
        if (clean_title != title or clean_author != author or 
            clean_publisher != publisher or clean_subjects != subjects or 
            clean_genre != genre):
            
            cursor.execute('''
                UPDATE records SET 
                    title = ?, author = ?, publisher = ?, subjects = ?, genre = ?
                WHERE id = ?
            ''', (clean_title, clean_author, clean_publisher, clean_subjects, clean_genre, record_id))
            
            # Store sources in a separate field for display
            unique_sources = list(set(all_sources))
            if unique_sources:
                cursor.execute('''
                    UPDATE records SET data_sources_used = ? WHERE id = ?
                ''', (','.join(unique_sources), record_id))
            
            updated_count += 1
            
            if record_number <= 20:  # Show first 20 records for verification
                print(f"Record #{record_number}: Cleaned fields, sources: {unique_sources}")
    
    conn.commit()
    conn.close()
    
    print(f"✅ Cleaned source attributions from {updated_count} records")

if __name__ == "__main__":
    clean_all_records()