#!/usr/bin/env python3
"""
Fix Notes (500) field format to: FIC/NF Genre(2 words max) Language(if not English) Format(if special)
"""

import sqlite3
import re

def parse_existing_notes(notes):
    """Parse existing notes to extract useful information"""
    if not notes:
        return None, None, None, None
    
    notes_lower = notes.lower()
    
    # Extract FIC/NF
    fic_nf = None
    if 'fic' in notes_lower:
        fic_nf = 'FIC'
    elif 'nf' in notes_lower:
        fic_nf = 'NF'
    
    # Extract language if not English
    language = None
    non_english_languages = ['spanish', 'french', 'german', 'chinese', 'japanese', 'korean', 
                            'russian', 'arabic', 'portuguese', 'italian']
    for lang in non_english_languages:
        if lang in notes_lower:
            language = lang.capitalize()
            break
    
    # Extract format
    format_type = None
    special_formats = ['manga', 'comic', 'graphic novel', 'light novel', 'large print', 'hardcover', 'paperback']
    for fmt in special_formats:
        if fmt in notes_lower:
            format_type = fmt.capitalize()
            break
    
    # Extract genre (try to find meaningful words)
    genre = None
    common_genres = ['mystery', 'fantasy', 'science fiction', 'sci-fi', 'romance', 'thriller', 
                    'horror', 'historical', 'biography', 'autobiography', 'christian', 'religious',
                    'business', 'self-help', 'cooking', 'travel', 'art', 'music', 'drama', 'poetry']
    
    for g in common_genres:
        if g in notes_lower:
            genre = g.capitalize()
            # Handle multi-word genres
            if g == 'science fiction':
                genre = 'Sci-Fi'
            elif g == 'self-help':
                genre = 'Self Help'
            break
    
    return fic_nf, genre, language, format_type

def generate_proper_notes(record):
    """Generate proper notes format based on record data"""
    title = record.get('title', '').lower()
    author = record.get('author', '').lower()
    subjects = record.get('subjects', '').lower()
    genre = record.get('genre', '').lower()
    language = record.get('language', '').lower()
    current_notes = record.get('notes', '')
    
    # Parse existing notes first
    fic_nf, existing_genre, existing_language, existing_format = parse_existing_notes(current_notes)
    
    # Determine FIC/NF if not found in notes
    if not fic_nf:
        # Common fiction indicators
        fiction_indicators = ['fiction', 'novel', 'story', 'tale', 'fantasy', 'sci-fi', 'mystery', 
                             'romance', 'thriller', 'horror', 'adventure']
        # Common nonfiction indicators  
        nonfiction_indicators = ['nonfiction', 'non-fiction', 'history', 'science', 'biography', 'reference',
                               'guide', 'manual', 'textbook', 'education', 'psychology', 'philosophy']
        
        has_fiction = any(indicator in title or indicator in subjects or indicator in genre 
                         for indicator in fiction_indicators)
        has_nonfiction = any(indicator in title or indicator in subjects or indicator in genre 
                           for indicator in nonfiction_indicators)
        
        # Prioritize nonfiction when both indicators present
        if has_nonfiction:
            fic_nf = 'NF'
        elif has_fiction:
            fic_nf = 'FIC'
        else:
            fic_nf = 'NF'  # Default to nonfiction
    
    # Determine genre if not found
    if not existing_genre:
        # Try to extract from available fields
        genre_sources = [genre, subjects, title]
        common_genres = ['mystery', 'fantasy', 'science fiction', 'sci-fi', 'romance', 'thriller', 
                        'horror', 'historical', 'biography', 'christian', 'religious', 'business']
        
        for source in genre_sources:
            for g in common_genres:
                if g in source:
                    existing_genre = g.capitalize()
                    if g == 'science fiction':
                        existing_genre = 'Sci-Fi'
                    break
            if existing_genre:
                break
    
    # Determine language if not English
    if not existing_language and language and language not in ['en', 'english', 'eng']:
        existing_language = language.capitalize()
    
    # Determine format
    if not existing_format:
        # Check for special formats in title/description
        title_desc = title + ' ' + (record.get('description', '') or '').lower()
        special_formats = ['manga', 'comic', 'graphic novel', 'light novel', 'large print']
        
        for fmt in special_formats:
            if fmt in title_desc:
                existing_format = fmt.capitalize()
                if fmt == 'graphic novel':
                    existing_format = 'Graphic Novel'
                elif fmt == 'light novel':
                    existing_format = 'Light Novel'
                break
    
    # Build the proper notes format
    parts = []
    if fic_nf:
        parts.append(fic_nf)
    if existing_genre:
        # Limit to 2 words maximum
        genre_words = existing_genre.split()
        if len(genre_words) > 2:
            existing_genre = ' '.join(genre_words[:2])
        parts.append(existing_genre)
    if existing_language:
        parts.append(existing_language)
    if existing_format:
        parts.append(existing_format)
    
    return ' '.join(parts) if parts else ''

def fix_all_notes_formats():
    """Fix notes format across all records"""
    
    conn = sqlite3.connect('data/reviews.db')
    cursor = conn.cursor()
    
    # Get all records
    cursor.execute("SELECT id, record_number, title, author, subjects, genre, language, description, notes FROM records")
    records = cursor.fetchall()
    
    updated_count = 0
    
    for record in records:
        record_id, record_number, title, author, subjects, genre, language, description, current_notes = record
        
        record_dict = {
            'title': title or '',
            'author': author or '',
            'subjects': subjects or '',
            'genre': genre or '',
            'language': language or '',
            'description': description or '',
            'notes': current_notes or ''
        }
        
        new_notes = generate_proper_notes(record_dict)
        
        # Only update if we generated meaningful notes and they're different from current
        if new_notes and new_notes != current_notes:
            cursor.execute("UPDATE records SET notes = ? WHERE id = ?", (new_notes, record_id))
            updated_count += 1
            
            if record_number <= 30:  # Show first 30 changes for verification
                print(f"Record #{record_number}: '{current_notes}' -> '{new_notes}'")
    
    conn.commit()
    conn.close()
    
    print(f"✅ Fixed notes format for {updated_count} records")
    return updated_count

if __name__ == "__main__":
    fix_all_notes_formats()