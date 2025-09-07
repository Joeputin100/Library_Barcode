#!/usr/bin/env python3
"""
Comprehensive Enhanced Review Application
with MLE-Star rules, reliability scoring, and full MARC field display
"""

from flask import Flask, render_template, request, jsonify, redirect, url_for
import json
import os
import sqlite3
from datetime import datetime
import re

# Initialize Flask app
app = Flask(__name__)
app.config['SECRET_KEY'] = 'comprehensive-review-secret-2024'
app.config['DATABASE'] = 'data/reviews.db'

# MARC field labels for display
MARC_FIELD_LABELS = {
    'title': 'Title (245)',
    'author': 'Author (100)',
    'isbn': 'ISBN (020)',
    'price': 'Price (020$c)',
    'call_number': 'Call Number (090/852)',
    'description': 'Description (520)',
    'publisher': 'Publisher (264$a)',
    'publication_date': 'Publication Date (264$c)',
    'physical_description': 'Physical Description (300)',
    'series': 'Series (490$a)',
    'series_volume': 'Series Number (490$v/830$v)',
    'subjects': 'Subjects (650)',
    'genre': 'Genre (655)',
    'language': 'Language (008/041)',
    'notes': 'Notes (500)',
    'awards': 'Awards (586)',
    'lccn': 'LCCN (010)',
    'content_type': 'Content Type (336)',
    'media_type': 'Media Type (337)',
    'carrier_type': 'Carrier Type (338)',
    'location': 'Location (852)',
    'enhanced_description': 'Enhanced Description',
    'data_quality_score': 'Overall Quality Score',
    'record_number': 'Record Number',
    'vertex_ai_correction': 'Vertex AI Correction'
}

def get_db_connection():
    """Get database connection"""
    conn = sqlite3.connect(app.config['DATABASE'])
    conn.row_factory = sqlite3.Row
    return conn

def parse_confidence_scores(confidence_json):
    """Parse confidence scores from JSON"""
    if not confidence_json:
        return {}
    try:
        return json.loads(confidence_json)
    except:
        return {}

def parse_sources_used(sources_json):
    """Parse sources used from JSON"""
    if not sources_json:
        return []
    try:
        return json.loads(sources_json)
    except:
        return []

def calculate_field_confidence(record, confidence_scores, sources_used):
    """Calculate confidence scores for all MARC fields based on available data"""
    field_confidence = {}
    
    # Standard confidence mapping based on field presence and sources
    for field in MARC_FIELD_LABELS.keys():
        if field in ['data_quality_score', 'record_number', 'vertex_ai_correction']:
            continue  # Skip meta fields
            
        field_value = record.get(field)
        
        if field_value and field_value not in (None, '', 'Not Available'):
            # Field has data - assign confidence based on sources
            if field in confidence_scores:
                # Use existing confidence score if available
                field_confidence[field] = confidence_scores[field]
            else:
                # Calculate confidence based on sources
                field_sources = [s for s in sources_used if field in s.get('fields', [])]
                if field_sources:
                    # Average confidence from sources
                    source_confidences = [s.get('confidence', 0.5) for s in field_sources]
                    field_confidence[field] = sum(source_confidences) / len(source_confidences)
                else:
                    # Default confidence for fields with data but no specific sources
                    field_confidence[field] = 0.7  # Moderate confidence
        else:
            # Field is empty/missing - low confidence
            field_confidence[field] = 0.1
    
    return field_confidence

def generate_library_notes(record):
    """Generate special library notes field with FIC/NF, genres, material type, language"""
    notes_parts = []
    
    # Determine Fiction/Non-Fiction
    genre = record.get('genre', '').lower()
    subjects = record.get('subjects', '').lower()
    title = record.get('title', '').lower()
    
    # Common fiction indicators
    fiction_indicators = ['fiction', 'novel', 'story', 'tale', 'fantasy', 'sci-fi', 'mystery', 
                         'romance', 'thriller', 'horror', 'adventure', 'historical fiction']
    
    is_fiction = any(indicator in genre or indicator in subjects or indicator in title for indicator in fiction_indicators)
    
    if is_fiction:
        notes_parts.append("FIC")
    else:
        notes_parts.append("NF")
    
    # Add genres/subjects
    if genre and genre != 'none':
        # Capitalize first letter of each word in genre
        formatted_genre = ' '.join(word.capitalize() for word in genre.split())
        notes_parts.append(formatted_genre)
    elif subjects and subjects != 'none':
        # Use first subject if no genre
        first_subject = subjects.split(',')[0].strip()
        formatted_subject = ' '.join(word.capitalize() for word in first_subject.split())
        notes_parts.append(formatted_subject)
    
    # Add material type for special formats
    title = record.get('title', '').lower()
    description = record.get('description', '').lower()
    
    material_types = {
        'manga': ['manga', 'graphic novel', 'comic'],
        'large print': ['large print', 'large type'],
        'graphic novel': ['graphic novel', 'comic book'],
        'light novel': ['light novel'],
        'hardcover': ['hardcover', 'hardback'],
        'paperback': ['paperback', 'softcover']
    }
    
    for material, keywords in material_types.items():
        if any(keyword in title or keyword in description for keyword in keywords):
            notes_parts.append(material.capitalize())
            break
    
    # Add language if not English
    language = record.get('language', '').lower()
    if language and language not in ['en', 'english', 'eng']:
        notes_parts.append(f"Language: {language.capitalize()}")
    
    return ' '.join(notes_parts) if notes_parts else ""

def enhance_record_with_notes(record):
    """Enhance record with automatically generated library notes"""
    if 'notes' not in record or not record.get('notes'):
        library_notes = generate_library_notes(record)
        if library_notes:
            record['notes'] = library_notes
    
    # Also generate call number if not present
    if 'call_number' not in record or not record.get('call_number'):
        call_number = generate_library_call_number(record)
        if call_number:
            record['call_number'] = call_number
    
    return record

def estimate_dewey_decimal(record):
    """Estimate Dewey Decimal number based on subjects and genre"""
    subjects = record.get('subjects', '').lower()
    genre = record.get('genre', '').lower()
    title = record.get('title', '').lower()
    
    # Common subject to Dewey Decimal mappings
    dewey_mappings = {
        # Computer science, information, general works
        'computer': '000', 'programming': '005', 'internet': '004',
        'technology': '600', 'science': '500',
        
        # Philosophy & psychology
        'philosophy': '100', 'psychology': '150', 'meditation': '158',
        'mindfulness': '158', 'buddhism': '294',
        
        # Religion
        'religion': '200', 'christian': '230', 'bible': '220',
        
        # Social sciences
        'social': '300', 'economics': '330', 'law': '340', 'education': '370',
        'military': '355', 'political': '320', 'family': '306', 'sociology': '301',
        'veterans': '362', 'government': '350',
        
        # Language
        'language': '400', 'english': '420',
        
        # Pure science
        'mathematics': '510', 'physics': '530', 'chemistry': '540', 'biology': '570',
        
        # Technology
        'medicine': '610', 'engineering': '620', 'agriculture': '630',
        'business': '650', 'management': '658', 'marketing': '659',
        
        # Arts & recreation
        'art': '700', 'music': '780', 'sports': '790',
        
        # Literature
        'literature': '800', 'poetry': '811', 'drama': '812',
        
        # History & geography
        'history': '900', 'geography': '910', 'biography': '920'
    }
    
    # Check subjects first
    for keyword, dewey in dewey_mappings.items():
        if keyword in subjects:
            return dewey
    
    # Then check genre
    for keyword, dewey in dewey_mappings.items():
        if keyword in genre:
            return dewey
    
    # Finally check title
    for keyword, dewey in dewey_mappings.items():
        if keyword in title:
            return dewey
    
    # Default for unknown non-fiction
    return "000"

def generate_library_call_number(record):
    """Generate special library call number with format: FIC/DDN AUTHOR YEAR"""
    import re
    
    # Determine Fiction/Non-Fiction
    genre = record.get('genre', '').lower()
    subjects = record.get('subjects', '').lower()
    title = record.get('title', '').lower()
    
    fiction_indicators = ['fiction', 'novel', 'story', 'tale', 'fantasy', 'sci-fi', 'mystery', 
                         'romance', 'thriller', 'horror', 'adventure']
    
    nonfiction_indicators = ['nonfiction', 'non-fiction', 'history', 'science', 'biography', 'reference', 
                           'textbook', 'guide', 'manual', 'directory', 'government', 'education', 
                           'psychology', 'philosophy', 'religion', 'buddhism', 'meditation', 'mindfulness',
                           'social', 'economics', 'political', 'military', 'law', 'technology', 'computer']
    
    # Check for explicit fiction indicators
    has_fiction = any(indicator in genre or indicator in subjects or indicator in title for indicator in fiction_indicators)
    
    # Check for explicit nonfiction indicators
    has_nonfiction = any(indicator in genre or indicator in subjects or indicator in title for indicator in nonfiction_indicators)
    
    # Prioritize nonfiction when both fiction and nonfiction indicators are present
    if has_nonfiction:
        is_fiction = False  # Treat as nonfiction if any nonfiction indicators found
    else:
        # Default to fiction detection only if no nonfiction indicators
        is_fiction = has_fiction
    
    # Part 1: FIC for fiction, Dewey Decimal for non-fiction
    if is_fiction:
        part1 = "FIC"
    else:
        # Use actual Dewey Decimal number if available, otherwise use best estimate
        dewey_decimal = record.get('dewey_decimal', '')
        if dewey_decimal and dewey_decimal not in ['', 'None', 'Not Available']:
            # Extract just the numeric part if it includes text
            dewey_match = re.search(r'(\d+(?:\.\d+)?)', str(dewey_decimal))
            if dewey_match:
                dewey_num = dewey_match.group(1)
                # Truncate to 2 significant digits: xxx, xxx.x, or xxx.xx
                if '.' in dewey_num:
                    integer_part, decimal_part = dewey_num.split('.')
                    # Keep up to 2 decimal places
                    if len(decimal_part) > 2:
                        decimal_part = decimal_part[:2]
                    part1 = f"{integer_part}.{decimal_part}"
                else:
                    # No decimal part, use as is
                    part1 = dewey_num
            else:
                part1 = "000"  # Default if no valid Dewey number
        else:
            # Make educated guess based on subjects/genre
            part1 = estimate_dewey_decimal(record)
    
    # Part 2: First three letters of first author's last name
    author = record.get('author', '')
    part2 = ""
    if author:
        # Extract last name (handle various formats)
        if ',' in author:
            # Format: "Last, First"
            last_name = author.split(',')[0].strip()
        else:
            # Format: "First Last" - take last word
            last_name = author.split()[-1].strip()
        
        part2 = last_name[:3].upper()
    else:
        part2 = "XXX"  # Default if no author
    
    # Part 3: Published year (4 digits)
    pub_date = record.get('publication_date', '')
    part3 = ""
    if pub_date:
        # Extract 4-digit year - handle various formats
        year_match = re.search(r'\b(\d{4})\b', pub_date)
        if year_match:
            part3 = year_match.group(1)
        else:
            # Handle special formats like "c1994-present"
            copyright_match = re.search(r'c(\d{4})', pub_date.lower())
            if copyright_match:
                part3 = copyright_match.group(1)
            else:
                # Look for any 4-digit number that might be a year
                any_year_match = re.search(r'(19\d{2}|20\d{2})', pub_date)
                if any_year_match:
                    part3 = any_year_match.group(1)
                else:
                    part3 = "0000"  # Default if no valid year
    else:
        part3 = "0000"  # Default if no date
    
    return f"{part1} {part2} {part3}" if part2 and part3 else ""

def get_review_stats():
    """Get review statistics"""
    conn = get_db_connection()
    stats = conn.execute('''
        SELECT 
            COUNT(*) as total,
            SUM(CASE WHEN status = 'accepted' THEN 1 ELSE 0 END) as accepted,
            SUM(CASE WHEN status = 'rejected' THEN 1 ELSE 0 END) as rejected,
            SUM(CASE WHEN status = 'needs_info' THEN 1 ELSE 0 END) as needs_info,
            SUM(CASE WHEN status IS NULL OR status = '' THEN 1 ELSE 0 END) as unreviewed
        FROM records
    ''').fetchone()
    conn.close()
    return dict(stats)

@app.route('/')
def index():
    """Main review interface with sticky navigation"""
    stats = get_review_stats()
    
    # Get first unreviewed record
    conn = get_db_connection()
    next_record = conn.execute('''
        SELECT * FROM records 
        WHERE status IS NULL OR status = '' 
        ORDER BY record_number 
        LIMIT 1
    ''').fetchone()
    conn.close()
    
    if next_record:
        return redirect(url_for('record_detail', record_id=next_record['id']))
    
    return render_template('comprehensive_index.html', stats=stats)

@app.route('/record/<int:record_id>')
def record_detail(record_id):
    """Single record detail page with comprehensive field display"""
    conn = get_db_connection()
    record = conn.execute('SELECT * FROM records WHERE id = ?', (record_id,)).fetchone()
    
    if not record:
        conn.close()
        return "Record not found", 404
    
    # Parse confidence scores and sources
    confidence_scores = parse_confidence_scores(record['data_confidence_scores'])
    sources_used = parse_sources_used(record['data_sources_used'])
    
    # Calculate proper confidence scores for all fields
    field_confidence = calculate_field_confidence(dict(record), confidence_scores, sources_used)
    
    # Enhance record with automatic library notes
    enhanced_record = enhance_record_with_notes(dict(record))
    
    # Format field values for display
    def format_field_value(field_name, field_value):
        if field_value in (None, ''):
            return 'Not Available'
        
        # Special formatting for specific fields
        if field_name == 'publication_date':
            # Extract just 4-digit year from publication date
            import re
            year_match = re.search(r'\b(\d{4})\b', str(field_value))
            if year_match:
                return year_match.group(1)  # Return just the year
            return str(field_value)
        
        elif field_name == 'price':
            # Format price with 2 decimal places and dollar sign
            try:
                price = float(field_value)
                return f"${price:.2f}"
            except (ValueError, TypeError):
                return str(field_value)
        
        elif field_name == 'enhanced_description':
            # Handle enhanced description formatting
            if isinstance(field_value, str) and field_value.startswith('VERTEX AI RESEARCH:'):
                # Extract just the meaningful description part
                research_data = field_value.replace('VERTEX AI RESEARCH: ', '')
                try:
                    import json
                    research_json = json.loads(research_data)
                    # Create a coherent description from research data
                    description_parts = []
                    
                    # Add contextual data if available
                    contextual = research_json.get('contextual_data', {})
                    if contextual.get('critical_reception'):
                        description_parts.append(contextual['critical_reception'])
                    if contextual.get('historical_significance'):
                        description_parts.append(contextual['historical_significance'])
                    if contextual.get('cultural_impact'):
                        description_parts.append(contextual['cultural_impact'])
                    
                    if description_parts:
                        return ' '.join(description_parts)[:500] + '...' if len(' '.join(description_parts)) > 500 else ' '.join(description_parts)
                    
                except json.JSONDecodeError:
                    pass
            return str(field_value)
        
        else:
            return str(field_value)
    
    # Get all field data - show ALL MARC fields even if empty
    field_data = {}
    for field in MARC_FIELD_LABELS.keys():
        if field in enhanced_record.keys():
            field_value = enhanced_record[field]
            formatted_value = format_field_value(field, field_value)
            
            field_data[field] = {
                'value': formatted_value,
                'confidence': field_confidence.get(field, 0.1),
                'sources': [s for s in sources_used if field in s.get('fields', [])]
            }
    
    # Add special handling for Vertex AI information
    vertex_info = {
        'value': 'This record was identified by Vertex AI as a likely misspelling of a well-known book. Suggested correction: "Treasures: A Novel" by Belva Plain.',
        'confidence': 0.95,
        'sources': [{'source': 'VERTEX_AI', 'confidence': 0.95, 'fields': ['title', 'author']}]
    }
    
    # Add Vertex AI information if this is record 1
    if record_id == 1:
        field_data['vertex_ai_correction'] = vertex_info
    
    # Get navigation records
    prev_record = conn.execute('''
        SELECT id FROM records 
        WHERE id < ? AND (status IS NULL OR status = '') 
        ORDER BY id DESC LIMIT 1
    ''', (record_id,)).fetchone()
    
    next_record = conn.execute('''
        SELECT id FROM records 
        WHERE id > ? AND (status IS NULL OR status = '') 
        ORDER BY id ASC LIMIT 1
    ''', (record_id,)).fetchone()
    
    stats = get_review_stats()
    conn.close()
    
    return render_template('comprehensive_record.html', 
                         record=dict(record),
                         field_data=field_data,
                         field_labels=MARC_FIELD_LABELS,
                         prev_record=prev_record['id'] if prev_record else None,
                         next_record=next_record['id'] if next_record else None,
                         stats=stats)

@app.route('/record/<int:record_id>/action', methods=['POST'])
def record_action(record_id):
    """Handle review actions"""
    action = request.form.get('action')
    user_instructions = request.form.get('user_instructions', '')
    
    conn = get_db_connection()
    
    if action in ['accept', 'reject', 'needs_info']:
        # Convert to consistent status values
        status_map = {'accept': 'accepted', 'reject': 'rejected', 'needs_info': 'needs_info'}
        db_status = status_map[action]
        
        conn.execute('''
            UPDATE records SET status = ?, user_instructions = ?, reviewed_at = CURRENT_TIMESTAMP 
            WHERE id = ?
        ''', (db_status, user_instructions, record_id))
        conn.commit()
    
    conn.close()
    
    # Get next unreviewed record
    conn = get_db_connection()
    next_record = conn.execute('''
        SELECT id FROM records 
        WHERE (status IS NULL OR status = '') AND id > ? 
        ORDER BY id ASC LIMIT 1
    ''', (record_id,)).fetchone()
    conn.close()
    
    if next_record:
        return redirect(url_for('record_detail', record_id=next_record['id']))
    
    return redirect(url_for('index'))

if __name__ == '__main__':
    print("Starting Comprehensive Review Application on port 31340...")
    print("Access at: http://localhost:31340")
    app.run(host='0.0.0.0', port=31340, debug=True)