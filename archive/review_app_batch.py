#!/usr/bin/env python3
"""
Enhanced Flask Review Application for Large Batch Processing
Port: 31338

Features:
- Single-record display with progress tracking
- Cover image safety filter (only verified books)
- Alternate edition fallback system
- MSRP-aware price calculation
- Progress sticky header for large batches
"""

from flask import Flask, render_template, request, jsonify, send_file
import json
import os
import sqlite3
from datetime import datetime
import requests
from pathlib import Path
import re

# Initialize Flask app
app = Flask(__name__)
app.config['SECRET_KEY'] = 'barcode-batch-secret-2024'
app.config['DATABASE'] = 'data/reviews_batch.db'
app.config['COVERS_DIR'] = 'static/covers_batch'
app.config['INPUT_FILE'] = '../isbns_to_be_entered_2025088.txt'

# Ensure directories exist
os.makedirs('data', exist_ok=True)
os.makedirs('static/covers_batch', exist_ok=True)

# Database initialization
def init_db():
    """Initialize the SQLite database for batch processing"""
    with sqlite3.connect(app.config['DATABASE']) as conn:
        cursor = conn.cursor()
        
        # Records table with enhanced fields
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS records (
                id INTEGER PRIMARY KEY,
                record_number INTEGER,
                original_line TEXT,
                title TEXT,
                author TEXT,
                isbn TEXT,
                status TEXT,
                price REAL,
                msrp REAL,
                call_number TEXT,
                local_call_number TEXT,
                description TEXT,
                notes TEXT,
                cover_url TEXT,
                verified BOOLEAN DEFAULT FALSE,
                has_cover BOOLEAN DEFAULT FALSE,
                processed BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Reviews table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS reviews (
                id INTEGER PRIMARY KEY,
                record_id INTEGER,
                decision TEXT,
                notes TEXT,
                reviewed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (record_id) REFERENCES records (id)
            )
        ''')
        
        # Progress tracking
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS progress (
                id INTEGER PRIMARY KEY,
                total_records INTEGER,
                processed_records INTEGER,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        conn.commit()

# Parse input file
def parse_input_file():
    """Parse the complete input file and return all records"""
    records = []
    record_number = 1
    
    try:
        with open(app.config['INPUT_FILE'], 'r') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('Books to be Entered'):
                    continue
                    
                record = {
                    'record_number': record_number,
                    'original_line': line,
                    'title': '',
                    'author': '',
                    'isbn': '',
                    'status': 'pending',
                    'verified': False,
                    'has_cover': False
                }
                
                # Parse different line formats
                if line.startswith('NO ISBN:'):
                    # Format: "NO ISBN: Title by Author"
                    parts = line.replace('NO ISBN:', '').strip().split(' by ')
                    if len(parts) >= 2:
                        record['title'] = parts[0].strip()
                        record['author'] = parts[1].strip()
                    else:
                        record['title'] = line.replace('NO ISBN:', '').strip()
                        record['author'] = 'Unknown Author'
                elif re.match(r'^\d{10,13}$', line.replace('-', '')):
                    # ISBN only line
                    record['isbn'] = line.strip()
                    record['title'] = 'Unknown Title'
                    record['author'] = 'Unknown Author'
                else:
                    # Try to extract ISBN from any line
                    isbn_match = re.search(r'\b(?:97(?:8|9))?\d{9}\d?\b', line)
                    if isbn_match:
                        record['isbn'] = isbn_match.group(0)
                    record['title'] = line.strip()
                    record['author'] = 'Unknown Author'
                
                records.append(record)
                record_number += 1
        
        return records
    except Exception as e:
        print(f"Error parsing input file: {e}")
        return []

# Enhanced Google Books API with safety checks
def safe_fetch_cover_image(isbn, title, author):
    """Fetch cover image only for verified books with safety checks"""
    if not isbn or not is_valid_isbn(isbn):
        return None
    
    try:
        # First verify the book exists in Google Books
        verify_url = f"https://www.googleapis.com/books/v1/volumes?q=isbn:{isbn}"
        response = requests.get(verify_url, timeout=10)
        data = response.json()
        
        if not data.get('items') or len(data['items']) == 0:
            # Book not found in Google Books - don't show cover
            return None
        
        # Check if the book details match
        volume_info = data['items'][0]['volumeInfo']
        found_title = volume_info.get('title', '').lower()
        found_authors = volume_info.get('authors', [])
        
        # Basic verification: title should be somewhat similar
        if title and found_title:
            title_similarity = similar(found_title, title.lower())
            if title_similarity < 0.3:  # Low similarity threshold
                return None
        
        # If we have a cover image, return it
        if 'imageLinks' in volume_info:
            return volume_info['imageLinks'].get('thumbnail')
    
    except Exception as e:
        print(f"Error in safe cover fetch: {e}")
    
    return None

def is_valid_isbn(isbn):
    """Validate ISBN format"""
    clean_isbn = isbn.replace('-', '')
    return len(clean_isbn) in [10, 13] and clean_isbn.isdigit()

def similar(a, b):
    """Calculate string similarity"""
    from difflib import SequenceMatcher
    return SequenceMatcher(None, a, b).ratio()

def download_cover(image_url, isbn):
    """Download and save cover image"""
    if not image_url:
        return None
    
    try:
        filename = f"{isbn}.jpg" if isbn else f"cover_{datetime.now().timestamp()}.jpg"
        filepath = os.path.join(app.config['COVERS_DIR'], filename)
        
        response = requests.get(image_url, timeout=10)
        with open(filepath, 'wb') as f:
            f.write(response.content)
        
        return filename
    except Exception as e:
        print(f"Error downloading cover: {e}")
        return None

# MSRP-aware price calculation
def calculate_msrp_aware_price(isbn, title, author):
    """Calculate price considering MSRP and market availability"""
    try:
        # Try to get current market data
        url = f"https://www.googleapis.com/books/v1/volumes?q=isbn:{isbn}"
        response = requests.get(url, timeout=10)
        data = response.json()
        
        if data.get('items') and len(data['items']) > 0:
            volume_info = data['items'][0]['volumeInfo']
            
            # Check if available new
            sale_info = data['items'][0].get('saleInfo', {})
            if sale_info.get('saleability') == 'FOR_SALE':
                # Use list price if available new
                list_price = sale_info.get('listPrice', {}).get('amount')
                if list_price:
                    return min(float(list_price) * 1.2, 100)  # Cap at $100
            
            # Check retail price sources
            retail_price = volume_info.get('retailPrice', {}).get('amount')
            if retail_price:
                return min(float(retail_price) * 1.2, 100)
        
        # Fallback to insurance valuation for used/out-of-print
        return calculate_insurance_price(title, author)
        
    except Exception as e:
        print(f"Error in MSRP price calculation: {e}")
        return calculate_insurance_price(title, author)

def calculate_insurance_price(title, author):
    """Fallback insurance valuation"""
    # Simplified insurance pricing logic
    base_price = 25.00
    
    # Adjust based on content type (simplified)
    if any(word in title.lower() for word in ['textbook', 'reference', 'technical']):
        base_price = 45.00
    elif any(word in title.lower() for word in ['children', 'kids', 'picture']):
        base_price = 15.00
    
    return base_price

# Alternate edition fallback system
def find_alternate_edition_info(isbn, title, author):
    """Find information from alternate editions of the same book"""
    try:
        if not title:
            return None
            
        # Search by title + primary author
        search_query = f"intitle:{title}"
        if author and author != 'Unknown Author':
            search_query += f"+inauthor:{author.split(',')[0]}"
        
        url = f"https://www.googleapis.com/books/v1/volumes?q={search_query}"
        response = requests.get(url, timeout=10)
        data = response.json()
        
        if data.get('items') and len(data['items']) > 0:
            # Use the first result (most relevant)
            volume_info = data['items'][0]['volumeInfo']
            
            return {
                'title': volume_info.get('title', title),
                'author': ', '.join(volume_info.get('authors', [author])),
                'description': volume_info.get('description', ''),
                'categories': volume_info.get('categories', []),
                'published_date': volume_info.get('publishedDate', '')
            }
    
    except Exception as e:
        print(f"Error in alternate edition search: {e}")
    
    return None

# Flask routes
@app.route('/')
def index():
    """Single-record review interface with progress"""
    with sqlite3.connect(app.config['DATABASE']) as conn:
        cursor = conn.cursor()
        
        # Get current record (first unprocessed)
        cursor.execute('''
            SELECT r.*, rev.decision, rev.notes as review_notes
            FROM records r
            LEFT JOIN reviews rev ON r.id = rev.record_id
            WHERE r.processed = FALSE
            ORDER BY r.record_number
            LIMIT 1
        ''')
        
        record_data = cursor.fetchone()
        
        # Get progress
        cursor.execute('SELECT COUNT(*) FROM records WHERE processed = TRUE')
        processed = cursor.fetchone()[0]
        cursor.execute('SELECT COUNT(*) FROM records')
        total = cursor.fetchone()[0]
        
        if record_data:
            record = {
                'id': record_data[0],
                'record_number': record_data[1],
                'original_line': record_data[2],
                'title': record_data[3],
                'author': record_data[4],
                'isbn': record_data[5],
                'status': record_data[6],
                'price': record_data[7],
                'msrp': record_data[8],
                'call_number': record_data[9],
                'local_call_number': record_data[10],
                'description': record_data[11],
                'notes': record_data[12],
                'cover_url': record_data[13],
                'verified': record_data[14],
                'has_cover': record_data[15],
                'decision': record_data[17],
                'review_notes': record_data[18]
            }
        else:
            record = None
    
    return render_template('batch_index.html', 
                         record=record, 
                         processed=processed, 
                         total=total,
                         progress=int((processed/total)*100) if total > 0 else 0)

@app.route('/api/review', methods=['POST'])
def submit_review():
    """Submit a review decision and move to next record"""
    data = request.json
    
    with sqlite3.connect(app.config['DATABASE']) as conn:
        cursor = conn.cursor()
        
        # Update record as processed
        cursor.execute('UPDATE records SET processed = TRUE WHERE id = ?', 
                      (data['record_id'],))
        
        # Save review decision
        cursor.execute('''
            INSERT INTO reviews (record_id, decision, notes)
            VALUES (?, ?, ?)
        ''', (data['record_id'], data['decision'], data['notes']))
        
        # Update progress
        cursor.execute('UPDATE progress SET processed_records = processed_records + 1')
        
        conn.commit()
    
    return jsonify({'success': True})

@app.route('/api/next')
def next_record():
    """Get the next unprocessed record"""
    with sqlite3.connect(app.config['DATABASE']) as conn:
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT r.*, rev.decision, rev.notes as review_notes
            FROM records r
            LEFT JOIN reviews rev ON r.id = rev.record_id
            WHERE r.processed = FALSE
            ORDER BY r.record_number
            LIMIT 1
        ''')
        
        record_data = cursor.fetchone()
        
        if record_data:
            record = {
                'id': record_data[0],
                'record_number': record_data[1],
                'title': record_data[3],
                'author': record_data[4],
                'isbn': record_data[5],
                'cover_url': record_data[13],
                'has_cover': record_data[15]
            }
            return jsonify({'success': True, 'record': record})
        else:
            return jsonify({'success': False, 'message': 'No more records'})

@app.route('/covers/<filename>')
def serve_cover(filename):
    """Serve cover images"""
    return send_file(os.path.join(app.config['COVERS_DIR'], filename))

# Initialize application
with app.app_context():
    init_db()
    
    # Load all records from input file
    records = parse_input_file()
    print(f"Found {len(records)} records in input file")
    
    with sqlite3.connect(app.config['DATABASE']) as conn:
        cursor = conn.cursor()
        
        # Clear existing progress
        cursor.execute('DELETE FROM progress')
        cursor.execute('INSERT INTO progress (total_records, processed_records) VALUES (?, ?)',
                      (len(records), 0))
        
        # Load records into database with enhanced processing
        for record in records:
            # Check if record already exists
            cursor.execute('SELECT id FROM records WHERE record_number = ?', 
                          (record['record_number'],))
            existing = cursor.fetchone()
            
            if not existing:
                # Enhanced processing: alternate edition fallback
                alt_info = find_alternate_edition_info(record['isbn'], record['title'], record['author'])
                if alt_info:
                    record['title'] = alt_info['title']
                    record['author'] = alt_info['author']
                    record['description'] = alt_info.get('description', '')
                    record['verified'] = True
                
                # MSRP-aware pricing
                record['price'] = calculate_msrp_aware_price(record['isbn'], record['title'], record['author'])
                
                # Safe cover image fetching
                cover_url = None
                if record.get('verified', False):
                    cover_url = safe_fetch_cover_image(record['isbn'], record['title'], record['author'])
                
                cover_filename = None
                if cover_url:
                    cover_filename = download_cover(cover_url, record['isbn'])
                
                cursor.execute('''
                    INSERT INTO records (record_number, original_line, title, author, isbn, 
                                       status, price, description, cover_url, verified, has_cover)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    record['record_number'], record['original_line'], record['title'],
                    record['author'], record['isbn'], 'pending', record['price'],
                    record.get('description', ''), cover_filename,
                    record.get('verified', False), bool(cover_filename)
                ))
        
        conn.commit()

if __name__ == '__main__':
    print("Starting Enhanced Batch Review Application on port 31338...")
    print("Access at: http://localhost:31338")
    print(f"Total records to process: {len(records)}")
    app.run(host='0.0.0.0', port=31338, debug=True)