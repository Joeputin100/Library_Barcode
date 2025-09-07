#!/usr/bin/env python3
"""
Flask Review Application for Barcode Project
Port: 31337

This application provides a web interface for reviewing book records,
approving recommendations, and adding notes for CLI follow-up.
"""

from flask import Flask, render_template, request, jsonify, send_file
import json
import os
import sqlite3
from datetime import datetime
import requests
from pathlib import Path

# Initialize Flask app
app = Flask(__name__)
app.config['SECRET_KEY'] = 'barcode-review-secret-key-2024'
app.config['DATABASE'] = 'data/reviews.db'
app.config['COVERS_DIR'] = 'static/covers'

# Ensure directories exist
os.makedirs('data', exist_ok=True)
os.makedirs('static/covers', exist_ok=True)

# Database initialization
def init_db():
    """Initialize the SQLite database"""
    with sqlite3.connect(app.config['DATABASE']) as conn:
        cursor = conn.cursor()
        
        # Records table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS records (
                id INTEGER PRIMARY KEY,
                record_number INTEGER,
                title TEXT,
                author TEXT,
                isbn TEXT,
                status TEXT,
                price REAL,
                call_number TEXT,
                description TEXT,
                notes TEXT,
                cover_url TEXT,
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
        
        conn.commit()

# Load current records from the project
def load_current_records():
    """Load records from current_review_state.json"""
    try:
        with open('../current_review_state.json', 'r') as f:
            data = json.load(f)
        
        records = []
        
        # Add completed records
        for record in data.get('records_completed', []):
            records.append({
                'record_number': record['record_number'],
                'title': record['title'],
                'author': record['author'],
                'isbn': record.get('isbn', ''),
                'status': 'completed',
                'price': float(record.get('insurance_value', '0').replace('$', '')),
                'call_number': record.get('call_number', ''),
                'description': record.get('description', ''),
                'notes': record.get('notes', '')
            })
        
        # Add pending records
        for record in data.get('records_pending', []):
            records.append({
                'record_number': record['record_number'],
                'title': record.get('title', 'Unknown Title'),
                'author': record.get('author', 'Unknown Author'),
                'isbn': record.get('isbn', ''),
                'status': record.get('status', 'pending'),
                'price': 0.0,
                'call_number': '',
                'description': '',
                'notes': record.get('issues', 'Needs review')
            })
        
        return records
    except Exception as e:
        print(f"Error loading records: {e}")
        return []

# Google Books API functions
def fetch_cover_image(isbn, title, author):
    """Fetch cover image from Google Books API"""
    if not isbn:
        return None
    
    try:
        # Try ISBN search first
        url = f"https://www.googleapis.com/books/v1/volumes?q=isbn:{isbn}"
        response = requests.get(url, timeout=10)
        data = response.json()
        
        if data.get('items') and len(data['items']) > 0:
            volume_info = data['items'][0]['volumeInfo']
            if 'imageLinks' in volume_info:
                return volume_info['imageLinks'].get('thumbnail')
        
        # Fallback to title/author search
        if title and author:
            query = f"intitle:{title}+inauthor:{author.split(',')[0]}"
            url = f"https://www.googleapis.com/books/v1/volumes?q={query}"
            response = requests.get(url, timeout=10)
            data = response.json()
            
            if data.get('items') and len(data['items']) > 0:
                volume_info = data['items'][0]['volumeInfo']
                if 'imageLinks' in volume_info:
                    return volume_info['imageLinks'].get('thumbnail')
    
    except Exception as e:
        print(f"Error fetching cover image: {e}")
    
    return None

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

def enhance_record_data(record):
    """Enhance record data with additional information from APIs"""
    enhanced = record.copy()
    
    # If we have basic info but missing enriched data, try to fetch it
    if record.get('title') and record.get('author') and not record.get('google_genres'):
        try:
            # Import here to avoid circular imports
            import sys
            sys.path.append('..')
            from api_calls import get_book_metadata_google_books
            
            # Fetch data from Google Books
            google_data, _, _ = get_book_metadata_google_books(
                record['title'], 
                record['author'], 
                record.get('isbn', ''), 
                {}
            )
            
            if google_data:
                enhanced.update({
                    'publication_year': google_data.get('publication_year', ''),
                    'google_genres': google_data.get('google_genres', []),
                    'publisher': google_data.get('publisher', ''),
                    'page_count': google_data.get('page_count', '')
                })
                
        except Exception as e:
            print(f"Error enhancing record data: {e}")
    
    return enhanced

# Flask routes
@app.route('/')
def index():
    """Main review interface"""
    with sqlite3.connect(app.config['DATABASE']) as conn:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT r.*, rev.decision, rev.notes as review_notes
            FROM records r
            LEFT JOIN reviews rev ON r.id = rev.record_id
            ORDER BY r.record_number
        ''')
        
        records = []
        for row in cursor.fetchall():
            records.append({
                'id': row[0],
                'record_number': row[1],
                'title': row[2],
                'author': row[3],
                'isbn': row[4],
                'status': row[5],
                'price': row[6],
                'call_number': row[7],
                'description': row[8],
                'notes': row[9],
                'cover_url': row[10],
                'decision': row[12],
                'review_notes': row[13]
            })
    
    return render_template('index.html', records=records)

@app.route('/record/<int:record_id>')
def single_record(record_id):
    """Single record review interface"""
    with sqlite3.connect(app.config['DATABASE']) as conn:
        cursor = conn.cursor()
        
        # Get the current record
        cursor.execute('''
            SELECT r.*, rev.decision, rev.notes as review_notes
            FROM records r
            LEFT JOIN reviews rev ON r.id = rev.record_id
            WHERE r.id = ?
        ''', (record_id,))
        
        row = cursor.fetchone()
        if not row:
            return "Record not found", 404
        
        record = {
            'id': row[0],
            'record_number': row[1],
            'title': row[2],
            'author': row[3],
            'isbn': row[4],
            'status': row[5],
            'price': row[6],
            'call_number': row[7],
            'description': row[8],
            'notes': row[9],
            'cover_url': row[10],
            'decision': row[12],
            'review_notes': row[13]
        }
        
        # Enhance record data with additional information
        record = enhance_record_data(record)
        
        # Get next record ID
        cursor.execute('''
            SELECT id FROM records 
            WHERE record_number > ? 
            ORDER BY record_number ASC 
            LIMIT 1
        ''', (record['record_number'],))
        next_record = cursor.fetchone()
        next_record_id = next_record[0] if next_record else None
        
        # Get statistics for progress bar
        cursor.execute('SELECT COUNT(*) FROM records')
        total_records = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM records WHERE status = "pending"')
        pending_count = cursor.fetchone()[0]
        
        cursor.execute('''
            SELECT COUNT(*) FROM reviews 
            WHERE decision = 'approve'
        ''')
        approved_count = cursor.fetchone()[0]
        
        cursor.execute('''
            SELECT COUNT(*) FROM reviews 
            WHERE decision = 'reject'
        ''')
        rejected_count = cursor.fetchone()[0]
        
        # Calculate progress percentage
        progress_percent = int(((approved_count + rejected_count) / total_records) * 100) if total_records > 0 else 0
    
    return render_template('single_record.html', 
                         record=record,
                         next_record_id=next_record_id,
                         total_records=total_records,
                         pending_count=pending_count,
                         approved_count=approved_count,
                         rejected_count=rejected_count,
                         progress_percent=progress_percent)

@app.route('/api/review', methods=['POST'])
def submit_review():
    """Submit a review decision"""
    data = request.json
    
    with sqlite3.connect(app.config['DATABASE']) as conn:
        cursor = conn.cursor()
        
        # Check if review already exists
        cursor.execute('SELECT id FROM reviews WHERE record_id = ?', (data['record_id'],))
        existing = cursor.fetchone()
        
        if existing:
            # Update existing review
            cursor.execute('''
                UPDATE reviews SET decision = ?, notes = ?, reviewed_at = CURRENT_TIMESTAMP
                WHERE record_id = ?
            ''', (data['decision'], data['notes'], data['record_id']))
        else:
            # Insert new review
            cursor.execute('''
                INSERT INTO reviews (record_id, decision, notes)
                VALUES (?, ?, ?)
            ''', (data['record_id'], data['decision'], data['notes']))
        
        conn.commit()
    
    return jsonify({'success': True})

@app.route('/api/export')
def export_reviews():
    """Export review decisions for CLI processing"""
    with sqlite3.connect(app.config['DATABASE']) as conn:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT r.record_number, r.title, r.author, r.isbn, rev.decision, rev.notes
            FROM records r
            JOIN reviews rev ON r.id = rev.record_id
            ORDER BY r.record_number
        ''')
        
        reviews = []
        for row in cursor.fetchall():
            reviews.append({
                'record_number': row[0],
                'title': row[1],
                'author': row[2],
                'isbn': row[3],
                'decision': row[4],
                'notes': row[5]
            })
    
    # Save to JSON file for CLI access
    export_path = '../review_decisions.json'
    with open(export_path, 'w') as f:
        json.dump(reviews, f, indent=2)
    
    return jsonify({'success': True, 'export_path': export_path})

@app.route('/covers/<filename>')
def serve_cover(filename):
    """Serve cover images"""
    return send_file(os.path.join(app.config['COVERS_DIR'], filename))

# Initialize application
with app.app_context():
    init_db()
    
    # Load current records into database
    records = load_current_records()
    with sqlite3.connect(app.config['DATABASE']) as conn:
        cursor = conn.cursor()
        
        for record in records:
            # Check if record already exists
            cursor.execute('SELECT id FROM records WHERE record_number = ?', (record['record_number'],))
            existing = cursor.fetchone()
            
            if not existing:
                # Fetch cover image
                cover_url = fetch_cover_image(record['isbn'], record['title'], record['author'])
                cover_filename = download_cover(cover_url, record['isbn']) if cover_url else None
                
                cursor.execute('''
                    INSERT INTO records (record_number, title, author, isbn, status, price, 
                                       call_number, description, notes, cover_url)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    record['record_number'], record['title'], record['author'], record['isbn'],
                    record['status'], record['price'], record['call_number'], 
                    record['description'], record['notes'], cover_filename
                ))
        
        conn.commit()

if __name__ == '__main__':
    print("Starting Flask review application on port 31337...")
    print("Access at: http://localhost:31337")
    app.run(host='0.0.0.0', port=31337, debug=True)