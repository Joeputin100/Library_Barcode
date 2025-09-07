#!/usr/bin/env python3
"""
Enhanced Flask Review Application with Multi-Source Data Quality Scoring
"""

from flask import Flask, render_template, request, jsonify, send_file
import json
import os
import sqlite3
from datetime import datetime
import requests
from pathlib import Path
import sys

# Add parent directory to path for imports
sys.path.append('..')
try:
    from multi_source_enricher import enrich_with_multiple_sources, SourcePriority
except ImportError:
    print("⚠️  Multi-source enricher not available, using basic enhancement")
    enrich_with_multiple_sources = None
    SourcePriority = None

def safe_json_loads(json_string, default):
    """Safely load JSON string, returning default on error"""
    if not json_string or not isinstance(json_string, str):
        return default
    try:
        return json.loads(json_string)
    except (json.JSONDecodeError, TypeError):
        return default

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
    """Initialize the SQLite database with enhanced fields"""
    with sqlite3.connect(app.config['DATABASE']) as conn:
        cursor = conn.cursor()
        
        # Records table with enhanced fields
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
                data_quality_score REAL,
                data_confidence_scores TEXT,
                data_sources_used TEXT,
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

def enhance_record_with_quality_scoring(record):
    """Enhance record with multi-source data and quality scoring"""
    enhanced = record.copy()
    
    if enrich_with_multiple_sources is not None:
        try:
            cache = {}  # Initialize empty cache
            # Use multi-source enrichment
            enrichment_result = enrich_with_multiple_sources(
                record.get('title', ''),
                record.get('author', ''),
                record.get('isbn', ''),
                record.get('lccn', ''),
                cache
            )
            
            # Update record with enriched data
            enhanced.update(enrichment_result['final_data'])
            
            # Add quality metrics
            enhanced['data_quality_score'] = enrichment_result['quality_score']
            enhanced['data_confidence_scores'] = json.dumps(enrichment_result['confidence_scores'])
            enhanced['data_sources_used'] = json.dumps(enrichment_result['source_results'])
            
        except Exception as e:
            print(f"Error in multi-source enrichment: {e}")
            # Fallback to basic enhancement if multi-source fails
            enhanced = enhance_record_data_basic(record)
    else:
        # Use basic enhancement if multi-source is not available
        enhanced = enhance_record_data_basic(record)
    
    return enhanced

def enhance_record_data_basic(record):
    """Basic enhancement fallback"""
    enhanced = record.copy()
    
    # Basic Google Books fallback
    if record.get('title') and record.get('author') and not record.get('google_genres'):
        try:
            from api_calls import get_book_metadata_google_books
            
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
            print(f"Error in basic enhancement: {e}")
    
    return enhanced

def get_quality_color(score):
    """Get color based on quality score"""
    if score >= 0.8:
        return '#27ae60'  # Green - high quality
    elif score >= 0.6:
        return '#f39c12'  # Orange - medium quality
    else:
        return '#e74c3c'  # Red - low quality

def get_source_icon(source_name):
    """Get icon for data source"""
    icons = {
        'GOOGLE_BOOKS': '📚',
        'LIBRARY_OF_CONGRESS': '🏛️',
        'OPEN_LIBRARY': '📖',
        'GOODREADS': '⭐',
        'LIBRARY_THING': '📊',
        'WIKIPEDIA': '🌐',
        'ISBNDB': '🏷️',
        'VERTEX_AI': '🤖',
        'ORIGINAL': '📋'
    }
    return icons.get(source_name, '❓')

# Flask routes
@app.route('/')
def index():
    """Main review interface with quality indicators"""
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
                'created_at': row[11],
                'data_quality_score': row[12],
                'data_confidence_scores': safe_json_loads(row[13], {}),
                'data_sources_used': safe_json_loads(row[14], []),
                'decision': row[15],
                'review_notes': row[16]
            }
            records.append(record)
    
    return render_template('index.html', records=records)

@app.route('/record/<int:record_id>')
def single_record(record_id):
    """Single record review interface with credibility scoring"""
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
            'created_at': row[11],
            'data_quality_score': row[12],
            'data_confidence_scores': safe_json_loads(row[13], {}),
            'data_sources_used': safe_json_loads(row[14], []),
            'decision': row[15],
            'review_notes': row[16]
        }
        
        # Enhance record data with quality scoring
        record = enhance_record_with_quality_scoring(record)
        
        # Update database with enhanced data
        cursor.execute('''
            UPDATE records SET 
                data_quality_score = ?,
                data_confidence_scores = ?,
                data_sources_used = ?
            WHERE id = ?
        ''', (
            record.get('data_quality_score'),
            json.dumps(record.get('data_confidence_scores', {})),
            json.dumps(record.get('data_sources_used', [])),
            record_id
        ))
        
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
        
        cursor.execute('SELECT COUNT(*) FROM reviews WHERE decision = "approve"')
        approved_count = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM reviews WHERE decision = "reject"')
        rejected_count = cursor.fetchone()[0]
        
        # Calculate progress percentage
        progress_percent = int(((approved_count + rejected_count) / total_records) * 100) if total_records > 0 else 0
        
        conn.commit()
    
    return render_template('enhanced_single_record.html', 
                         record=record,
                         next_record_id=next_record_id,
                         total_records=total_records,
                         pending_count=pending_count,
                         approved_count=approved_count,
                         rejected_count=rejected_count,
                         progress_percent=progress_percent,
                         get_quality_color=get_quality_color,
                         get_source_icon=get_source_icon)

# Keep existing API routes...
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
    
    # Load current records into database (keep existing logic)
    # This would need to be updated to include quality scoring during initial load
    
if __name__ == '__main__':
    print("Starting Enhanced Flask review application on port 31339...")
    print("Access at: http://localhost:31339")
    app.run(host='0.0.0.0', port=31339, debug=True)