#!/usr/bin/env python3
"""
Prepare enriched data for Flask app manual review
Loads data into the SQLite database used by the Flask review app
"""

import json
import sqlite3
import os
from datetime import datetime

def load_enriched_data():
    """Load enriched data from JSON file"""
    try:
        with open('enriched_data_batch.json', 'r') as f:
            data = json.load(f)
        return data['enriched_records']
    except FileNotFoundError:
        print("Enriched data file not found. Run processing first.")
        return []

def init_flask_database():
    """Initialize Flask app database with enhanced schema"""
    db_path = 'review_app/data/reviews.db'
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Create enhanced records table with all required columns
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS records (
            id INTEGER PRIMARY KEY,
            record_number INTEGER,
            title TEXT,
            author TEXT,
            isbn TEXT,
            status TEXT DEFAULT 'pending',
            price REAL DEFAULT 0.0,
            call_number TEXT,
            description TEXT,
            notes TEXT,
            cover_url TEXT,
            data_quality_score REAL,
            data_confidence_scores TEXT,
            data_sources_used TEXT,
            original_data TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Add missing columns if they don't exist
    cursor.execute("PRAGMA table_info(records)")
    columns = [col[1] for col in cursor.fetchall()]
    
    if 'original_data' not in columns:
        cursor.execute("ALTER TABLE records ADD COLUMN original_data TEXT")
        print("Added original_data column to records table")
    
    # Create reviews table
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
    conn.close()
    print(f"Database initialized at {db_path}")

def load_data_to_database(records):
    """Load enriched records into the database"""
    db_path = 'review_app/data/reviews.db'
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Clear existing data (optional - comment out to keep existing reviews)
    cursor.execute('DELETE FROM records')
    cursor.execute('DELETE FROM reviews')
    
    for record in records:
        # Prepare data for insertion
        data_quality = record.get('data_quality', {})
        
        cursor.execute('''
            INSERT INTO records 
            (record_number, title, author, isbn, call_number, description, notes,
             data_quality_score, data_confidence_scores, data_sources_used, original_data)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            record.get('record_number'),
            record.get('title', 'Unknown Title'),
            record.get('author', 'Unknown Author'),
            record.get('isbn', ''),
            record.get('call_number', ''),
            record.get('description', ''),
            record.get('notes', ''),
            data_quality.get('score'),
            json.dumps(data_quality.get('confidence_scores', {})),
            json.dumps(data_quality.get('sources_used', [])),
            json.dumps(record)
        ))
    
    conn.commit()
    
    # Count inserted records
    cursor.execute('SELECT COUNT(*) FROM records')
    count = cursor.fetchone()[0]
    
    conn.close()
    print(f"Loaded {count} records into database")
    return count

def create_review_state():
    """Create review state file for Flask app"""
    state = {
        'processing_completed': datetime.now().isoformat(),
        'total_records': len(load_enriched_data()),
        'data_source': 'enriched_data_batch.json',
        'quality_stats': {
            'average_score': 0.0,
            'min_score': 1.0,
            'max_score': 0.0,
            'high_quality': 0,
            'medium_quality': 0,
            'low_quality': 0
        }
    }
    
    # Calculate quality statistics
    records = load_enriched_data()
    quality_scores = []
    
    for record in records:
        score = record.get('data_quality', {}).get('score', 0)
        quality_scores.append(score)
        
        if score >= 0.8:
            state['quality_stats']['high_quality'] += 1
        elif score >= 0.6:
            state['quality_stats']['medium_quality'] += 1
        else:
            state['quality_stats']['low_quality'] += 1
    
    if quality_scores:
        state['quality_stats']['average_score'] = sum(quality_scores) / len(quality_scores)
        state['quality_stats']['min_score'] = min(quality_scores)
        state['quality_stats']['max_score'] = max(quality_scores)
    
    with open('review_app/flask_review_state.json', 'w') as f:
        json.dump(state, f, indent=2)
    
    print(f"Review state created with quality statistics")
    print(f"Average quality score: {state['quality_stats']['average_score']:.2f}")
    print(f"High quality: {state['quality_stats']['high_quality']} records")
    print(f"Medium quality: {state['quality_stats']['medium_quality']} records")
    print(f"Low quality: {state['quality_stats']['low_quality']} records")

def main():
    """Main function to prepare data for Flask review"""
    print("Preparing data for Flask app manual review...")
    
    # Load enriched data
    records = load_enriched_data()
    if not records:
        print("No enriched data found. Please run processing first.")
        return
    
    print(f"Loaded {len(records)} enriched records")
    
    # Initialize database
    init_flask_database()
    
    # Load data into database
    loaded_count = load_data_to_database(records)
    
    # Create review state
    create_review_state()
    
    print(f"\nPreparation complete!")
    print(f"{loaded_count} records loaded into Flask app database")
    print(f"Access the Flask app with: cd review_app && python enhanced_app.py")
    print(f"Review state: review_app/flask_review_state.json")

if __name__ == "__main__":
    main()