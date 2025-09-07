#!/usr/bin/env python3
"""
CSV Export of Enrichment Status for 809 Records
Exports to ~/storage/shared/Download/enrichment_status.csv
"""

import json
import csv
import os
from datetime import datetime

def load_enriched_data():
    """Load the enriched data from BigQuery or local JSON"""
    try:
        # Try to load from local enriched data first
        with open('enriched_data_full.json', 'r') as f:
            data = json.load(f)
        return data.get('enriched_records', [])
    except FileNotFoundError:
        print("Local enriched data not found, checking BigQuery...")
        # Fallback to BigQuery if local file not available
        try:
            from google.cloud import bigquery
            client = bigquery.Client(project='static-webbing-461904-c4')
            query = """
                SELECT 
                    barcode,
                    final_title as title,
                    final_author as author,
                    isbn,
                    lccn,
                    google_books_title IS NOT NULL as google_books_enriched,
                    vertex_ai_classification IS NOT NULL as vertex_ai_enriched,
                    marc_title IS NOT NULL as loc_enriched,
                    open_library_title IS NOT NULL as open_library_enriched
                FROM `barcode.mangle_enriched_books`
                ORDER BY barcode
            """
            query_job = client.query(query)
            results = []
            for row in query_job:
                results.append(dict(row))
            return results
        except Exception as e:
            print(f"BigQuery query failed: {e}")
            return []

def load_current_enrichment_state():
    """Load current enrichment state to check what's been processed"""
    try:
        with open('mangle_enrichment_state.json', 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        return {}

def generate_enrichment_csv():
    """Generate CSV with enrichment status for all 809 records"""
    
    # Load data
    records = load_enriched_data()
    current_state = load_current_enrichment_state()
    
    if not records:
        print("❌ No records found to export")
        return False
    
    print(f"📊 Found {len(records)} records to export")
    
    # Prepare output file
    output_dir = os.path.expanduser('~/storage/shared/Download')
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, 'enrichment_status.csv')
    
    # CSV headers
    fieldnames = [
        'barcode',
        'title', 
        'author',
        'isbn',
        'lccn',
        'loc_enriched',
        'google_books_enriched',
        'vertex_ai_enriched',
        'open_library_enriched',
        'fully_enriched'
    ]
    
    # Write CSV
    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        
        processed_count = 0
        for record in records:
            # Extract basic info
            barcode = record.get('barcode', '')
            title = record.get('title', record.get('final_title', ''))
            author = record.get('author', record.get('final_author', ''))
            isbn = record.get('isbn', '')
            lccn = record.get('lccn', '')
            
            # Check enrichment status from data_quality.sources_used
            sources_used = record.get('data_quality', {}).get('sources_used', [])
            source_names = [source['source'] for source in sources_used]
            
            loc_enriched = 'X' if any('LIBRARY_OF_CONGRESS' in source or 'LOC' in source for source in source_names) else ''
            google_books_enriched = 'X' if any('GOOGLE' in source for source in source_names) else ''
            vertex_ai_enriched = 'X' if any('VERTEX' in source for source in source_names) else ''
            open_library_enriched = 'X' if any('OPEN_LIBRARY' in source for source in source_names) else ''
            
            # Check if fully enriched (all sources)
            fully_enriched = 'X' if all([
                loc_enriched, 
                google_books_enriched, 
                vertex_ai_enriched, 
                open_library_enriched
            ]) else ''
            
            writer.writerow({
                'barcode': barcode,
                'title': title,
                'author': author,
                'isbn': isbn,
                'lccn': lccn,
                'loc_enriched': loc_enriched,
                'google_books_enriched': google_books_enriched,
                'vertex_ai_enriched': vertex_ai_enriched,
                'open_library_enriched': open_library_enriched,
                'fully_enriched': fully_enriched
            })
            
            processed_count += 1
            if processed_count % 100 == 0:
                print(f"Processed {processed_count} records...")
    
    print(f"✅ CSV exported successfully to: {output_file}")
    print(f"📋 Total records exported: {processed_count}")
    
    # Show summary
    if current_state:
        print(f"\n📈 Current Enrichment Progress:")
        print(f"   Total records processed: {current_state.get('total_records', 0)}")
        print(f"   Overall progress: {current_state.get('overall_progress', 0):.1f}%")
        
        source_counts = current_state.get('source_counts', {})
        print(f"   Library of Congress: {source_counts.get('LIBRARY_OF_CONGRESS', 0)}")
        print(f"   Google Books: {source_counts.get('GOOGLE_BOOKS', 0)}")
        print(f"   Vertex AI: {source_counts.get('VERTEX_AI', 0)}")
        print(f"   Open Library: {source_counts.get('OPEN_LIBRARY', 0)}")
    
    return True

if __name__ == "__main__":
    generate_enrichment_csv()