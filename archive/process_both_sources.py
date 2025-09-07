#!/usr/bin/env python3
"""
Process both data sources through full enrichment and quality control pipeline:
1. B-prefix barcodes from cimb_bibliographic.marc
2. ISBNs from isbns_to_be_entered_2025088.txt
"""

import json
import re
from datetime import datetime
from typing import List, Dict, Any
from marc_processor import load_marc_records, get_field_value
from multi_source_enricher import batch_enrich_with_quality_check
from data_quality_validator import DataQualityValidator
from caching import load_cache, save_cache

def parse_isbn_file(file_path: str) -> List[Dict[str, Any]]:
    """Parse the ISBN text file and extract book information"""
    records = []
    record_number = 1
    
    with open(file_path, 'r') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('Books to be Entered'):
                continue
                
            record = {
                'record_number': record_number,
                'source': 'isbn_file',
                'original_input': line
            }
            
            # Parse different formats
            if line.startswith('NO ISBN:'):
                # Format: "NO ISBN: Title by Author"
                parts = line.replace('NO ISBN:', '').strip().split(' by ')
                if len(parts) >= 2:
                    record['title'] = parts[0].strip()
                    record['author'] = parts[1].strip()
                else:
                    record['title'] = line.replace('NO ISBN:', '').strip()
                    record['author'] = 'Unknown Author'
            elif re.match(r'^\d{10,13}[Xx]?$', line.replace('-', '')):
                # Pure ISBN
                record['isbn'] = line
                record['title'] = 'Unknown Title'
                record['author'] = 'Unknown Author'
            else:
                # Assume it's a title/author line
                record['title'] = line
                record['author'] = 'Unknown Author'
            
            records.append(record)
            record_number += 1
    
    return records

def extract_b_prefix_barcodes(marc_file_path: str) -> List[Dict[str, Any]]:
    """Extract records with barcodes starting with 'B' from MARC file"""
    records = []
    marc_records = load_marc_records(marc_file_path)
    
    for i, record in enumerate(marc_records, 1):
        # Extract fields using get_field_value
        barcodes = get_field_value(record, "holding barcode")
        titles = get_field_value(record, "title")
        authors = get_field_value(record, "author")
        isbns = get_field_value(record, "isbn")
        lccns = get_field_value(record, "lccn")
        call_numbers = get_field_value(record, "call number")
        
        # Check if any barcode starts with 'B'
        b_prefix_barcodes = [b for b in barcodes if b and b.startswith('B')]
        
        if b_prefix_barcodes:
            records.append({
                'record_number': i,
                'source': 'marc_file',
                'barcode': b_prefix_barcodes[0],  # Use first B-prefix barcode
                'title': titles[0] if titles else 'Unknown Title',
                'author': authors[0] if authors else 'Unknown Author',
                'isbn': isbns[0] if isbns else '',
                'lccn': lccns[0] if lccns else '',
                'call_number': call_numbers[0] if call_numbers else '',
                'original_data': {
                    'title': titles[0] if titles else 'Unknown Title',
                    'author': authors[0] if authors else 'Unknown Author',
                    'isbn': isbns[0] if isbns else '',
                    'lccn': lccns[0] if lccns else '',
                    'call_number': call_numbers[0] if call_numbers else ''
                }
            })
    
    return records

def save_processing_state(state: Dict[str, Any], filename: str = 'processing_state.json'):
    """Save processing state to JSON file"""
    state['last_updated'] = datetime.now().isoformat()
    with open(filename, 'w') as f:
        json.dump(state, f, indent=2)
    print(f"State saved to {filename}")

def main():
    """Main processing pipeline"""
    
    # Initialize state
    state = {
        'start_time': datetime.now().isoformat(),
        'sources_processed': [],
        'total_records': 0,
        'records_processed': 0,
        'quality_scores': [],
        'validation_results': []
    }
    
    # Load cache
    cache = load_cache()
    
    # Process ISBN file
    print("Processing ISBN file...")
    isbn_records = parse_isbn_file('isbns_to_be_entered_2025088.txt')
    print(f"Found {len(isbn_records)} records from ISBN file")
    
    # Process MARC file for B-prefix barcodes
    print("Processing MARC file for B-prefix barcodes...")
    marc_records = extract_b_prefix_barcodes('cimb_bibliographic.marc')
    print(f"Found {len(marc_records)} records with B-prefix barcodes")
    
    # Combine records
    all_records = isbn_records + marc_records
    state['total_records'] = len(all_records)
    state['source_counts'] = {
        'isbn_file': len(isbn_records),
        'marc_file': len(marc_records)
    }
    
    save_processing_state(state)
    
    # Enrich records with multi-source data
    print(f"Enriching {len(all_records)} records with multi-source data...")
    enriched_records = batch_enrich_with_quality_check(all_records, cache)
    
    # Update state with enrichment results
    state['enrichment_completed'] = datetime.now().isoformat()
    state['records_processed'] = len(enriched_records)
    state['quality_scores'] = [
        r.get('data_quality', {}).get('score', 0) 
        for r in enriched_records 
        if 'data_quality' in r
    ]
    
    save_processing_state(state)
    
    # Run quality validation
    print("Running data quality validation...")
    validator = DataQualityValidator()
    all_validation_results = []
    
    for record in enriched_records:
        # Convert to validation format
        sources_data = {
            'google_books': record,
            'original': record.get('original_data', {})
        }
        
        barcode = record.get('barcode', f'record_{record["record_number"]}')
        results = validator.validate_record(barcode, sources_data)
        all_validation_results.extend(results)
    
    # Generate validation report
    validation_report = validator.generate_validation_report(all_validation_results)
    state['validation_results'] = validation_report
    
    save_processing_state(state)
    
    # Save enriched data for Flask review
    output_data = {
        'enriched_records': enriched_records,
        'validation_report': validation_report,
        'processing_state': state,
        'timestamp': datetime.now().isoformat()
    }
    
    with open('enriched_data_for_review.json', 'w') as f:
        json.dump(output_data, f, indent=2)
    
    # Update Flask app database
    print("Updating Flask app database...")
    # This would involve loading the enriched data into the SQLite database
    # For now, we'll save the data and the Flask app can load it
    
    print(f"\nProcessing complete!")
    print(f"Total records processed: {len(enriched_records)}")
    print(f"Average quality score: {sum(state['quality_scores']) / len(state['quality_scores']) if state['quality_scores'] else 0:.2f}")
    print(f"Validation issues found: {validation_report['summary']['total_issues']}")
    print(f"Data saved to: enriched_data_for_review.json")
    
    # Final state update
    state['completion_time'] = datetime.now().isoformat()
    state['status'] = 'completed'
    save_processing_state(state)

if __name__ == "__main__":
    main()