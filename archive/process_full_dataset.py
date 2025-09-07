#!/usr/bin/env python3
"""
Process FULL dataset through enrichment pipeline:
- All 439 records from isbns_to_be_entered_2025088.txt
- All 369 B-prefix records from cimb_bibliographic.marc
"""

import json
import re
from datetime import datetime
from typing import List, Dict, Any
from marc_processor import load_marc_records, get_field_value
from multi_source_enricher import batch_enrich_with_quality_check
from data_quality_validator import DataQualityValidator
from caching import load_cache, save_cache

def parse_full_isbn_file(file_path: str) -> List[Dict[str, Any]]:
    """Parse the complete ISBN file"""
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

def extract_all_b_prefix_barcodes(marc_file_path: str) -> List[Dict[str, Any]]:
    """Extract ALL records with barcodes starting with 'B' from MARC file"""
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

def update_processing_state(state_updates: Dict[str, Any]):
    """Update processing state with new information"""
    try:
        with open('processing_state_full.json', 'r') as f:
            state = json.load(f)
    except FileNotFoundError:
        state = {
            'start_time': datetime.now().isoformat(),
            'total_records': 0,
            'records_processed': 0,
            'quality_scores': [],
            'batches_completed': 0,
            'status': 'processing'
        }
    
    state.update(state_updates)
    state['last_updated'] = datetime.now().isoformat()
    
    with open('processing_state_full.json', 'w') as f:
        json.dump(state, f, indent=2)
    
    print(f"State updated: {list(state_updates.keys())}")

def process_in_batches(records: List[Dict[str, Any]], batch_size: int = 50):
    """Process records in batches with state updates"""
    cache = load_cache()
    all_enriched = []
    
    total_batches = (len(records) + batch_size - 1) // batch_size
    
    for batch_num, batch_start in enumerate(range(0, len(records), batch_size), 1):
        batch = records[batch_start:batch_start + batch_size]
        
        print(f"Processing batch {batch_num}/{total_batches} ({len(batch)} records)...")
        
        try:
            enriched_batch = batch_enrich_with_quality_check(batch, cache)
            all_enriched.extend(enriched_batch)
            
            # Update state
            batch_quality_scores = [r.get('data_quality', {}).get('score', 0) for r in enriched_batch]
            
            update_processing_state({
                'batches_completed': batch_num,
                'records_processed': len(all_enriched),
                'quality_scores': batch_quality_scores,
                'current_batch': batch_num
            })
            
            # Save cache periodically
            if batch_num % 5 == 0:
                save_cache(cache)
                print(f"Cache saved ({len(cache)} entries)")
                
        except Exception as e:
            print(f"Error processing batch {batch_num}: {e}")
            # Add records with error info
            for record in batch:
                record['processing_error'] = str(e)
                all_enriched.append(record)
    
    # Final cache save
    save_cache(cache)
    
    return all_enriched

def main():
    """Process the full dataset"""
    print("Processing FULL dataset...")
    print("=" * 50)
    
    # Parse ISBN file
    print("Parsing ISBN file...")
    isbn_records = parse_full_isbn_file('isbns_to_be_entered_2025088.txt')
    print(f"Found {len(isbn_records)} records from ISBN file")
    
    # Extract MARC B-prefix records
    print("Extracting B-prefix barcodes from MARC file...")
    marc_records = extract_all_b_prefix_barcodes('cimb_bibliographic.marc')
    print(f"Found {len(marc_records)} records with B-prefix barcodes")
    
    # Combine records
    all_records = isbn_records + marc_records
    
    update_processing_state({
        'total_records': len(all_records),
        'source_counts': {
            'isbn_file': len(isbn_records),
            'marc_file': len(marc_records)
        },
        'start_time': datetime.now().isoformat()
    })
    
    print(f"Total records to process: {len(all_records)}")
    print(f"Starting enrichment in batches...")
    
    # Process in batches
    enriched_records = process_in_batches(all_records, batch_size=50)
    
    # Save final results
    output_data = {
        'enriched_records': enriched_records,
        'total_processed': len(enriched_records),
        'completion_time': datetime.now().isoformat(),
        'cache_stats': {'size': len(load_cache())},
        'quality_stats': {
            'average_score': sum(r.get('data_quality', {}).get('score', 0) for r in enriched_records) / len(enriched_records) if enriched_records else 0,
            'total_records': len(enriched_records)
        }
    }
    
    with open('enriched_data_full.json', 'w') as f:
        json.dump(output_data, f, indent=2)
    
    # Final state update
    update_processing_state({
        'status': 'completed',
        'completion_time': datetime.now().isoformat(),
        'final_record_count': len(enriched_records),
        'quality_stats': output_data['quality_stats']
    })
    
    print(f"\nProcessing completed!")
    print(f"Total records processed: {len(enriched_records)}")
    print(f"Results saved to: enriched_data_full.json")
    print(f"Final cache size: {len(load_cache())} entries")

if __name__ == "__main__":
    main()