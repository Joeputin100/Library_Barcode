#!/usr/bin/env python3
"""
Resume processing with incremental batches and frequent state updates
"""

import json
import re
from datetime import datetime
from typing import List, Dict, Any
from marc_processor import load_marc_records, get_field_value
from multi_source_enricher import enrich_with_multiple_sources
from data_quality_validator import DataQualityValidator
from caching import load_cache, save_cache

def update_processing_state(state_updates: Dict[str, Any]):
    """Update processing state with new information"""
    try:
        with open('processing_state.json', 'r') as f:
            state = json.load(f)
    except FileNotFoundError:
        state = {
            'start_time': datetime.now().isoformat(),
            'sources_processed': [],
            'total_records': 0,
            'records_processed': 0,
            'quality_scores': [],
            'validation_results': [],
            'batches_completed': 0
        }
    
    state.update(state_updates)
    state['last_updated'] = datetime.now().isoformat()
    
    with open('processing_state.json', 'w') as f:
        json.dump(state, f, indent=2)
    
    print(f"State updated: {state_updates}")

def process_batch(records_batch: List[Dict[str, Any]], batch_num: int, cache: Dict) -> List[Dict[str, Any]]:
    """Process a batch of records with enrichment and validation"""
    enriched_batch = []
    validator = DataQualityValidator()
    
    for i, record in enumerate(records_batch):
        try:
            # Enrich record
            enrichment_result = enrich_with_multiple_sources(
                record.get('title', ''),
                record.get('author', ''),
                record.get('isbn', ''),
                record.get('lccn', ''),
                cache
            )
            
            # Merge with original record
            merged_record = {**record, **enrichment_result['final_data']}
            merged_record['data_quality'] = {
                'score': enrichment_result['quality_score'],
                'confidence_scores': enrichment_result['confidence_scores'],
                'sources_used': enrichment_result['source_results']
            }
            
            enriched_batch.append(merged_record)
            
            # Update state every 10 records
            if (i + 1) % 10 == 0:
                update_processing_state({
                    'records_processed': state.get('records_processed', 0) + 10,
                    'quality_scores': state.get('quality_scores', []) + [enrichment_result['quality_score']]
                })
                
        except Exception as e:
            print(f"Error processing record {record.get('record_number', 'unknown')}: {e}")
            # Add record with error info
            record['processing_error'] = str(e)
            enriched_batch.append(record)
    
    return enriched_batch

def main():
    """Main processing function with incremental batches"""
    print("Resuming processing with incremental batches...")
    
    # Load cache
    cache = load_cache()
    
    # Parse ISBN file (smaller subset for testing)
    print("Parsing ISBN file...")
    isbn_records = []
    with open('isbns_to_be_entered_2025088.txt', 'r') as f:
        for i, line in enumerate(f, 1):
            line = line.strip()
            if not line or line.startswith('Books to be Entered'):
                continue
            
            record = {'record_number': i, 'source': 'isbn_file', 'original_input': line}
            
            if line.startswith('NO ISBN:'):
                parts = line.replace('NO ISBN:', '').strip().split(' by ')
                record['title'] = parts[0].strip() if parts else 'Unknown Title'
                record['author'] = parts[1].strip() if len(parts) > 1 else 'Unknown Author'
            elif re.match(r'^\d{10,13}[Xx]?$', line.replace('-', '')):
                record['isbn'] = line
                record['title'] = 'Unknown Title'
                record['author'] = 'Unknown Author'
            else:
                record['title'] = line
                record['author'] = 'Unknown Author'
            
            isbn_records.append(record)
            
            # Process first 50 records for testing
            if len(isbn_records) >= 50:
                break
    
    # Process MARC file B-prefix barcodes (smaller subset)
    print("Extracting B-prefix barcodes from MARC file...")
    marc_records = []
    marc_data = load_marc_records('cimb_bibliographic.marc')
    
    for i, record in enumerate(marc_data[:100], 1):  # First 100 records
        barcodes = get_field_value(record, "holding barcode")
        b_prefix_barcodes = [b for b in barcodes if b and b.startswith('B')]
        
        if b_prefix_barcodes:
            marc_records.append({
                'record_number': i + len(isbn_records),
                'source': 'marc_file',
                'barcode': b_prefix_barcodes[0],
                'title': get_field_value(record, "title")[0] if get_field_value(record, "title") else 'Unknown Title',
                'author': get_field_value(record, "author")[0] if get_field_value(record, "author") else 'Unknown Author',
                'isbn': get_field_value(record, "isbn")[0] if get_field_value(record, "isbn") else '',
                'lccn': get_field_value(record, "lccn")[0] if get_field_value(record, "lccn") else ''
            })
    
    # Combine records
    all_records = isbn_records + marc_records
    
    update_processing_state({
        'total_records': len(all_records),
        'source_counts': {
            'isbn_file': len(isbn_records),
            'marc_file': len(marc_records)
        }
    })
    
    print(f"Processing {len(all_records)} records in batches...")
    
    # Process in batches
    batch_size = 10
    all_enriched = []
    
    for batch_start in range(0, len(all_records), batch_size):
        batch = all_records[batch_start:batch_start + batch_size]
        batch_num = batch_start // batch_size + 1
        
        print(f"Processing batch {batch_num} ({len(batch)} records)...")
        enriched_batch = process_batch(batch, batch_num, cache)
        all_enriched.extend(enriched_batch)
        
        update_processing_state({
            'batches_completed': batch_num,
            'records_processed': len(all_enriched)
        })
    
    # Save results
    output_data = {
        'enriched_records': all_enriched,
        'total_processed': len(all_enriched),
        'completion_time': datetime.now().isoformat(),
        'cache_stats': {'size': len(cache)}
    }
    
    with open('enriched_data_batch.json', 'w') as f:
        json.dump(output_data, f, indent=2)
    
    # Save cache
    save_cache(cache)
    
    print(f"\nProcessing completed!")
    print(f"Total records processed: {len(all_enriched)}")
    print(f"Results saved to: enriched_data_batch.json")
    print(f"Cache size: {len(cache)} entries")

if __name__ == "__main__":
    main()