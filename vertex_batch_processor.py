#!/usr/bin/env python3
"""
Vertex AI Grounded Deep Research Batch Processor
Processes all 808 records through Vertex AI for comprehensive MARC field enrichment
"""

import sqlite3
import json
import time
from datetime import datetime
from caching import load_cache, save_cache
from vertex_grounded_research import perform_grounded_research, apply_research_to_record

def create_batch_processing_plan():
    """Create a processing plan for all records"""
    
    conn = sqlite3.connect('review_app/data/reviews.db')
    cursor = conn.cursor()
    
    # Get all records that need Vertex AI processing
    # We'll process all records for comprehensive enrichment
    cursor.execute('''
        SELECT id, record_number, title, author, isbn, 
               publisher, physical_description, description,
               series_volume, edition, lccn, dewey_decimal
        FROM records 
        ORDER BY record_number
    ''')
    
    all_records = cursor.fetchall()
    conn.close()
    
    # Categorize records by priority based on missing critical fields
    high_priority = []    # Missing series_volume, description, publisher
    medium_priority = []  # Missing other important fields
    low_priority = []     # Mostly complete records
    
    for record in all_records:
        (
            id, record_number, title, author, isbn,
            publisher, physical_desc, description,
            series_volume, edition, lccn, dewey_decimal
        ) = record
        
        record_data = {
            'id': id,
            'record_number': record_number,
            'title': title,
            'author': author,
            'isbn': isbn,
            'publisher': publisher,
            'physical_description': physical_desc,
            'description': description,
            'series_volume': series_volume,
            'edition': edition,
            'lccn': lccn,
            'dewey_decimal': dewey_decimal
        }
        
        # Priority calculation - focus on most missing critical fields
        missing_critical = 0
        if not series_volume or series_volume in ['', 'None']:
            missing_critical += 3  # Highest priority
        if not description or description in ['', 'None']:
            missing_critical += 2
        if not publisher or publisher in ['', 'None']:
            missing_critical += 2
        
        if missing_critical >= 3:
            high_priority.append(record_data)
        elif missing_critical >= 1:
            medium_priority.append(record_data)
        else:
            low_priority.append(record_data)
    
    return {
        'high_priority': high_priority,
        'medium_priority': medium_priority,
        'low_priority': low_priority,
        'total_records': len(all_records)
    }

def process_batch(records_batch, batch_name="", batch_size=10):
    """Process a batch of records through Vertex AI"""
    
    cache = load_cache()
    conn = sqlite3.connect('review_app/data/reviews.db')
    
    results = {
        'processed': 0,
        'cached': 0,
        'new_research': 0,
        'errors': 0,
        'updates_applied': 0,
        'start_time': datetime.now().isoformat()
    }
    
    print(f"\n🚀 Processing {batch_name} batch: {len(records_batch)} records")
    print("=" * 60)
    
    for i, record_data in enumerate(records_batch):
        if i % batch_size == 0 and i > 0:
            print(f"Processed {i}/{len(records_batch)} records...")
            # Auto-save progress every batch_size records
            conn.commit()
            save_cache(cache)
        
        try:
            # Perform grounded research
            research_results, cached = perform_grounded_research(record_data, cache)
            
            if 'error' in research_results:
                print(f"❌ Research failed for record {record_data['record_number']}: {research_results['error']}")
                results['errors'] += 1
                continue
            
            # Apply research to database
            updates_applied = apply_research_to_record(record_data['id'], research_results, conn)
            
            if cached:
                results['cached'] += 1
            else:
                results['new_research'] += 1
            
            results['processed'] += 1
            results['updates_applied'] += updates_applied
            
            print(f"✅ Record {record_data['record_number']}: {updates_applied} fields updated ({'cached' if cached else 'new'})")
            
            # Respectful delay between API calls to avoid rate limiting
            if not cached:
                time.sleep(2)
                
        except Exception as e:
            print(f"❌ Error processing record {record_data['record_number']}: {e}")
            results['errors'] += 1
            continue
    
    # Final commit and cache save
    conn.commit()
    conn.close()
    save_cache(cache)
    
    results['end_time'] = datetime.now().isoformat()
    results['success_rate'] = (results['processed'] / len(records_batch)) * 100 if records_batch else 0
    
    return results

def run_complete_batch_processing():
    """Run complete batch processing for all records"""
    
    print("🎯 Vertex AI Grounded Deep Research Batch Processing")
    print("=" * 70)
    print("Target: Complete MARC field enrichment for all 808 records")
    print("Focus: series_volume (0%), description (0.2%), publisher (0.5%)")
    print("=" * 70)
    
    # Create processing plan
    plan = create_batch_processing_plan()
    
    print(f"📊 Processing Plan:")
    print(f"   High Priority: {len(plan['high_priority'])} records (critical fields missing)")
    print(f"   Medium Priority: {len(plan['medium_priority'])} records (some fields missing)")
    print(f"   Low Priority: {len(plan['low_priority'])} records (mostly complete)")
    print(f"   Total Records: {plan['total_records']}")
    
    overall_results = {}
    
    # Process in priority order
    for priority_level in ['high_priority', 'medium_priority', 'low_priority']:
        records = plan[priority_level]
        if records:
            batch_results = process_batch(records, f"{priority_level.replace('_', ' ')}", batch_size=20)
            overall_results[priority_level] = batch_results
            
            print(f"\n📈 {priority_level.replace('_', ' ').title()} Batch Complete:")
            print(f"   Processed: {batch_results['processed']}/{len(records)}")
            print(f"   Cached: {batch_results['cached']}")
            print(f"   New Research: {batch_results['new_research']}")
            print(f"   Updates Applied: {batch_results['updates_applied']}")
            print(f"   Errors: {batch_results['errors']}")
            print(f"   Success Rate: {batch_results['success_rate']:.1f}%")
            
            # Brief pause between priority levels
            time.sleep(5)
    
    # Generate final report
    print("\n" + "=" * 70)
    print("🎉 BATCH PROCESSING COMPLETE!")
    print("=" * 70)
    
    total_processed = sum(r['processed'] for r in overall_results.values())
    total_errors = sum(r['errors'] for r in overall_results.values())
    total_updates = sum(r['updates_applied'] for r in overall_results.values())
    
    print(f"Total Records Processed: {total_processed}/{plan['total_records']}")
    print(f"Total Field Updates Applied: {total_updates}")
    print(f"Total Errors: {total_errors}")
    print(f"Overall Success Rate: {(total_processed/plan['total_records'])*100:.1f}%")
    
    # Save final results
    with open('vertex_batch_results.json', 'w') as f:
        json.dump(overall_results, f, indent=2)
    
    return overall_results

def monitor_progress():
    """Monitor current processing progress"""
    
    conn = sqlite3.connect('review_app/data/reviews.db')
    cursor = conn.cursor()
    
    # Check current field completion
    cursor.execute('''
        SELECT 
            SUM(CASE WHEN series_volume != '' AND series_volume != 'None' THEN 1 ELSE 0 END) as series_volume_complete,
            SUM(CASE WHEN description != '' AND description != 'None' THEN 1 ELSE 0 END) as description_complete,
            SUM(CASE WHEN publisher != '' AND publisher != 'None' THEN 1 ELSE 0 END) as publisher_complete
        FROM records
    ''')
    
    series_volume, description, publisher = cursor.fetchone()
    total = 808
    
    print("\n📊 Current Critical Field Progress:")
    print("-" * 40)
    print(f"series_volume: {series_volume}/{total} ({series_volume/total*100:.1f}%)")
    print(f"description:   {description}/{total} ({description/total*100:.1f}%)")
    print(f"publisher:     {publisher}/{total} ({publisher/total*100:.1f}%)")
    
    conn.close()

if __name__ == "__main__":
    # First check current status
    monitor_progress()
    
    # Run complete batch processing
    run_complete_batch_processing()
    
    # Final status check
    monitor_progress()