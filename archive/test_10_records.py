#!/usr/bin/env python3
"""
Test script for 10-record enhanced enrichment test
Selects 10 random records, enriches them with multi-source data,
and prepares them for Flask app review
"""
import json
import random
import sys
import os

# Add the current directory to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def select_random_records(data, count=10):
    """Select random records from the dataset"""
    keys = list(data.keys())
    selected_keys = random.sample(keys, min(count, len(keys)))
    return {key: data[key] for key in selected_keys}

def main():
    # Load the extracted data
    print("Loading extracted data...")
    with open('extracted_data.json', 'r') as f:
        data = json.load(f)
    
    print(f"Loaded {len(data)} records")
    
    # Select 10 random records
    print("Selecting 10 random records...")
    test_records = select_random_records(data, 10)
    
    # Save the test records
    with open('test_records.json', 'w') as f:
        json.dump(test_records, f, indent=2)
    
    print("Test records saved to test_records.json")
    
    # Try to import and use the multi-source enricher
    try:
        from multi_source_enricher import batch_enrich_with_quality_check
        
        print("Starting multi-source enrichment...")
        
        # Convert to list format for the enricher
        records_list = [
            {"id": key, **value} for key, value in test_records.items()
        ]
        
        # Enrich with empty cache (will make real API calls)
        cache = {}
        enriched_records = batch_enrich_with_quality_check(records_list, cache)
        
        # Save enriched results
        with open('enriched_test_records.json', 'w') as f:
            json.dump(enriched_records, f, indent=2)
        
        print("Enriched records saved to enriched_test_records.json")
        
        # Prepare for Flask app
        flask_data = {}
        for record in enriched_records:
            record_id = record.get('id', 'unknown')
            flask_data[record_id] = record
        
        with open('flask_test_data.json', 'w') as f:
            json.dump(flask_data, f, indent=2)
        
        print("Flask-ready data saved to flask_test_data.json")
        
        # Print summary
        print("\n=== TEST SUMMARY ===")
        print(f"Records processed: {len(enriched_records)}")
        
        if enriched_records:
            avg_quality = sum(r.get('data_quality', {}).get('score', 0) for r in enriched_records) / len(enriched_records)
            print(f"Average quality score: {avg_quality:.2f}/1.0")
            
            # Show sources used
            sources_used = set()
            for record in enriched_records:
                sources = record.get('data_quality', {}).get('sources_used', [])
                for source in sources:
                    sources_used.add(source.get('source', 'unknown'))
            
            print(f"Sources used: {', '.join(sorted(sources_used))}")
        
    except ImportError as e:
        print(f"Multi-source enricher not available: {e}")
        print("Creating test data structure without enrichment...")
        
        # Create basic test data structure
        test_data = {}
        for key, record in test_records.items():
            test_data[key] = {
                **record,
                "data_quality": {
                    "score": 0.0,
                    "confidence_scores": {},
                    "sources_used": []
                }
            }
        
        with open('flask_test_data.json', 'w') as f:
            json.dump(test_data, f, indent=2)
        
        print("Basic test data saved to flask_test_data.json")
    
    print("\n=== NEXT STEPS ===")
    print("1. Copy flask_test_data.json to review_app/data/")
    print("2. Start the enhanced Flask app:")
    print("   cd review_app && python enhanced_app.py")
    print("3. Access: http://localhost:31338")

if __name__ == "__main__":
    main()