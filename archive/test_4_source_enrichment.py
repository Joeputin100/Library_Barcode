#!/usr/bin/env python3
"""
Test script to enrich 10 records with all 4 sources and measure timings
"""
import json
import time
from datetime import datetime
from api_calls import get_book_metadata_initial_pass
from caching import load_cache, save_cache

def test_4_source_enrichment():
    """Test enrichment of 10 records with all 4 sources"""
    
    # Load test data
    with open('enriched_data_combined_mangle.json', 'r') as f:
        all_data = json.load(f)
    
    # Select 10 random records for representative testing
    import random
    random.seed(42)  # For reproducible results
    test_records = random.sample(all_data, 10)
    cache = load_cache()
    
    print("🚀 Testing 4-Source Enrichment on 10 Records")
    print("=" * 60)
    
    total_times = []
    individual_times = []
    
    for i, record in enumerate(test_records):
        barcode = record['barcode']
        title = record.get('final_title', '')
        author = record.get('final_author', '')
        
        print(f"\n📦 Processing {i+1}/10: {barcode}")
        print(f"   Title: {title}")
        print(f"   Author: {author}")
        
        # Measure full enrichment time
        start_time = time.time()
        
        try:
            metadata, google_cached, loc_cached, openlibrary_success, loc_success, vertex_ai_success = \
                get_book_metadata_initial_pass(title, author, '', '', cache)
            
            end_time = time.time()
            processing_time = end_time - start_time
            total_times.append(processing_time)
            
            print(f"   ✅ Enrichment completed in {processing_time:.2f}s")
            print(f"   📊 Results:")
            print(f"      - Google Books: {'✅' if google_cached or openlibrary_success else '❌'}")
            print(f"      - Library of Congress: {'✅' if loc_cached or loc_success else '❌'}")
            print(f"      - Open Library: {'✅' if openlibrary_success else '❌'}")
            print(f"      - Vertex AI: {'✅' if vertex_ai_success else '❌'}")
            
            # Show Vertex AI details if available
            if vertex_ai_success:
                print(f"      - Vertex AI Classification: {metadata.get('vertex_ai_classification', 'N/A')}")
                print(f"      - Vertex AI Quality: {metadata.get('vertex_ai_quality_score', 'N/A')}/10")
                print(f"      - Vertex AI Confidence: {metadata.get('vertex_ai_confidence', 'N/A')}")
            
        except Exception as e:
            end_time = time.time()
            processing_time = end_time - start_time
            total_times.append(processing_time)
            print(f"   ❌ Error: {e}")
            import traceback
            traceback.print_exc()
            print(f"   ⏱️  Time: {processing_time:.2f}s")
    
    # Calculate statistics
    if total_times:
        print("\n" + "=" * 60)
        print("📈 PERFORMANCE SUMMARY:")
        print(f"   Total records processed: {len(total_times)}")
        print(f"   Total time: {sum(total_times):.2f}s")
        print(f"   Average time per record: {sum(total_times)/len(total_times):.2f}s")
        print(f"   Fastest record: {min(total_times):.2f}s")
        print(f"   Slowest record: {max(total_times):.2f}s")
        
        # Estimate full batch time
        remaining_records = 809 - len(total_times)
        estimated_total_time = (sum(total_times)/len(total_times)) * 809
        print(f"\n   📊 ESTIMATED FULL BATCH (809 records):")
        print(f"   Estimated total time: {estimated_total_time:.2f}s ({estimated_total_time/3600:.2f} hours)")
        print(f"   Estimated time remaining: {(estimated_total_time - sum(total_times))/3600:.2f} hours")

if __name__ == "__main__":
    test_4_source_enrichment()