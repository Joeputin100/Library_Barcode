#!/usr/bin/env python3
"""
Parallel Mangle Processor with 4 workers
Uses existing cache system for API responses
"""
import json
import logging
import time
import concurrent.futures
from datetime import datetime
from simple_mangle_integration import run_mangle_enrichment
from caching import load_cache, save_cache
from api_calls import get_book_metadata_initial_pass

logger = logging.getLogger(__name__)

def process_single_record(record, cache):
    """Process a single record with caching"""
    try:
        barcode = record.get("barcode", "")
        if not barcode:
            return None, [], {}
        
        # Get data using existing cached API system
        metadata, google_cached, loc_cached, openlibrary_success, loc_success, vertex_ai_success = \
            get_book_metadata_initial_pass(
                record.get("title", ""),
                record.get("author", ""),
                record.get("isbn", ""),
                record.get("lccn", ""),
                cache,
                is_blank=False,
                is_problematic=False
            )
        
        # Prepare Mangle inputs
        mangle_inputs = []
        source_usage = {}
        
        # MARC data
        mangle_inputs.append({
            'type': 'marc_record',
            'data': {
                "barcode": barcode,
                "title": record.get("title", ""),
                "author": record.get("author", ""),
                "call_number": record.get("call_number", ""),
                "lccn": record.get("lccn", ""),
                "isbn": record.get("isbn", "")
            },
            'source': 'MARC'
        })
        
        # Google Books data
        mangle_inputs.append({
            'type': 'google_books_data',
            'data': {
                "barcode": barcode,
                "title": metadata.get("title", record.get("title", "")),
                "author": metadata.get("author", record.get("author", "")),
                "genres": ",".join(metadata.get("google_genres", [])),
                "classification": metadata.get("classification", ""),
                "series": metadata.get("series_name", ""),
                "volume": metadata.get("volume_number", ""),
                "year": metadata.get("publication_year", ""),
                "description": ""
            },
            'source': 'GOOGLE_BOOKS'
        })
        
        # Track source usage
        source_usage["GOOGLE_BOOKS"] = 1
        
        # LOC data if available
        if loc_success and metadata.get("classification"):
            mangle_inputs.append({
                'type': 'loc_data',
                'data': {
                    "barcode": barcode,
                    "title": metadata.get("title", ""),
                    "author": metadata.get("author", ""),
                    "classification": metadata.get("classification", ""),
                    "subjects": ",".join(metadata.get("genres", [])),
                    "publisher": "",
                    "year": metadata.get("publication_year", ""),
                    "description": ""
                },
                'source': 'LIBRARY_OF_CONGRESS'
            })
            source_usage["LIBRARY_OF_CONGRESS"] = 1
        
        # Vertex AI data if available
        if vertex_ai_success and metadata.get("vertex_ai_classification"):
            mangle_inputs.append({
                'type': 'vertex_ai_data',
                'data': {
                    "barcode": barcode,
                    "classification": metadata.get("vertex_ai_classification", ""),
                    "confidence": 0.8,
                    "source_urls": "https://cloud.google.com/vertex-ai",
                    "reviews": "AI-generated classification",
                    "genres": ",".join(metadata.get("genres", [])),
                    "series_info": "",
                    "years": metadata.get("publication_year", "")
                },
                'source': 'VERTEX_AI'
            })
            source_usage["VERTEX_AI"] = 1
        
        # Process through Mangle
        mangle_results = run_mangle_enrichment(mangle_inputs)
        
        if mangle_results:
            # Add source information
            for result in mangle_results:
                result.update({
                    "original_record_number": record.get("record_number"),
                    "source": record.get("source"),
                    "processing_timestamp": datetime.utcnow().isoformat(),
                    "mangle_version": "final_rules_v1",
                    "api_sources_used": list(source_usage.keys())
                })
        
        return barcode, mangle_results, source_usage
        
    except Exception as e:
        logger.error(f"Error processing record {record.get('barcode', 'unknown')}: {e}")
        return None, [], {}

def process_batch_parallel(records, batch_size=50, max_workers=4):
    """Process records in parallel batches"""
    results = []
    processed = 0
    failed = 0
    total_source_usage = {
        "LIBRARY_OF_CONGRESS": 0,
        "GOOGLE_BOOKS": 0,
        "VERTEX_AI": 0,
        "OPEN_LIBRARY": 0
    }
    
    # Load cache
    cache = load_cache()
    
    # Process in parallel
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_record = {
            executor.submit(process_single_record, record, cache): record 
            for record in records
        }
        
        for i, future in enumerate(concurrent.futures.as_completed(future_to_record)):
            record = future_to_record[future]
            try:
                barcode, record_results, source_usage = future.result()
                
                if barcode:
                    results.extend(record_results)
                    processed += 1
                    
                    # Update source usage
                    for source, count in source_usage.items():
                        if source in total_source_usage:
                            total_source_usage[source] += count
                else:
                    failed += 1
                
                # Log progress every 10 records
                if (i + 1) % 10 == 0:
                    logger.info(f"Processed {i + 1}/{len(records)} records")
                    logger.info(f"Source usage: {total_source_usage}")
                    
                    # Update state
                    update_enrichment_state(i + 1, total_source_usage)
                    
            except Exception as e:
                logger.error(f"Error processing record {record.get('barcode', 'unknown')}: {e}")
                failed += 1
    
    # Save cache
    save_cache(cache)
    
    return results, processed, failed, total_source_usage

def update_enrichment_state(processed_count, source_usage=None):
    """Update the enrichment state file"""
    try:
        try:
            with open("mangle_enrichment_state.json", "r") as f:
                state = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            state = {
                "timestamp": datetime.utcnow().isoformat(),
                "total_records": 0,
                "source_counts": {
                    "LIBRARY_OF_CONGRESS": 0,
                    "GOOGLE_BOOKS": 0,
                    "VERTEX_AI": 0,
                    "OPEN_LIBRARY": 0,
                    "NO_ENRICHMENT": 809
                },
                "overall_progress": 0.0
            }
        
        state["timestamp"] = datetime.utcnow().isoformat()
        state["total_records"] = processed_count
        
        if source_usage:
            for source, count in source_usage.items():
                if source in state["source_counts"]:
                    state["source_counts"][source] = count
        
        state["source_counts"]["NO_ENRICHMENT"] = 809 - processed_count
        state["overall_progress"] = (processed_count / 809) * 100
        
        with open("mangle_enrichment_state.json", "w") as f:
            json.dump(state, f, indent=2)
            
    except Exception as e:
        logger.error(f"Failed to update enrichment state: {e}")

def main():
    """Main processing function"""
    logger.info("Starting parallel Mangle processing of 809 records...")
    
    # Load all records
    try:
        with open('enriched_data_full.json', 'r') as f:
            data = json.load(f)
        records = data.get('enriched_records', [])
        logger.info(f"Loaded {len(records)} records from enriched_data_full.json")
    except Exception as e:
        logger.error(f"Failed to load records: {e}")
        return
    
    if not records:
        logger.error("No records found to process")
        return
    
    logger.info(f"Found {len(records)} records to process with 4 parallel workers")
    
    # Process records in parallel
    results, processed, failed, source_usage = process_batch_parallel(records, max_workers=4)
    
    # Save results
    try:
        with open("mangle_processed_results.json", 'w') as f:
            json.dump({
                "processed_timestamp": datetime.utcnow().isoformat(),
                "total_records": len(results),
                "results": results
            }, f, indent=2)
        logger.info(f"Saved {len(results)} processed records to mangle_processed_results.json")
    except Exception as e:
        logger.error(f"Failed to save results: {e}")
    
    # Update final state
    update_enrichment_state(processed, source_usage)
    
    # Summary
    logger.info(f"Processing complete!")
    logger.info(f"Successfully processed: {processed}")
    logger.info(f"Failed: {failed}")
    logger.info(f"Total results: {len(results)}")
    logger.info(f"Final source usage: {source_usage}")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    main()