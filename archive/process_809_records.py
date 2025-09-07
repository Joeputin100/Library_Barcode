"""
Process all 809 records through Mangle for Phase 4 completion
"""

import json
from simple_mangle_integration import run_mangle_enrichment
from loc_integration import get_loc_data
from vertex_ai_integration import get_vertex_ai_data
from open_library_integration import get_open_library_data
from datetime import datetime
import logging
import time

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_all_records():
    """Load all 809 records from the enriched data file"""
    try:
        with open('enriched_data_full.json', 'r') as f:
            data = json.load(f)
        
        logger.info(f"Loaded {len(data.get('enriched_records', []))} records from enriched_data_full.json")
        return data.get('enriched_records', [])
        
    except Exception as e:
        logger.error(f"Failed to load records: {e}")
        return []

def prepare_mangle_input(record):
    """Prepare Mangle input from enriched record with real API data"""
    
    barcode = record.get("barcode", "")
    
    # MARC data (base record)
    marc_data = {
        "barcode": barcode,
        "title": record.get("title", ""),
        "author": record.get("author", ""),
        "call_number": record.get("call_number", ""),
        "lccn": record.get("lccn", ""),
        "isbn": record.get("isbn", "")
    }
    
    # Google Books data (from existing enrichment)
    google_data = {
        "barcode": barcode,
        "title": record.get("title", ""),
        "author": record.get("author", ""),
        "genres": ",".join(record.get("google_genres", [])),
        "classification": record.get("google_classification", ""),
        "series": record.get("series", ""),
        "volume": record.get("volume", ""),
        "year": record.get("publication_year", ""),
        "description": record.get("description", "")
    }
    
    # Get real API data with rate limiting
    loc_data = None
    vertex_data = None
    open_lib_data = None
    
    try:
        # Library of Congress data
        loc_data = get_loc_data(marc_data)
        if loc_data:
            logger.debug(f"Found LOC data for {barcode}")
        time.sleep(0.1)  # Rate limiting
    except Exception as e:
        logger.error(f"LOC API failed for {barcode}: {e}")
    
    try:
        # Vertex AI classification
        vertex_data = get_vertex_ai_data(marc_data, google_data)
        if vertex_data:
            logger.debug(f"Got Vertex AI classification for {barcode}")
        time.sleep(0.05)  # Rate limiting
    except Exception as e:
        logger.error(f"Vertex AI failed for {barcode}: {e}")
    
    try:
        # Open Library data
        open_lib_data = get_open_library_data(marc_data)
        if open_lib_data:
            logger.debug(f"Found Open Library data for {barcode}")
        time.sleep(0.1)  # Rate limiting
    except Exception as e:
        logger.error(f"Open Library failed for {barcode}: {e}")
    
    # Prepare data for Mangle in the expected format
    mangle_inputs = []
    
    # Always include MARC and Google data
    mangle_inputs.append({
        'type': 'marc_record',
        'data': marc_data,
        'source': 'MARC'
    })
    
    mangle_inputs.append({
        'type': 'google_books_data', 
        'data': google_data,
        'source': 'GOOGLE_BOOKS'
    })
    
    # Add other sources if available
    if loc_data:
        mangle_inputs.append({
            'type': 'loc_data',
            'data': {
                "barcode": barcode,
                "title": loc_data.get('title', ''),
                "author": loc_data.get('author', ''),
                "classification": loc_data.get('classification', ''),
                "subjects": ",".join(loc_data.get('subjects', [])),
                "publisher": loc_data.get('publisher', ''),
                "year": loc_data.get('publication_year', ''),
                "description": loc_data.get('description', '')
            },
            'source': 'LIBRARY_OF_CONGRESS'
        })
    
    if vertex_data:
        mangle_inputs.append({
            'type': 'vertex_ai_data',
            'data': vertex_data,
            'source': 'VERTEX_AI'
        })
    
    if open_lib_data:
        mangle_inputs.append({
            'type': 'open_library_data',
            'data': {
                "barcode": barcode,
                "title": open_lib_data.get('title', ''),
                "author": open_lib_data.get('author', ''),
                "classification": "",  # Open Library doesn't provide classification
                "subjects": ",".join(open_lib_data.get('subjects', [])),
                "publisher": open_lib_data.get('publisher', ''),
                "year": open_lib_data.get('publication_year', ''),
                "description": open_lib_data.get('description', '')
            },
            'source': 'OPEN_LIBRARY'
        })
    
    return mangle_inputs

def process_batch(records, batch_size=50):
    """Process records in batches with multiple API sources"""
    results = []
    processed = 0
    failed = 0
    
    # Track which sources are actually used
    source_usage = {
        "LIBRARY_OF_CONGRESS": 0,
        "GOOGLE_BOOKS": 0,
        "VERTEX_AI": 0,
        "OPEN_LIBRARY": 0
    }
    
    for i, record in enumerate(records):
        try:
            # Prepare Mangle input from all APIs
            mangle_inputs = prepare_mangle_input(record)
            
            # Skip records without barcodes (ISBN-only entries)
            barcode = None
            for input_data in mangle_inputs:
                if input_data['type'] == 'marc_record':
                    barcode = input_data['data'].get("barcode", "")
                    break
            
            if not barcode:
                continue
            
            # Track which sources we actually have data for
            for input_data in mangle_inputs:
                source = input_data.get('source')
                if source and source in source_usage:
                    source_usage[source] += 1
            
            # Process through Mangle
            mangle_results = run_mangle_enrichment(mangle_inputs)
            
            if mangle_results:
                # Add source information to results
                for result in mangle_results:
                    result.update({
                        "original_record_number": record.get("record_number"),
                        "source": record.get("source"),
                        "processing_timestamp": datetime.utcnow().isoformat(),
                        "mangle_version": "final_rules_v1",
                        "api_sources_used": list(source_usage.keys())
                    })
                results.extend(mangle_results)
                processed += 1
            else:
                failed += 1
            
            # Log progress and update state with source information
            if (i + 1) % 10 == 0:
                logger.info(f"Processed {i + 1}/{len(records)} records")
                logger.info(f"Source usage: {source_usage}")
                update_enrichment_state(i + 1, source_usage)
                
        except Exception as e:
            logger.error(f"Error processing record {i}: {e}")
            failed += 1
    
    return results, processed, failed, source_usage

def update_enrichment_state(processed_count, source_usage=None):
    """Update the enrichment state file for visualizer monitoring"""
    try:
        # Read current state
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
        
        # Update state
        state["timestamp"] = datetime.utcnow().isoformat()
        state["total_records"] = processed_count
        
        # Update source counts if provided
        if source_usage:
            for source, count in source_usage.items():
                if source in state["source_counts"]:
                    state["source_counts"][source] = count
        
        state["source_counts"]["NO_ENRICHMENT"] = 809 - processed_count
        state["overall_progress"] = (processed_count / 809) * 100
        
        # Save updated state
        with open("mangle_enrichment_state.json", "w") as f:
            json.dump(state, f, indent=2)
            
    except Exception as e:
        logger.error(f"Failed to update enrichment state: {e}")

def save_results(results, filename="mangle_processed_results.json"):
    """Save Mangle processing results"""
    try:
        with open(filename, 'w') as f:
            json.dump({
                "processed_timestamp": datetime.utcnow().isoformat(),
                "total_records": len(results),
                "results": results
            }, f, indent=2)
        
        logger.info(f"Saved {len(results)} processed records to {filename}")
        
    except Exception as e:
        logger.error(f"Failed to save results: {e}")

def main():
    """Main processing function"""
    logger.info("Starting Mangle processing of 809 records...")
    
    # Load all records
    records = load_all_records()
    if not records:
        logger.error("No records found to process")
        return
    
    logger.info(f"Found {len(records)} records to process")
    
    # Process records
    results, processed, failed, source_usage = process_batch(records)
    
    # Save results
    save_results(results)
    
    # Update final state with source usage
    update_enrichment_state(processed, source_usage)
    
    # Summary
    logger.info(f"Processing complete!")
    logger.info(f"Successfully processed: {processed}")
    logger.info(f"Failed: {failed}")
    logger.info(f"Total results: {len(results)}")
    
    # Show sample results
    if results:
        logger.info("Sample Mangle results:")
        for i, result in enumerate(results[:3]):
            logger.info(f"Result {i + 1}: {result['barcode']} - {result['final_title']} by {result['final_author']} ({result['final_classification']})")

if __name__ == "__main__":
    main()