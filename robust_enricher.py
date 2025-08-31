#!/usr/bin/env python3
"""
Robust enrichment process with comprehensive state management
for unstable Termux environments.
"""

import json
import requests
import time
import os
import subprocess
import datetime
import logging
import psutil
from typing import Dict, List, Any, Optional
from api_calls import get_book_metadata_google_books, get_vertex_ai_classification_batch
from multi_source_enricher import enrich_with_multiple_sources
from data_transformers import clean_call_number

# Configure detailed logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('enrichment_detailed.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class RobustEnrichmentState:
    """Comprehensive state management for enrichment process"""
    
    def __init__(self, state_file: str = "enrichment_state.json"):
        self.state_file = state_file
        self.state = self._load_state()
        self._last_save_time = time.time()
        self._save_interval = 60  # Save every 60 seconds
        self._records_since_last_save = 0
        self._max_records_between_saves = 20  # Save every 20 records max
    
    def _load_state(self) -> Dict[str, Any]:
        """Load existing state or create new one"""
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, 'r') as f:
                    return json.load(f)
            except (json.JSONDecodeError, FileNotFoundError):
                logger.warning("State file corrupted, creating new state")
        
        # Create fresh state
        return {
            "process_id": f"enrichment_{int(time.time())}",
            "start_time": datetime.datetime.utcnow().isoformat(),
            "last_updated": datetime.datetime.utcnow().isoformat(),
            "status": "initializing",
            "total_records": 0,
            "processed_records": 0,
            "failed_records": 0,
            "current_barcode": None,
            "last_successful_barcode": None,
            "queue_position": 0,
            "api_stats": {
                "loc_calls": 0,
                "google_books_calls": 0,
                "vertex_ai_batches": 0,
                "successful_calls": 0,
                "failed_calls": 0
            },
            "processing_times": {
                "average_time_per_record": 0,
                "total_processing_time": 0,
                "last_processing_time": 0
            },
            "error_log": [],
            "checkpoints": [],
            "environment": {
                "termux_stability": "unstable",
                "last_known_good_state": None,
                "recovery_count": 0,
                "last_recovery_time": None
            },
            "micro_details": []
        }
    
    def save_state(self, force: bool = False) -> None:
        """Save current state to file with intelligent batching"""
        current_time = time.time()
        
        # Check if we should save based on time or record count
        time_elapsed = current_time - self._last_save_time
        should_save = (force or 
                      time_elapsed >= self._save_interval or 
                      self._records_since_last_save >= self._max_records_between_saves)
        
        if not should_save:
            self._records_since_last_save += 1
            return
            
        self.state["last_updated"] = datetime.datetime.utcnow().isoformat()
        try:
            with open(self.state_file, 'w') as f:
                json.dump(self.state, f, indent=2)
            logger.info(f"State saved successfully to {self.state_file} "
                       f"({self._records_since_last_save} records since last save)")
            
            # Reset counters
            self._last_save_time = current_time
            self._records_since_last_save = 0
            
        except Exception as e:
            logger.error(f"Failed to save state: {e}")
            # Don't reset counters on failure - try again next time
    
    def check_memory_usage(self) -> Dict[str, float]:
        """Check current memory usage and return statistics"""
        try:
            process = psutil.Process()
            memory_info = process.memory_info()
            
            # Get system memory
            virtual_memory = psutil.virtual_memory()
            swap_memory = psutil.swap_memory()
            
            return {
                "process_rss_mb": memory_info.rss / 1024 / 1024,
                "process_vms_mb": memory_info.vms / 1024 / 1024,
                "system_used_mb": virtual_memory.used / 1024 / 1024,
                "system_available_mb": virtual_memory.available / 1024 / 1024,
                "system_total_mb": virtual_memory.total / 1024 / 1024,
                "swap_used_mb": swap_memory.used / 1024 / 1024,
                "swap_free_mb": swap_memory.free / 1024 / 1024,
                "memory_percent": virtual_memory.percent,
                "swap_percent": swap_memory.percent
            }
        except Exception as e:
            logger.warning(f"Memory monitoring failed: {e}")
            return {}
    
    def is_memory_critical(self, threshold_percent: float = 90.0) -> bool:
        """Check if memory usage is critically high"""
        memory_stats = self.check_memory_usage()
        if not memory_stats:
            return False
            
        # Check if system memory is critically low
        if memory_stats.get("memory_percent", 0) > threshold_percent:
            logger.warning(f"Critical memory usage: {memory_stats['memory_percent']:.1f}%")
            return True
            
        # Check if swap usage is critically high
        if memory_stats.get("swap_percent", 0) > 80.0:  # 80% swap usage
            logger.warning(f"Critical swap usage: {memory_stats['swap_percent']:.1f}%")
            return True
            
        return False
    
    def update_status(self, status: str, barcode: Optional[str] = None) -> None:
        """Update process status"""
        self.state["status"] = status
        if barcode:
            self.state["current_barcode"] = barcode
        self.save_state()
    
    def record_success(self, barcode: str, processing_time: float, details: Dict[str, Any]) -> None:
        """Record successful processing"""
        self.state["processed_records"] += 1
        self.state["last_successful_barcode"] = barcode
        self.state["processing_times"]["last_processing_time"] = processing_time
        self.state["processing_times"]["total_processing_time"] += processing_time
        self.state["processing_times"]["average_time_per_record"] = (
            self.state["processing_times"]["total_processing_time"] / 
            self.state["processed_records"]
        )
        
        # Add micro-details
        micro_detail = {
            "timestamp": datetime.datetime.utcnow().isoformat(),
            "barcode": barcode,
            "status": "success",
            "processing_time": processing_time,
            "details": details
        }
        self.state["micro_details"].append(micro_detail)
        
        # Keep only last 100 micro-details to prevent state file bloat
        if len(self.state["micro_details"]) > 100:
            self.state["micro_details"] = self.state["micro_details"][-100:]
        
        self.save_state()
    
    def record_failure(self, barcode: str, error: str, details: Dict[str, Any]) -> None:
        """Record processing failure"""
        self.state["failed_records"] += 1
        
        error_entry = {
            "timestamp": datetime.datetime.utcnow().isoformat(),
            "barcode": barcode,
            "error": error,
            "details": details
        }
        self.state["error_log"].append(error_entry)
        
        # Add micro-details
        micro_detail = {
            "timestamp": datetime.datetime.utcnow().isoformat(),
            "barcode": barcode,
            "status": "failure",
            "error": error,
            "details": details
        }
        self.state["micro_details"].append(micro_detail)
        
        self.save_state()
    
    def create_checkpoint(self) -> None:
        """Create recovery checkpoint"""
        checkpoint = {
            "timestamp": datetime.datetime.utcnow().isoformat(),
            "processed_records": self.state["processed_records"],
            "last_successful_barcode": self.state["last_successful_barcode"],
            "queue_position": self.state["queue_position"]
        }
        self.state["checkpoints"].append(checkpoint)
        self.save_state()

def get_loc_data(lccn: str, state: RobustEnrichmentState) -> Optional[str]:
    """Fetch data from Library of Congress API with state tracking"""
    start_time = time.time()
    url = f"https://www.loc.gov/item/{lccn}/?fo=json"
    
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        state.state["api_stats"]["loc_calls"] += 1
        state.state["api_stats"]["successful_calls"] += 1
        
        if "item" in data and "call_number" in data["item"]:
            processing_time = time.time() - start_time
            logger.info(f"LOC API success for LCCN {lccn}: {processing_time:.2f}s")
            return data["item"]["call_number"][0]
        
    except requests.exceptions.RequestException as e:
        state.state["api_stats"]["loc_calls"] += 1
        state.state["api_stats"]["failed_calls"] += 1
        logger.warning(f"LOC API error for LCCN {lccn}: {e}")
    except json.JSONDecodeError:
        state.state["api_stats"]["loc_calls"] += 1
        state.state["api_stats"]["failed_calls"] += 1
        logger.warning(f"JSON parse error for LCCN {lccn}")
    
    return None

def robust_enrichment_process():
    """Main enrichment process with robust state management"""
    
    # Initialize state management
    state = RobustEnrichmentState()
    state.update_status("loading_data")
    
    try:
        # Load extracted data
        with open("extracted_data.json", "r") as f:
            extracted_data = json.load(f)
        
        # Load enrichment queue
        barcodes_to_process = []
        if os.path.exists("enrichment_queue.txt"):
            with open("enrichment_queue.txt", "r") as f:
                barcodes_to_process = [line.strip() for line in f if line.strip()]
        else:
            barcodes_to_process = list(extracted_data.keys())
        
        state.state["total_records"] = len(barcodes_to_process)
        logger.info(f"Loaded {len(barcodes_to_process)} barcodes to process")
        
        # Load LOC cache
        loc_cache = {}
        if os.path.exists("loc_cache.json"):
            try:
                with open("loc_cache.json", "r") as f:
                    loc_cache = json.load(f)
            except (json.JSONDecodeError, FileNotFoundError):
                logger.warning("LOC cache file corrupted, starting fresh")
        
        # Vertex AI credentials (placeholder - should be loaded from secure source)
        vertex_ai_credentials = {
            "project_id": "your-gcp-project-id",
            "private_key_id": "your-private-key-id",
            "private_key": "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n",
            "client_email": "your-service-account-email",
            "client_id": "your-client-id",
        }
        
        state.update_status("processing")
        
        # Process each barcode with comprehensive state tracking
        for i, barcode in enumerate(barcodes_to_process):
            state.state["queue_position"] = i
            record_start_time = time.time()
            
            state.update_status("processing_record", barcode)
            logger.info(f"Processing barcode {i+1}/{len(barcodes_to_process)}: {barcode}")
            
            # Memory monitoring - pause if memory is critical
            if state.is_memory_critical():
                logger.warning("Critical memory usage detected. Pausing for 30 seconds...")
                memory_stats = state.check_memory_usage()
                logger.warning(f"Memory stats: {memory_stats}")
                time.sleep(30)
                
                # Check again after pause
                if state.is_memory_critical():
                    logger.error("Memory still critical after pause. Saving state and exiting.")
                    state.save_state(force=True)
                    return
            
            data = extracted_data.get(barcode)
            if not data:
                logger.warning(f"No data found for barcode {barcode}")
                state.record_failure(barcode, "No data found", {"queue_position": i})
                continue
            
            processing_details = {
                "queue_position": i,
                "has_lccn": bool(data.get("lccn")),
                "has_call_number": bool(data.get("call_number")),
                "api_calls": []
            }
            
            try:
                # LOC API enrichment
                if data.get("lccn") and not data.get("call_number"):
                    loc_start = time.time()
                    call_number = get_loc_data(data["lccn"], state)
                    loc_time = time.time() - loc_start
                    
                    processing_details["api_calls"].append({
                        "service": "loc",
                        "duration": loc_time,
                        "success": call_number is not None
                    })
                    
                    if call_number:
                        data["call_number"] = call_number
                        logger.info(f"Found call number from LOC: {call_number}")
                    else:
                        logger.info(f"No call number from LOC for LCCN {data['lccn']}")
                    
                    time.sleep(1)  # Rate limiting
                
                # Multi-source enrichment (all 8 APIs)
                enrichment_start = time.time()
                enrichment_result = enrich_with_multiple_sources(
                    data.get("title", ""), 
                    data.get("author", ""), 
                    data.get("isbn", ""),
                    data.get("lccn", ""),
                    loc_cache
                )
                enrichment_time = time.time() - enrichment_start
                
                processing_details["api_calls"].append({
                    "service": "multi_source_enrichment",
                    "duration": enrichment_time,
                    "success": enrichment_result["quality_score"] > 0.5,
                    "quality_score": enrichment_result["quality_score"]
                })
                
                # Update all fields from multi-source enrichment
                if enrichment_result["final_data"]:
                    for field, value in enrichment_result["final_data"].items():
                        if value and value not in ["", [], None] and not data.get(field):
                            data[field] = value
                    
                    # Track multi-source enrichment as a single successful call
                    state.state["api_stats"]["multi_source_calls"] = state.state["api_stats"].get("multi_source_calls", 0) + 1
                    if enrichment_result["quality_score"] > 0.5:
                        state.state["api_stats"]["successful_calls"] += 1
                    else:
                        state.state["api_stats"]["failed_calls"] += 1
                
                # Update extracted data
                extracted_data[barcode] = data
                
                # Save progress every 10 records
                if (i + 1) % 10 == 0:
                    with open("extracted_data.json", "w") as f:
                        json.dump(extracted_data, f, indent=4)
                    state.create_checkpoint()
                    logger.info(f"Checkpoint created at record {i+1}")
                
                processing_time = time.time() - record_start_time
                state.record_success(barcode, processing_time, processing_details)
                logger.info(f"Completed {barcode} in {processing_time:.2f}s")
                
                time.sleep(0.1)  # Small delay between records
                
            except Exception as e:
                processing_time = time.time() - record_start_time
                error_msg = f"Error processing {barcode}: {str(e)}"
                logger.error(error_msg)
                state.record_failure(barcode, error_msg, processing_details)
                
                # Save state immediately on error
                state.save_state()
                
                # Short pause before continuing
                time.sleep(2)
        
        # Final save
        with open("extracted_data.json", "w") as f:
            json.dump(extracted_data, f, indent=4)
        
        # Save LOC cache
        with open("loc_cache.json", "w") as f:
            json.dump(loc_cache, f, indent=4)
        
        state.update_status("completed")
        logger.info("Enrichment process completed successfully!")
        
    except Exception as e:
        state.update_status("failed")
        logger.error(f"Fatal error in enrichment process: {e}")
        state.record_failure("process", f"Fatal error: {e}", {"stage": "main_process"})
        raise

if __name__ == "__main__":
    robust_enrichment_process()