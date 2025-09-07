"""
Mangle-BigQuery Integration for Phase 4
Process 809 records through Mangle and store in BigQuery
"""

import json
from google.cloud import bigquery
from simple_mangle_integration import run_mangle_enrichment
from typing import List, Dict, Any
import logging
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MangleBigQueryIntegration:
    """Integrate Mangle processing with BigQuery storage"""
    
    def __init__(self):
        self.bq_client = bigquery.Client()
        self.table_id = "barcode.mangle_enriched_books"
        
    def process_single_record(self, marc_data: Dict, google_data: Dict, vertex_data: Dict) -> Dict:
        """Process a single record through Mangle and return enriched data"""
        try:
            results = run_mangle_enrichment(marc_data, google_data, vertex_data)
            if results:
                # Take the first result (Mangle may return multiple possibilities)
                enriched_data = results[0]
                
                # Add source provenance and metadata
                enriched_data.update({
                    "marc_title": marc_data.get("title", ""),
                    "marc_author": marc_data.get("author", ""),
                    "marc_call_number": marc_data.get("call_number", ""),
                    "google_books_title": google_data.get("title", ""),
                    "google_books_author": google_data.get("author", ""),
                    "google_books_classification": google_data.get("classification", ""),
                    "vertex_ai_classification": vertex_data.get("classification", ""),
                    "vertex_ai_confidence": vertex_data.get("confidence", 0.0),
                    "enrichment_timestamp": datetime.utcnow().isoformat(),
                    "processing_version": "mangle-v1.0",
                    "confidence_score": vertex_data.get("confidence", 0.7),
                    "source_combination": ["marc", "google_books", "vertex_ai"],
                    "mangle_rule_version": "final_rules_v1",
                    "rule_execution_time": 0.1,  # Placeholder - would measure actual time
                    "conflicting_sources": len(results) > 1  # True if multiple results (conflicts)
                })
                
                return enriched_data
            
        except Exception as e:
            logger.error(f"Error processing record {marc_data.get('barcode', 'unknown')}: {e}")
            return None
    
    def insert_to_bigquery(self, records: List[Dict]) -> int:
        """Insert enriched records into BigQuery"""
        try:
            errors = self.bq_client.insert_rows_json(self.table_id, records)
            if errors:
                logger.error(f"BigQuery insert errors: {errors}")
                return 0
            
            logger.info(f"Successfully inserted {len(records)} records to BigQuery")
            return len(records)
            
        except Exception as e:
            logger.error(f"BigQuery insertion failed: {e}")
            return 0
    
    def process_batch(self, batch_data: List[Dict]) -> Dict:
        """Process a batch of records"""
        successful = 0
        failed = 0
        
        enriched_records = []
        
        for record_data in batch_data:
            enriched = self.process_single_record(
                record_data.get("marc", {}),
                record_data.get("google_books", {}),
                record_data.get("vertex_ai", {})
            )
            
            if enriched:
                enriched_records.append(enriched)
                successful += 1
            else:
                failed += 1
        
        # Insert to BigQuery
        inserted = self.insert_to_bigquery(enriched_records)
        
        return {
            "total_processed": successful + failed,
            "successful": successful,
            "failed": failed,
            "inserted": inserted,
            "batch_size": len(batch_data)
        }

def load_sample_data() -> List[Dict]:
    """Load sample data for testing"""
    sample_records = []
    
    # Sample record 1
    sample_records.append({
        "marc": {
            "barcode": "B000001",
            "title": "Sample Book One",
            "author": "Author One",
            "call_number": "FIC ONE",
            "lccn": "LCC001",
            "isbn": "1111111111"
        },
        "google_books": {
            "barcode": "B000001",
            "title": "Sample Book One: Enhanced",
            "author": "Author One Updated",
            "genres": "Fiction,Mystery",
            "classification": "FIC",
            "series": "Sample Series",
            "volume": "1",
            "year": "2023",
            "description": "Enhanced description"
        },
        "vertex_ai": {
            "barcode": "B000001",
            "classification": "Mystery",
            "confidence": 0.85,
            "source_urls": "https://example.com/book1",
            "reviews": "Good reviews",
            "genres": "Mystery,Thriller",
            "series_info": "Sample Series Info",
            "years": "2023"
        }
    })
    
    # Sample record 2
    sample_records.append({
        "marc": {
            "barcode": "B000002",
            "title": "Sample Book Two",
            "author": "Author Two",
            "call_number": "FIC TWO",
            "lccn": "LCC002",
            "isbn": "2222222222"
        },
        "google_books": {
            "barcode": "B000002",
            "title": "Sample Book Two: Special Edition",
            "author": "Author Two Revised",
            "genres": "Science Fiction,Fantasy",
            "classification": "SCI-FI",
            "series": "Galaxy Series",
            "volume": "2",
            "year": "2024",
            "description": "Special edition description"
        },
        "vertex_ai": {
            "barcode": "B000002",
            "classification": "Science Fiction",
            "confidence": 0.92,
            "source_urls": "https://example.com/book2",
            "reviews": "Excellent reviews",
            "genres": "Sci-Fi,Space Opera",
            "series_info": "Galaxy Series Info",
            "years": "2024"
        }
    })
    
    return sample_records

def test_integration():
    """Test the Mangle-BigQuery integration"""
    logger.info("Starting Mangle-BigQuery integration test...")
    
    integration = MangleBigQueryIntegration()
    sample_data = load_sample_data()
    
    results = integration.process_batch(sample_data)
    
    logger.info(f"Integration test results: {results}")
    
    return results

if __name__ == "__main__":
    test_integration()