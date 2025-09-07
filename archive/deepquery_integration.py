"""
DeepQuery integration layer for MARC enrichment
Connects existing APIs with Mangle reasoning engine
"""

import json
import logging
from typing import Dict, List, Any
from mangle_enrichment_engine import MangleEnrichmentEngine
from api_calls import get_book_metadata_google_books, get_vertex_ai_classification_batch

logger = logging.getLogger(__name__)

class DeepQueryIntegration:
    """Integration layer between APIs and Mangle reasoning"""
    
    def __init__(self):
        self.engine = MangleEnrichmentEngine()
        self.cache = {}
    
    def load_marc_data(self, marc_json_path: str):
        """Load MARC data into the knowledge base"""
        try:
            with open(marc_json_path, 'r') as f:
                marc_data = json.load(f)
            
            for record in marc_data:
                self.engine.add_marc_record(
                    record.get("barcode", ""),
                    record.get("title", ""),
                    record.get("author", ""),
                    record.get("call_number", ""),
                    record.get("lccn", ""),
                    record.get("isbn", "")
                )
            
            logger.info(f"Loaded {len(marc_data)} MARC records")
            return len(marc_data)
            
        except Exception as e:
            logger.error(f"Failed to load MARC data: {e}")
            return 0
    
    def enrich_with_apis(self, barcode: str, title: str, author: str, isbn: str):
        """Enrich a single record using all APIs"""
        cache_key = f"{barcode}_{title}_{author}_{isbn}".lower()
        
        # Check cache first
        if cache_key in self.cache:
            return self.cache[cache_key]
        
        enrichment_data = {}
        
        try:
            # Google Books API
            google_data, cached, success = get_book_metadata_google_books(
                title, author, isbn, {}
            )
            if success and google_data:
                self.engine.add_google_books_data(barcode, google_data)
                enrichment_data["google_books"] = google_data
            
            # Vertex AI Batch (simulated for single record)
            vertex_data = self._get_vertex_ai_enrichment(title, author, isbn)
            if vertex_data:
                self.engine.add_vertex_ai_data(barcode, vertex_data)
                enrichment_data["vertex_ai"] = vertex_data
            
            # Cache the results
            self.cache[cache_key] = enrichment_data
            
        except Exception as e:
            logger.error(f"API enrichment failed for {barcode}: {e}")
        
        return enrichment_data
    
    def _get_vertex_ai_enrichment(self, title: str, author: str, isbn: str) -> Dict[str, Any]:
        """Get Vertex AI enrichment with Deep Research prompt"""
        # This would be replaced with actual Vertex AI call
        # Using mock data for demonstration
        return {
            "classification": "Fiction",
            "confidence": 0.92,
            "source_urls": [
                f"https://books.google.com/search?q={title}+{author}",
                f"https://www.loc.gov/search/?q={isbn}"
            ],
            "genres": ["Fiction", "Literary"],
            "series_info": "Sample Series",
            "publication_years": {"copyright": "2023", "original": "2022"},
            "review_snippets": ["A compelling read with rich character development"]
        }
    
    def batch_enrich(self, batch_size: int = 10) -> List[Dict[str, Any]]:
        """Process batch enrichment using Mangle reasoning"""
        try:
            # Get all MARC records from engine
            marc_records = [f["data"] for f in self.engine.facts 
                          if f["predicate"] == "marc_record"]
            
            # Process in batches
            results = []
            for i in range(0, len(marc_records), batch_size):
                batch = marc_records[i:i + batch_size]
                
                for record in batch:
                    self.enrich_with_apis(
                        record["barcode"],
                        record["title"],
                        record["author"],
                        record["isbn"]
                    )
                
                # Execute Mangle reasoning on current batch
                batch_results = self.engine.execute_enrichment()
                results.extend(batch_results)
                
                logger.info(f"Processed batch {i//batch_size + 1}: {len(batch_results)} records")
            
            return results
            
        except Exception as e:
            logger.error(f"Batch enrichment failed: {e}")
            return []
    
    def save_results(self, results: List[Dict[str, Any]], output_path: str):
        """Save enriched results to JSON file"""
        try:
            with open(output_path, 'w') as f:
                json.dump(results, f, indent=2)
            logger.info(f"Saved {len(results)} enriched records to {output_path}")
        except Exception as e:
            logger.error(f"Failed to save results: {e}")

# Example usage
def main():
    """Main integration example"""
    integration = DeepQueryIntegration()
    
    # Load MARC data
    integration.load_marc_data("extracted_marc_data.json")
    
    # Process batch enrichment
    results = integration.batch_enrich(batch_size=5)
    
    # Save results
    integration.save_results(results, "enriched_data_mangle.json")
    
    print(f"Enriched {len(results)} records with Mangle reasoning")

if __name__ == "__main__":
    main()