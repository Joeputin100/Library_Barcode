"""
Production integration for Mangle-based enrichment
Migrates from current robust_enricher to new architecture
"""

import json
import logging
import time
from typing import Dict, List, Any, Optional
from deepquery_integration import DeepQueryIntegration

logger = logging.getLogger(__name__)

class ProductionMigrator:
    """Migrates from current enrichment to Mangle architecture"""
    
    def __init__(self):
        self.integration = DeepQueryIntegration()
        self.migration_stats = {
            "start_time": None,
            "end_time": None,
            "records_processed": 0,
            "successful_records": 0,
            "failed_records": 0,
            "processing_time": 0
        }
    
    def migrate_existing_data(self) -> bool:
        """Migrate existing extracted data to new architecture"""
        self.migration_stats["start_time"] = time.time()
        
        try:
            # Load current extracted MARC data
            with open("extracted_marc_data_filtered.json", "r") as f:
                marc_data = json.load(f)
            
            logger.info(f"Loaded {len(marc_data)} MARC records for migration")
            
            # Load existing enrichment cache if available
            cache_data = self._load_existing_cache()
            
            # Migrate each record
            successful = 0
            for record in marc_data:
                if self._migrate_record(record, cache_data):
                    successful += 1
            
            self.migration_stats["successful_records"] = successful
            self.migration_stats["records_processed"] = len(marc_data)
            self.migration_stats["failed_records"] = len(marc_data) - successful
            
            # Execute enrichment on all migrated data
            results = self.integration.engine.execute_enrichment()
            
            # Save results
            self._save_migration_results(results)
            
            self.migration_stats["end_time"] = time.time()
            self.migration_stats["processing_time"] = self.migration_stats["end_time"] - self.migration_stats["start_time"]
            
            logger.info(f"Migration completed: {successful}/{len(marc_data)} records")
            logger.info(f"Total processing time: {self.migration_stats['processing_time']:.2f}s")
            
            return True
            
        except Exception as e:
            logger.error(f"Migration failed: {e}")
            self.migration_stats["end_time"] = time.time()
            self.migration_stats["processing_time"] = self.migration_stats["end_time"] - self.migration_stats["start_time"]
            return False
    
    def _load_existing_cache(self) -> Dict[str, Any]:
        """Load existing API cache data"""
        cache_data = {}
        
        # Try to load LOC cache (which appears to contain Google Books data)
        try:
            with open("loc_cache.json", "r") as f:
                loc_cache = json.load(f)
                # Extract Google Books data from LOC cache structure
                google_books_data = {}
                for key, value in loc_cache.items():
                    # Extract barcode from the key (format: "title|author|")
                    if "|" in key:
                        # This appears to be Google Books data stored in LOC cache
                        google_books_data[key] = value
                
                if google_books_data:
                    cache_data["google_books"] = google_books_data
                    logger.info(f"Loaded {len(google_books_data)} Google Books entries from LOC cache")
        except FileNotFoundError:
            logger.warning("LOC cache not found")
        except Exception as e:
            logger.warning(f"Error loading LOC cache: {e}")
        
        return cache_data
    
    def _migrate_record(self, record: Dict[str, Any], cache_data: Dict[str, Any]) -> bool:
        """Migrate a single record to new architecture"""
        try:
            barcode = record.get("barcode")
            if not barcode:
                return False
            
            # Add MARC record to engine
            self.integration.engine.add_marc_record(
                barcode,
                record.get("title", ""),
                record.get("author", ""),
                record.get("call_number", ""),
                record.get("lccn", ""),
                record.get("isbn", "")
            )
            
            # Add cached enrichment data if available
            self._add_cached_enrichment(barcode, cache_data)
            
            return True
            
        except Exception as e:
            logger.warning(f"Failed to migrate record {record.get('barcode')}: {e}")
            return False
    
    def _find_matching_cache_entry(self, barcode: str, cache_entries: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Find cache entry that matches the barcode's title/author"""
        # Load the MARC record to get title/author for matching
        try:
            with open("extracted_marc_data_filtered.json", "r") as f:
                marc_data = json.load(f)
                
            # Find the record for this barcode
            record = next((r for r in marc_data if r.get("barcode") == barcode), None)
            if not record:
                return None
                
            title = record.get("title", "") or ""
            author = record.get("author", "") or ""
            
            # Look for matching cache entries
            for cache_key, cache_value in cache_entries.items():
                cache_key_lower = (cache_key or "").lower()
                if title.lower() in cache_key_lower and author.lower() in cache_key_lower:
                    return cache_value
                    
        except Exception as e:
            logger.warning(f"Error finding cache match for {barcode}: {e}")
        
        return None
    
    def _convert_cache_to_google_books_format(self, cache_data: Dict[str, Any]) -> Dict[str, Any]:
        """Convert cache format to Google Books API response format"""
        return {
            "title": "",  # Will be populated from cache key matching
            "author": "", # Will be populated from cache key matching
            "genres": cache_data.get("google_genres", []),
            "classification": cache_data.get("classification", ""),
            "series": cache_data.get("series_name", ""),
            "volume": cache_data.get("volume_number", ""),
            "publication_year": cache_data.get("publication_year", ""),
            "description": ""  # Not available in cache
        }
    
    def _add_cached_enrichment(self, barcode: str, cache_data: Dict[str, Any]):
        """Add cached enrichment data to engine"""
        # Add Google Books cached data - need to find by title/author match
        if "google_books" in cache_data:
            record = self._find_matching_cache_entry(barcode, cache_data["google_books"])
            if record:
                # Convert cache format to expected Google Books format
                gb_data = self._convert_cache_to_google_books_format(record)
                if gb_data:
                    self.integration.engine.add_google_books_data(barcode, gb_data)
                    logger.debug(f"Added cached Google Books data for barcode {barcode}")
    
    def _save_migration_results(self, results: List[Dict[str, Any]]):
        """Save migration results"""
        # Ensure statistics are complete
        if self.migration_stats["end_time"] is None:
            self.migration_stats["end_time"] = time.time()
            self.migration_stats["processing_time"] = self.migration_stats["end_time"] - self.migration_stats["start_time"]
        
        # Save enriched data
        with open("enriched_data_mangle_production.json", "w") as f:
            json.dump(results, f, indent=2)
        
        # Save migration statistics
        stats_path = "migration_statistics.json"
        with open(stats_path, "w") as f:
            json.dump(self.migration_stats, f, indent=2)
        
        logger.info(f"Saved {len(results)} enriched records to enriched_data_mangle_production.json")
        logger.info(f"Migration statistics saved to {stats_path}")
    
    def generate_migration_report(self) -> Dict[str, Any]:
        """Generate comprehensive migration report"""
        # Load the actual statistics from file
        try:
            with open("migration_statistics.json", "r") as f:
                actual_stats = json.load(f)
        except FileNotFoundError:
            actual_stats = self.migration_stats
        
        records_processed = actual_stats.get("records_processed", 0)
        successful_records = actual_stats.get("successful_records", 0)
        processing_time = actual_stats.get("processing_time", 0)
        
        return {
            "migration_summary": {
                "status": "completed" if actual_stats.get("end_time") else "failed",
                "total_records": records_processed,
                "successful_records": successful_records,
                "failed_records": actual_stats.get("failed_records", 0),
                "success_rate": (successful_records / records_processed * 100) if records_processed > 0 else 0,
                "processing_time_seconds": processing_time
            },
            "architecture_changes": {
                "old_architecture": "Python-based imperative enrichment",
                "new_architecture": "Mangle-based declarative enrichment",
                "key_improvements": [
                    "Declarative conflict resolution rules",
                    "Source provenance tracking", 
                    "Confidence-based data combination",
                    "Better scalability",
                    "Improved maintainability"
                ]
            },
            "performance_metrics": {
                "records_per_second": records_processed / processing_time if processing_time > 0 else 0,
                "average_processing_time_per_record": processing_time / records_processed if records_processed > 0 else 0
            }
        }

def main():
    """Main migration function"""
    print("🚀 Starting Production Migration to Mangle Architecture\n")
    
    migrator = ProductionMigrator()
    
    # Perform migration
    success = migrator.migrate_existing_data()
    
    if success:
        # Generate and display report
        report = migrator.generate_migration_report()
        
        print("✅ Migration Completed Successfully!")
        print("=" * 50)
        
        summary = report["migration_summary"]
        print(f"📊 Records: {summary['successful_records']}/{summary['total_records']} "
              f"({summary['success_rate']:.1f}% success)")
        print(f"⏱️  Time: {summary['processing_time_seconds']:.2f}s "
              f"({report['performance_metrics']['records_per_second']:.1f} records/s)")
        
        print("\n🏗️  Architecture Improvements:")
        for improvement in report["architecture_changes"]["key_improvements"]:
            print(f"   • {improvement}")
            
        print(f"\n📋 Full report saved to migration_statistics.json")
    else:
        print("❌ Migration Failed!")
        
    return success

if __name__ == "__main__":
    main()