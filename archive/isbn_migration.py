"""
ISBN Migration to Mangle Architecture
Migrates ISBN records from text file to Mangle-based enrichment system
"""

import json
import re
import logging
from typing import Dict, List, Any
from deepquery_integration import DeepQueryIntegration

logger = logging.getLogger(__name__)

class ISBNMigrator:
    """Migrates ISBN records to Mangle architecture"""
    
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
    
    def parse_isbn_file(self, file_path: str) -> List[Dict[str, Any]]:
        """Parse the ISBN text file and extract all entries"""
        records = []
        
        try:
            with open(file_path, 'r') as f:
                lines = f.readlines()
            
            # Skip header line
            isbn_lines = lines[1:]  # Skip "Books to be Entered into Atriuum:"
            
            for i, line in enumerate(isbn_lines):
                line = line.strip()
                if not line:
                    continue
                
                record = self._parse_isbn_line(line, i + 2)  # +2 for header and 1-indexing
                if record:
                    records.append(record)
                    
            logger.info(f"Parsed {len(records)} ISBN records from {file_path}")
            
        except Exception as e:
            logger.error(f"Error parsing ISBN file: {e}")
            
        return records
    
    def _parse_isbn_line(self, line: str, line_number: int) -> Dict[str, Any]:
        """Parse a single line from the ISBN file"""
        
        # Handle "NO ISBN" entries
        if line.startswith("NO ISBN:"):
            # Extract title and author from "NO ISBN: Title by Author" format
            parts = line.replace("NO ISBN:", "").strip().split(" by ")
            if len(parts) >= 2:
                title = parts[0].strip()
                author = " by ".join(parts[1:]).strip()
                return {
                    "type": "no_isbn",
                    "title": title,
                    "author": author,
                    "isbn": None,
                    "original_line": line,
                    "line_number": line_number
                }
            else:
                # Just title, no author
                return {
                    "type": "no_isbn", 
                    "title": parts[0].strip(),
                    "author": "",
                    "isbn": None,
                    "original_line": line,
                    "line_number": line_number
                }
        
        # Handle regular ISBN entries
        elif re.match(r'^[0-9X\-]+$', line):
            # Clean ISBN (remove hyphens)
            clean_isbn = line.replace("-", "")
            return {
                "type": "isbn",
                "isbn": clean_isbn,
                "title": "",
                "author": "",
                "original_line": line,
                "line_number": line_number
            }
        
        logger.warning(f"Unrecognized format at line {line_number}: {line}")
        return None
    
    def migrate_isbn_records(self, isbn_file_path: str) -> bool:
        """Migrate all ISBN records to Mangle architecture"""
        self.migration_stats["start_time"] = time.time()
        
        try:
            # Parse ISBN file
            isbn_records = self.parse_isbn_file(isbn_file_path)
            
            logger.info(f"Migrating {len(isbn_records)} ISBN records to Mangle architecture")
            
            # Load existing cache data
            cache_data = self._load_existing_cache()
            
            # Migrate each record
            successful = 0
            for record in isbn_records:
                if self._migrate_isbn_record(record, cache_data):
                    successful += 1
            
            self.migration_stats["successful_records"] = successful
            self.migration_stats["records_processed"] = len(isbn_records)
            self.migration_stats["failed_records"] = len(isbn_records) - successful
            
            # Execute enrichment on all migrated data
            results = self.integration.engine.execute_enrichment()
            
            # Combine with existing MARC data if available
            combined_results = self._combine_with_marc_data(results)
            
            # Save results
            self._save_migration_results(combined_results)
            
            self.migration_stats["end_time"] = time.time()
            self.migration_stats["processing_time"] = self.migration_stats["end_time"] - self.migration_stats["start_time"]
            
            logger.info(f"ISBN migration completed: {successful}/{len(isbn_records)} records")
            logger.info(f"Total processing time: {self.migration_stats['processing_time']:.2f}s")
            
            return True
            
        except Exception as e:
            logger.error(f"ISBN migration failed: {e}")
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
    
    def _migrate_isbn_record(self, record: Dict[str, Any], cache_data: Dict[str, Any]) -> bool:
        """Migrate a single ISBN record to new architecture"""
        try:
            # Generate a unique barcode for ISBN records
            if record["type"] == "isbn":
                barcode = f"ISBN_{record['isbn']}"
            else:
                # For NO ISBN entries, use a hash of title+author
                import hashlib
                title_author = f"{record['title']}{record['author']}".encode()
                barcode_hash = hashlib.md5(title_author).hexdigest()[:8]
                barcode = f"NOISBN_{barcode_hash}"
            
            # Add to Mangle engine
            self.integration.engine.add_marc_record(
                barcode,
                record.get("title", ""),
                record.get("author", ""),
                "",  # call_number
                "",  # lccn
                record.get("isbn", "")  # isbn
            )
            
            # Add cached enrichment data if available
            self._add_cached_enrichment(barcode, record, cache_data)
            
            return True
            
        except Exception as e:
            logger.warning(f"Failed to migrate ISBN record {record.get('original_line')}: {e}")
            return False
    
    def _add_cached_enrichment(self, barcode: str, record: Dict[str, Any], cache_data: Dict[str, Any]):
        """Add cached enrichment data to engine"""
        # Add Google Books cached data - try to find by title/author or ISBN
        if "google_books" in cache_data:
            cache_entry = self._find_matching_cache_entry(record, cache_data["google_books"])
            if cache_entry:
                # Convert cache format to expected Google Books format
                gb_data = self._convert_cache_to_google_books_format(cache_entry)
                if gb_data:
                    self.integration.engine.add_google_books_data(barcode, gb_data)
                    logger.debug(f"Added cached Google Books data for barcode {barcode}")
    
    def _find_matching_cache_entry(self, record: Dict[str, Any], cache_entries: Dict[str, Any]) -> Dict[str, Any]:
        """Find cache entry that matches the record"""
        for cache_key, cache_value in cache_entries.items():
            cache_key_lower = (cache_key or "").lower()
            
            # Try to match by ISBN
            if record["type"] == "isbn" and record["isbn"] and record["isbn"] in cache_key_lower:
                return cache_value
            
            # Try to match by title and author
            title = (record.get("title", "") or "").lower()
            author = (record.get("author", "") or "").lower()
            
            if title and title in cache_key_lower and author and author in cache_key_lower:
                return cache_value
            
            # Try to match by title only
            if title and title in cache_key_lower:
                return cache_value
        
        return None
    
    def _convert_cache_to_google_books_format(self, cache_data: Dict[str, Any]) -> Dict[str, Any]:
        """Convert cache format to Google Books API response format"""
        return {
            "title": "",  # Will be populated from actual record data
            "author": "", # Will be populated from actual record data
            "genres": cache_data.get("google_genres", []),
            "classification": cache_data.get("classification", ""),
            "series": cache_data.get("series_name", ""),
            "volume": cache_data.get("volume_number", ""),
            "publication_year": cache_data.get("publication_year", ""),
            "description": ""  # Not available in cache
        }
    
    def _combine_with_marc_data(self, isbn_results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Combine ISBN results with existing MARC data"""
        combined_results = isbn_results.copy()
        
        # Try to load existing MARC enriched data
        try:
            with open("enriched_data_mangle_production.json", "r") as f:
                marc_results = json.load(f)
                combined_results.extend(marc_results)
                logger.info(f"Combined {len(marc_results)} MARC records with {len(isbn_results)} ISBN records")
        except FileNotFoundError:
            logger.warning("No existing MARC enriched data found")
        except Exception as e:
            logger.warning(f"Error loading MARC data: {e}")
        
        return combined_results
    
    def _save_migration_results(self, results: List[Dict[str, Any]]):
        """Save migration results"""
        # Ensure statistics are complete
        if self.migration_stats["end_time"] is None:
            self.migration_stats["end_time"] = time.time()
            self.migration_stats["processing_time"] = self.migration_stats["end_time"] - self.migration_stats["start_time"]
        
        # Save enriched data
        with open("enriched_data_combined_mangle.json", "w") as f:
            json.dump(results, f, indent=2)
        
        # Save migration statistics
        stats_path = "isbn_migration_statistics.json"
        with open(stats_path, "w") as f:
            json.dump(self.migration_stats, f, indent=2)
        
        logger.info(f"Saved {len(results)} enriched records to enriched_data_combined_mangle.json")
        logger.info(f"ISBN migration statistics saved to {stats_path}")
    
    def generate_migration_report(self) -> Dict[str, Any]:
        """Generate comprehensive migration report"""
        # Load the actual statistics from file
        try:
            with open("isbn_migration_statistics.json", "r") as f:
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
            "record_types": {
                "isbn_records": records_processed,
                "no_isbn_records": "Will be determined after processing"
            },
            "performance_metrics": {
                "records_per_second": records_processed / processing_time if processing_time > 0 else 0,
                "average_processing_time_per_record": processing_time / records_processed if records_processed > 0 else 0
            }
        }

def main():
    """Main migration function"""
    print("🚀 Starting ISBN Migration to Mangle Architecture\n")
    
    migrator = ISBNMigrator()
    
    # Perform migration
    success = migrator.migrate_isbn_records("isbns_to_be_entered_2025088.txt")
    
    if success:
        # Generate and display report
        report = migrator.generate_migration_report()
        
        print("✅ ISBN Migration Completed Successfully!")
        print("=" * 50)
        
        summary = report["migration_summary"]
        print(f"📊 Records: {summary['successful_records']}/{summary['total_records']} "
              f"({summary['success_rate']:.1f}% success)")
        print(f"⏱️  Time: {summary['processing_time_seconds']:.2f}s "
              f"({report['performance_metrics']['records_per_second']:.1f} records/s)")
        
        print(f"\n📋 Full report saved to isbn_migration_statistics.json")
        print(f"📚 Combined enriched data saved to enriched_data_combined_mangle.json")
    else:
        print("❌ ISBN Migration Failed!")
        
    return success

if __name__ == "__main__":
    import time
    main()