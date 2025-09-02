"""
Python-Go integration bridge for Google Mangle library
This module provides a Python interface to the Go Mangle engine
"""

import json
import subprocess
import tempfile
import os
from pathlib import Path
from typing import Dict, List, Any

class MangleGoIntegration:
    """Python wrapper for Google Mangle Go library"""
    
    def __init__(self, mangle_dir: str = "./mangle"):
        self.mangle_dir = Path(mangle_dir).absolute()
        self.rules_file = self.mangle_dir / "mangle_final_rules.mg"
        
        # Create the rules file if it doesn't exist
        if not self.rules_file.exists():
            self._create_default_rules()
    
    def _create_default_rules(self):
        """Create default MARC enrichment rules"""
        rules = """
# MARC Book Enrichment Rules

# Base Facts
decimal marc_record(Barcode, Title, Author, CallNumber, LCCN, ISBN).
decimal google_books_data(Barcode, Title, Author, Genres, Classification, Series, Volume, Year, Description).
decimal loc_data(Barcode, CallNumber, Subjects, PubInfo, PhysicalDesc, Notes).
decimal open_library_data(Barcode, Title, Author, Rating, Reviews, Subjects, Desc, FirstPubYear).
decimal vertex_ai_data(Barcode, Classification, Confidence, SourceURLs, Reviews, Genres, SeriesInfo, Years).

# Conflict Resolution with confidence scoring
final_title(Barcode, Title, Source, Confidence) :-
  google_books_data(Barcode, Title, _, _, _, _, _, _, _),
  Source = "google_books",
  Confidence = 0.9.

final_title(Barcode, Title, Source, Confidence) :-
  loc_data(Barcode, _, _, Title, _, _),
  Source = "loc", 
  Confidence = 0.8.

final_title(Barcode, Title, Source, Confidence) :-
  marc_record(Barcode, Title, _, _, _, _),
  Source = "marc",
  Confidence = 0.7.

final_author(Barcode, Author, Source, Confidence) :-
  google_books_data(Barcode, _, Author, _, _, _, _, _, _),
  Source = "google_books",
  Confidence = 0.9.

final_author(Barcode, Author, Source, Confidence) :-
  marc_record(Barcode, _, Author, _, _, _),
  Source = "marc",
  Confidence = 0.8.

final_classification(Barcode, Class, Source, Conf) :-
  vertex_ai_data(Barcode, Class, Conf, _, _, _, _, _),
  Conf >= 0.7,
  Source = "vertex_ai".

final_classification(Barcode, Class, Source, Confidence) :-
  google_books_data(Barcode, _, _, _, Class, _, _, _, _),
  Source = "google_books",
  Confidence = 0.8.

# Main enrichment rule
enriched_book(
    Barcode,
    Title, Author, Classification,
    Year, Genres, Subjects, PhysicalDesc,
    Rating, Reviews, Description, Notes,
    Series, Volume, Sources, Conf
) :-
  final_title(Barcode, Title, _, _),
  final_author(Barcode, Author, _, _),
  final_classification(Barcode, Classification, _, Conf),
  vertex_ai_data(Barcode, _, _, Sources, _, _, Series, Year).
"""
        with open(self.rules_file, 'w') as f:
            f.write(rules)
    
    def _run_mangle_query(self, query: str, data_files: List[str] = None) -> List[Dict]:
        """Execute a Mangle query and return results as JSON"""
        
        # Build command
        cmd = [
            'go', 'run', 'interpreter/mg/mg.go',
            '-exec', query,
            '-load', 'mangle_final_rules.mg'
        ]
        
        if data_files:
            cmd.extend(['-load', ','.join(data_files)])
        
        # Run command
        print(f"Executing command: {' '.join(cmd)}")
        print(f"Working directory: {self.mangle_dir}")
        print(f"Rules file exists: {self.rules_file.exists()}")
        
        result = subprocess.run(
            cmd,
            cwd=self.mangle_dir,
            capture_output=True,
            text=True
        )
        
        if result.returncode != 0:
            raise Exception(f"Mangle query failed. STDOUT: {result.stdout}\nSTDERR: {result.stderr}")
        
        # Parse results
        return self._parse_mangle_output(result.stdout)
    
    def _parse_mangle_output(self, output: str) -> List[Dict]:
        """Parse Mangle output into structured data"""
        results = []
        for line in output.strip().split('\n'):
            if line.startswith('enriched_book(') and line.endswith(')'):
                # Parse enriched_book(/B12345, "Title", "Author", "Classification")
                content = line[len('enriched_book('):-1]
                # Remove the / prefix from barcode
                parts = [part.strip('"') for part in content.split(', ')]
                
                if len(parts) >= 4:
                    # Remove the / prefix from barcode
                    barcode = parts[0].lstrip('/')
                    results.append({
                        'barcode': barcode,
                        'final_title': parts[1],
                        'final_author': parts[2],
                        'final_classification': parts[3]
                    })
        
        return results
    
    def add_marc_data(self, barcode: str, title: str, author: str, 
                     call_number: str, lccn: str, isbn: str) -> str:
        """Add MARC record data and return temporary data file path"""
        data = f'marc_record(/{barcode}, "{title}", "{author}", "{call_number}", "{lccn}", "{isbn}").'
        return self._create_temp_data_file(data)
    
    def add_google_books_data(self, barcode: str, data: Dict[str, Any]) -> str:
        """Add Google Books data"""
        title = data.get("title", "")
        author = data.get("author", "")
        genres = data.get("genres", "")
        classification = data.get("classification", "")
        series = data.get("series", "")
        volume = data.get("volume", "")
        year = data.get("publication_year", "")
        description = data.get("description", "")
        
        data_str = f'google_books_data(/{barcode}, "{title}", "{author}", "{genres}", "{classification}", "{series}", "{volume}", "{year}", "{description}").'
        return self._create_temp_data_file(data_str)
    
    def add_vertex_ai_data(self, barcode: str, data: Dict[str, Any]) -> str:
        """Add Vertex AI data"""
        classification = data.get("classification", "")
        confidence = data.get("confidence", 0)
        source_urls = data.get("source_urls", "")
        reviews = data.get("reviews", "")
        genres = data.get("genres", "")
        series_info = data.get("series_info", "")
        years = data.get("years", "")
        
        data_str = f'vertex_ai_data(/{barcode}, "{classification}", {confidence}, "{source_urls}", "{reviews}", "{genres}", "{series_info}", "{years}").'
        return self._create_temp_data_file(data_str)
    
    def _create_temp_data_file(self, content: str) -> str:
        """Create temporary data file for Mangle"""
        fd, path = tempfile.mkstemp(suffix='.mg', text=True)
        with os.fdopen(fd, 'w') as f:
            f.write(content)
        return path
    
    def execute_enrichment(self, data_files: List[str]) -> List[Dict]:
        """Execute enrichment and return combined results"""
        query = "enriched_book(Barcode, Title, Author, Classification)"
        
        try:
            return self._run_mangle_query(query, data_files)
        finally:
            # Clean up temporary files
            for file_path in data_files:
                if os.path.exists(file_path):
                    os.unlink(file_path)

# Example usage
def test_integration():
    """Test the Mangle Go integration"""
    integration = MangleGoIntegration()
    
    # Create sample data files
    marc_file = integration.add_marc_data(
        "B12345", "Sample Book", "John Doe", 
        "FIC DOE", "123456", "9781234567890"
    )
    
    google_file = integration.add_google_books_data("B12345", {
        "title": "Sample Book Enhanced",
        "author": "Johnathan Doe",
        "genres": "Fiction,Mystery",
        "classification": "FIC",
        "publication_year": "2023"
    })
    
    vertex_file = integration.add_vertex_ai_data("B12345", {
        "classification": "Mystery",
        "confidence": 0.85,
        "source_urls": "https://example.com",
        "genres": "Mystery,Thriller"
    })
    
    # Execute enrichment
    results = integration.execute_enrichment([marc_file, google_file, vertex_file])
    
    print("Mangle Enrichment Results:")
    for result in results:
        print(json.dumps(result, indent=2))
    
    return results

if __name__ == "__main__":
    test_integration()