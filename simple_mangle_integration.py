"""
Simple Python-Go Mangle integration that works
"""

import subprocess
import tempfile
import os
from pathlib import Path

def run_mangle_enrichment(marc_data: dict, google_data: dict, vertex_data: dict):
    """Run Mangle enrichment with provided data"""
    
    # Create temporary data files
    with tempfile.NamedTemporaryFile(mode='w', suffix='.mg', delete=False) as f:
        f.write(f'marc_record(/{marc_data["barcode"]}, "{marc_data["title"]}", "{marc_data["author"]}", "{marc_data["call_number"]}", "{marc_data["lccn"]}", "{marc_data["isbn"]}").\n')
        f.write(f'google_books_data(/{google_data["barcode"]}, "{google_data["title"]}", "{google_data["author"]}", "{google_data["genres"]}", "{google_data["classification"]}", "{google_data["series"]}", "{google_data["volume"]}", "{google_data["year"]}", "{google_data["description"]}").\n')
        f.write(f'vertex_ai_data(/{vertex_data["barcode"]}, "{vertex_data["classification"]}", {vertex_data["confidence"]}, "{vertex_data["source_urls"]}", "{vertex_data["reviews"]}", "{vertex_data["genres"]}", "{vertex_data["series_info"]}", "{vertex_data["years"]}").\n')
        data_file = f.name
    
    try:
        # Run Mangle
        cmd = [
            'go', 'run', 'interpreter/mg/mg.go',
            '-exec', 'enriched_book(Barcode, Title, Author, Classification)',
            '-load', 'mangle_final_rules.mg,' + data_file
        ]
        
        result = subprocess.run(
            cmd,
            cwd='mangle',
            capture_output=True,
            text=True
        )
        
        if result.returncode != 0:
            print(f"Mangle failed: {result.stderr}")
            return []
        
        # Parse results
        results = []
        for line in result.stdout.strip().split('\n'):
            if line.startswith('enriched_book(') and line.endswith(')'):
                content = line[len('enriched_book('):-1]
                parts = [part.strip('"') for part in content.split(', ')]
                
                if len(parts) >= 4:
                    barcode = parts[0].lstrip('/')
                    results.append({
                        'barcode': barcode,
                        'final_title': parts[1],
                        'final_author': parts[2],
                        'final_classification': parts[3]
                    })
        
        return results
        
    finally:
        # Clean up
        os.unlink(data_file)

def test_simple_integration():
    """Test the simple integration"""
    
    marc_data = {
        "barcode": "B12345",
        "title": "Sample Book", 
        "author": "John Doe",
        "call_number": "FIC DOE",
        "lccn": "123456",
        "isbn": "9781234567890"
    }
    
    google_data = {
        "barcode": "B12345",
        "title": "Sample Book Enhanced",
        "author": "Johnathan Doe", 
        "genres": "Fiction,Mystery",
        "classification": "FIC",
        "series": "Sample Series",
        "volume": "1",
        "year": "2023",
        "description": "Enhanced description"
    }
    
    vertex_data = {
        "barcode": "B12345", 
        "classification": "Mystery",
        "confidence": 0.85,
        "source_urls": "https://example.com",
        "reviews": "Good reviews",
        "genres": "Mystery,Thriller",
        "series_info": "Sample Series Info",
        "years": "2023"
    }
    
    results = run_mangle_enrichment(marc_data, google_data, vertex_data)
    
    print("Mangle Enrichment Results:")
    for result in results:
        print(f"Barcode: {result['barcode']}")
        print(f"Title: {result['final_title']}")
        print(f"Author: {result['final_author']}")
        print(f"Classification: {result['final_classification']}")
        print("---")
    
    return results

if __name__ == "__main__":
    test_simple_integration()