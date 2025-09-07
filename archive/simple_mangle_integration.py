"""
Simple Python-Go Mangle integration that works
"""

import subprocess
import tempfile
import os
from pathlib import Path

def run_mangle_enrichment(mangle_inputs: list):
    """Run Mangle enrichment with provided data from multiple sources"""
    
    # Create temporary data files
    with tempfile.NamedTemporaryFile(mode='w', suffix='.mg', delete=False) as f:
        for input_data in mangle_inputs:
            data_type = input_data['type']
            data = input_data['data']
            
            if data_type == 'marc_record':
                f.write(f'marc_record(/{data["barcode"]}, "{data["title"]}", "{data["author"]}", "{data["call_number"]}", "{data["lccn"]}", "{data["isbn"]}").\n')
            elif data_type == 'google_books_data':
                f.write(f'google_books_data(/{data["barcode"]}, "{data["title"]}", "{data["author"]}", "{data["genres"]}", "{data["classification"]}", "{data["series"]}", "{data["volume"]}", "{data["year"]}", "{data["description"]}").\n')
            elif data_type == 'vertex_ai_data':
                f.write(f'vertex_ai_data(/{data["barcode"]}, "{data["classification"]}", {data["confidence"]}, "{data["source_urls"]}", "{data["reviews"]}", "{data["genres"]}", "{data["series_info"]}", "{data["years"]}").\n')
            elif data_type == 'loc_data':
                f.write(f'loc_data(/{data["barcode"]}, "{data["title"]}", "{data["author"]}", "{data["classification"]}", "{data["subjects"]}", "{data["publisher"]}", "{data["year"]}", "{data["description"]}").\n')
            elif data_type == 'open_library_data':
                f.write(f'open_library_data(/{data["barcode"]}, "{data["title"]}", "{data["author"]}", "{data["classification"]}", "{data["subjects"]}", "{data["publisher"]}", "{data["year"]}", "{data["description"]}").\n')
        
        data_file = f.name
    
    try:
        # Run Mangle - use the full MLE-STAR rules for production
        cmd = [
            'go', 'run', 'interpreter/mg/mg.go',
            '-exec', 'enriched_book(Barcode, Title, Author, Classification, Publisher, Year, Subjects)',
            '-load', 'mangle_final_rules.mg,' + data_file
        ]
        
        result = subprocess.run(
            cmd,
            cwd='./mangle',
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
                # Proper parsing of Mangle output format
                parts = []
                current_part = ""
                in_quotes = False
                
                for char in content:
                    if char == '"':
                        in_quotes = not in_quotes
                        current_part += char
                    elif char == ',' and not in_quotes:
                        parts.append(current_part.strip())
                        current_part = ""
                    else:
                        current_part += char
                
                # Add the last part
                if current_part:
                    parts.append(current_part.strip())
                
                # Remove quotes from parts
                parts = [part.strip('"') for part in parts]
                
                if len(parts) >= 7:
                    barcode = parts[0].lstrip('/')
                    results.append({
                        'barcode': barcode,
                        'final_title': parts[1],
                        'final_author': parts[2],
                        'final_classification': parts[3],
                        'final_publisher': parts[4],
                        'final_publication_year': parts[5],
                        'final_subjects': parts[6]
                    })
                elif len(parts) >= 4:
                    # Fallback for simplified output
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
    
    mangle_inputs = [
        {
            'type': 'marc_record',
            'data': {
                "barcode": "B12345",
                "title": "Sample Book", 
                "author": "John Doe",
                "call_number": "FIC DOE",
                "lccn": "123456",
                "isbn": "9781234567890"
            },
            'source': 'MARC'
        },
        {
            'type': 'google_books_data',
            'data': {
                "barcode": "B12345",
                "title": "Sample Book Enhanced",
                "author": "Johnathan Doe", 
                "genres": "Fiction,Mystery",
                "classification": "FIC",
                "series": "Sample Series",
                "volume": "1",
                "year": "2023",
                "description": "Enhanced description"
            },
            'source': 'GOOGLE_BOOKS'
        },
        {
            'type': 'vertex_ai_data',
            'data': {
                "barcode": "B12345", 
                "classification": "Mystery",
                "confidence": 0.85,
                "source_urls": "https://cloud.google.com/vertex-ai",
                "reviews": "AI-generated classification",
                "genres": "Mystery,Thriller",
                "series_info": "Sample Series Info",
                "years": "2023"
            },
            'source': 'VERTEX_AI'
        }
    ]
    
    results = run_mangle_enrichment(mangle_inputs)
    
    print("Mangle Enrichment Results:")
    if results:
        print(f"Found {len(results)} enrichment results:")
        for i, result in enumerate(results, 1):
            print(f"Result {i}:")
            print(f"  Barcode: {result['barcode']}")
            print(f"  Title: {result['final_title']}")
            print(f"  Author: {result['final_author']}")
            print(f"  Classification: {result['final_classification']}")
            print("  ---")
    else:
        print("No results returned from Mangle")
        print("Debug: Check Mangle rules and input format")
    
    return results

if __name__ == "__main__":
    test_simple_integration()