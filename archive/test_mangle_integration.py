"""
Simple test for Mangle integration without processing full dataset
"""

from deepquery_integration import DeepQueryIntegration

def test_small_batch():
    """Test with small sample data"""
    integration = DeepQueryIntegration()
    
    # Add sample MARC records directly
    sample_records = [
        {"barcode": "B000001", "title": "Test Book 1", "author": "Author One", "call_number": "FIC ONE", "lccn": "LCC001", "isbn": "1111111111"},
        {"barcode": "B000002", "title": "Test Book 2", "author": "Author Two", "call_number": "FIC TWO", "lccn": "LCC002", "isbn": "2222222222"}
    ]
    
    for record in sample_records:
        integration.engine.add_marc_record(
            record["barcode"],
            record["title"],
            record["author"],
            record["call_number"],
            record["lccn"],
            record["isbn"]
        )
    
    # Add mock API data
    integration.engine.add_google_books_data("B000001", {
        "title": "Test Book 1: Enhanced",
        "author": "Author One Updated",
        "genres": ["Fiction", "Mystery"],
        "classification": "FIC",
        "publication_year": "2023"
    })
    
    integration.engine.add_vertex_ai_data("B000001", {
        "classification": "Mystery",
        "confidence": 0.88,
        "source_urls": ["https://example.com/book1"],
        "genres": ["Mystery", "Thriller"]
    })
    
    # Execute enrichment
    results = integration.engine.execute_enrichment()
    
    print("Mangle Enrichment Results:")
    for result in results:
        print(f"\nBarcode: {result['barcode']}")
        print(f"Title: {result['final_title']}")
        print(f"Author: {result['final_author']}")
        print(f"Classification: {result['final_classification']} (conf: {result.get('classification_confidence', 0)})")
        print(f"Genres: {result['genres']}")
    
    return results

if __name__ == "__main__":
    test_small_batch()