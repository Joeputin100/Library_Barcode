"""
Comprehensive test for the new Mangle-based enrichment architecture
"""

import json
import time
from deepquery_integration import DeepQueryIntegration

def test_comprehensive_architecture():
    """Test the complete new architecture"""
    print("🧪 Testing New Mangle-Based Enrichment Architecture\n")
    
    # Initialize the integration layer
    integration = DeepQueryIntegration()
    
    # Test data - sample MARC records
    test_records = [
        {
            "barcode": "B000001", 
            "title": "The Great Novel", 
            "author": "Jane Smith", 
            "call_number": "FIC SMI", 
            "lccn": "2023000001", 
            "isbn": "9781234567890"
        },
        {
            "barcode": "B000002", 
            "title": "Science Handbook", 
            "author": "John Doe", 
            "call_number": "500 DOE", 
            "lccn": "2023000002", 
            "isbn": "9780987654321"
        }
    ]
    
    # Add test records to engine
    for record in test_records:
        integration.engine.add_marc_record(
            record["barcode"],
            record["title"],
            record["author"],
            record["call_number"],
            record["lccn"],
            record["isbn"]
        )
    
    print("✅ Added test MARC records to knowledge base")
    
    # Add mock API responses (simulating real API calls)
    integration.engine.add_google_books_data("B000001", {
        "title": "The Great Novel: Special Edition",
        "author": "Jane A. Smith", 
        "genres": ["Fiction", "Literary"],
        "classification": "FIC",
        "series": "Great Works",
        "volume": "1",
        "publication_year": "2023",
        "description": "A masterpiece of modern literature"
    })
    
    integration.engine.add_vertex_ai_data("B000001", {
        "classification": "Literary Fiction",
        "confidence": 0.92,
        "source_urls": [
            "https://books.google.com/books?id=123",
            "https://www.loc.gov/item/2023000001/"
        ],
        "review_snippets": [
            "A profound exploration of human nature",
            "Smith's best work to date"
        ],
        "genres": ["Literary Fiction", "Drama"],
        "series_info": "Great Works Series",
        "publication_years": {"copyright": "2023", "original": "2022"}
    })
    
    integration.engine.add_google_books_data("B000002", {
        "title": "Science Handbook: Revised Edition",
        "author": "Dr. John Doe", 
        "genres": ["Science", "Reference"],
        "classification": "500",
        "publication_year": "2022",
        "description": "Comprehensive guide to modern science"
    })
    
    print("✅ Added mock API enrichment data")
    
    # Execute Mangle reasoning
    start_time = time.time()
    results = integration.engine.execute_enrichment()
    processing_time = time.time() - start_time
    
    print("✅ Executed Mangle reasoning engine")
    print(f"⏱️  Processing time: {processing_time:.3f} seconds")
    
    # Display results
    print("\n📊 Enrichment Results:")
    print("=" * 50)
    
    for result in results:
        print(f"\n📚 Book: {result['barcode']}")
        print(f"   Title: {result['final_title']}")
        print(f"   Author: {result['final_author']}")
        print(f"   Classification: {result.get('final_classification', 'N/A')}")
        if 'classification_confidence' in result:
            print(f"   Confidence: {result['classification_confidence']:.2f}")
        print(f"   Genres: {result.get('genres', [])}")
        
        # Show source provenance
        sources = []
        if result['final_title'] != test_records[0]['title']:
            sources.append("Google Books")
        if 'final_classification' in result:
            sources.append("Vertex AI")
        
        if sources:
            print(f"   Sources: {', '.join(sources)}")
    
    # Validate results
    print("\n✅ Validation Checks:")
    print("=" * 50)
    
    success_count = 0
    total_checks = 0
    
    # Check 1: Title enrichment worked
    for result in results:
        if result['barcode'] == 'B000001':
            total_checks += 1
            if result['final_title'] == 'The Great Novel: Special Edition':
                print("✅ Title enrichment: PASS")
                success_count += 1
            else:
                print("❌ Title enrichment: FAIL")
    
    # Check 2: Author enrichment worked  
    for result in results:
        if result['barcode'] == 'B000001':
            total_checks += 1
            if result['final_author'] == 'Jane A. Smith':
                print("✅ Author enrichment: PASS")
                success_count += 1
            else:
                print("❌ Author enrichment: FAIL")
    
    # Check 3: Classification with confidence
    for result in results:
        if result['barcode'] == 'B000001':
            total_checks += 1
            if result.get('final_classification') and result.get('classification_confidence', 0) > 0.7:
                print("✅ Classification with confidence: PASS")
                success_count += 1
            else:
                print("❌ Classification with confidence: FAIL")
    
    # Check 4: Genre aggregation
    for result in results:
        if result['barcode'] == 'B000001':
            total_checks += 1
            genres = result.get('genres', [])
            if len(genres) >= 2 and 'Literary Fiction' in genres:
                print("✅ Genre aggregation: PASS")
                success_count += 1
            else:
                print("❌ Genre aggregation: FAIL")
    
    # Summary
    success_rate = (success_count / total_checks) * 100 if total_checks > 0 else 0
    print(f"\n📈 Test Results: {success_count}/{total_checks} checks passed ({success_rate:.1f}%)")
    
    if success_rate >= 75:
        print("🎉 Architecture validation: SUCCESS")
    else:
        print("⚠️  Architecture validation: NEEDS IMPROVEMENT")
    
    return results, success_rate

def save_test_report(results, success_rate):
    """Save test results to report"""
    report = {
        "timestamp": time.strftime('%Y-%m-%d %H:%M:%S'),
        "success_rate": success_rate,
        "records_processed": len(results),
        "results": results
    }
    
    with open('architecture_test_report.json', 'w') as f:
        json.dump(report, f, indent=2)
    
    print(f"\n📋 Test report saved to 'architecture_test_report.json'")

if __name__ == "__main__":
    results, success_rate = test_comprehensive_architecture()
    save_test_report(results, success_rate)