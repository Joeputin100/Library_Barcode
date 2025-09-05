#!/usr/bin/env python3
"""
Vertex AI Grounded Deep Research System
Provides comprehensive, attributed research for bibliographic records
"""

import json
import re
import time
from datetime import datetime
from caching import load_cache, save_cache

# Import existing Vertex AI function
from api_calls import get_vertex_ai_classification_batch

def create_grounded_research_prompt(record_data):
    """Create a comprehensive grounded research prompt with proper attribution"""
    
    title = record_data.get('title', '')
    author = record_data.get('author', '')
    isbn = record_data.get('isbn', '')
    publisher = record_data.get('publisher', '')
    publication_date = record_data.get('publication_date', '')
    description = record_data.get('description', '')
    
    prompt = f"""
Perform GROUNDED DEEP RESEARCH for this book with proper attribution to reliable sources.

BOOK INFORMATION:
Title: {title}
Author: {author}
ISBN: {isbn}
Publisher: {publisher}
Publication Date: {publication_date}
Description: {description[:500]}{'...' if description and len(description) > 500 else ''}

RESEARCH TASKS:
1. VERIFICATION: Verify the bibliographic details against authoritative sources
2. ENRICHMENT: Provide missing information with source attribution
3. CLASSIFICATION: Genre, Dewey Decimal, and subject classification
4. CONTEXT: Historical significance, critical reception, and cultural impact
5. EDITIONS: Information about different editions and translations
6. PRICING: Current market value and availability information

SOURCE REQUIREMENTS:
- Always attribute information to specific reliable sources
- Prefer library databases, academic sources, and verified book databases
- Include URLs or source identifiers when available
- Rate source reliability (High/Medium/Low)

OUTPUT FORMAT (JSON):
{{
  "verified_data": {{
    "title": "verified title with source",
    "author": "verified author with source", 
    "publisher": "verified publisher with source",
    "publication_date": "verified date with source",
    "edition": "edition information with source",
    "language": "language with source"
  }},
  "enriched_data": {{
    "genres": ["genre1 with source", "genre2 with source"],
    "subjects": ["subject1 with source", "subject2 with source"],
    "dewey_decimal": "classification with source",
    "lccn": "LCCN with source if available",
    "physical_description": "complete physical description with source",
    "series_info": "series information with source",
    "awards": ["award1 with source", "award2 with source"]
  }},
  "contextual_data": {{
    "critical_reception": "reception analysis with sources",
    "historical_significance": "significance analysis with sources", 
    "cultural_impact": "impact analysis with sources",
    "similar_works": ["similar book1 with source", "similar book2 with source"]
  }},
  "market_data": {{
    "current_value": "market value with source",
    "availability": "availability information with source",
    "editions": ["edition1 details with source", "edition2 details with source"]
  }},
  "source_attributions": [
    {{"information": "specific fact", "source": "source name", "reliability": "High/Medium/Low", "url": "source url if available"}}
  ],
  "research_quality": {{
    "completeness_score": 0-10,
    "verification_score": 0-10,
    "source_reliability": "Overall reliability assessment"
  }}
}}

GROUNDING INSTRUCTIONS:
- Base all information on verifiable sources, not general knowledge
- If information cannot be verified, state "Unable to verify"
- Prioritize library catalogs, ISBN databases, and academic sources
- Include specific source details for each piece of information
"""
    
    return prompt.strip()

def perform_grounded_research(record_data, cache):
    """Perform grounded deep research for a single record"""
    
    # Create unique cache key
    cache_key = f"vertex_grounded_{record_data.get('isbn', '')}_{record_data.get('title', '')}_{record_data.get('author', '')}".lower()
    
    # Check cache first
    if cache_key in cache:
        print(f"Using cached grounded research for {record_data.get('title', 'Unknown')}")
        return cache[cache_key], True
    
    try:
        # Create research prompt
        research_prompt = create_grounded_research_prompt(record_data)
        
        # Initialize Vertex AI (using existing infrastructure)
        import google.auth
        import vertexai
        from vertexai.generative_models import GenerativeModel
        
        credentials, project_id = google.auth.default()
        vertexai.init(project=project_id, credentials=credentials, location="us-central1")
        model = GenerativeModel("gemini-2.5-flash")
        
        print(f"Performing grounded research for: {record_data.get('title', 'Unknown')}")
        
        # Execute research with retries
        retry_delays = [10, 20, 30]
        for i in range(len(retry_delays) + 1):
            try:
                response = model.generate_content(research_prompt)
                response_text = response.text.strip()
                
                # Clean response (handle JSON code blocks)
                if response_text.startswith("```json") and response_text.endswith("```"):
                    response_text = response_text[7:-3].strip()
                elif response_text.startswith("```") and response_text.endswith("```"):
                    response_text = response_text[3:-3].strip()
                
                # Parse JSON response
                research_results = json.loads(response_text)
                
                # Save to cache
                cache[cache_key] = research_results
                save_cache(cache)
                
                print(f"✅ Grounded research completed for {record_data.get('title', 'Unknown')}")
                return research_results, False
                
            except json.JSONDecodeError as e:
                print(f"JSON parse error: {e}")
                print(f"Response text: {response_text[:200]}...")
                if i < len(retry_delays):
                    time.sleep(retry_delays[i])
                else:
                    return {"error": "Failed to parse research results"}, False
                    
            except Exception as e:
                print(f"Research error: {e}")
                if i < len(retry_delays):
                    time.sleep(retry_delays[i])
                else:
                    return {"error": str(e)}, False
    
    except Exception as e:
        print(f"Failed to perform grounded research: {e}")
        return {"error": str(e)}, False

def apply_research_to_record(record_id, research_results, db_conn):
    """Apply research findings to database record"""
    
    cursor = db_conn.cursor()
    updates = []
    params = []
    
    # Extract verified data
    verified = research_results.get('verified_data', {})
    if verified.get('title'):
        updates.append("title = ?")
        params.append(verified['title'])
    if verified.get('author'):
        updates.append("author = ?")
        params.append(verified['author'])
    if verified.get('publisher'):
        updates.append("publisher = ?")
        params.append(verified['publisher'])
    if verified.get('publication_date'):
        updates.append("publication_date = ?")
        params.append(verified['publication_date'])
    if verified.get('edition'):
        updates.append("edition = ?")
        params.append(verified['edition'])
    if verified.get('language'):
        updates.append("language = ?")
        params.append(verified['language'])
    
    # Extract enriched data
    enriched = research_results.get('enriched_data', {})
    if enriched.get('dewey_decimal'):
        updates.append("dewey_decimal = ?")
        params.append(enriched['dewey_decimal'])
    if enriched.get('lccn'):
        updates.append("lccn = ?")
        params.append(enriched['lccn'])
    if enriched.get('physical_description'):
        updates.append("physical_description = ?")
        params.append(enriched['physical_description'])
    if enriched.get('series_info'):
        updates.append("series = ?")
        params.append(enriched['series_info'])
    
    # Handle genres and subjects
    if enriched.get('genres'):
        genres = ", ".join([g.split(' with source')[0] for g in enriched['genres'] if ' with source' in g])
        updates.append("genre = ?")
        params.append(genres)
    if enriched.get('subjects'):
        subjects = ", ".join([s.split(' with source')[0] for s in enriched['subjects'] if ' with source' in s])
        updates.append("subjects = ?")
        params.append(subjects)
    
    # Store research data in enhanced_description field instead of research_data
    research_json = json.dumps(research_results)
    updates.append("enhanced_description = ?")
    params.append(f"VERTEX AI RESEARCH: {research_json}")
    
    if updates:
        params.append(record_id)
        cursor.execute(f"""
            UPDATE records SET {', '.join(updates)}
            WHERE id = ?
        """, params)
        
        db_conn.commit()
        return len(updates)
    
    return 0

def test_grounded_research():
    """Test grounded research on sample records"""
    
    # Load cache
    cache = load_cache()
    
    # Connect to database
    import sqlite3
    conn = sqlite3.connect('review_app/data/reviews.db')
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    # Test on records that have been enriched with Google Books
    test_records = [1, 2, 3, 22]  # Records with complete Google Books data
    
    print("🧪 Testing Vertex Grounded Deep Research")
    print("=" * 50)
    
    for record_number in test_records:
        cursor.execute('''
            SELECT * FROM records WHERE record_number = ?
        ''', (record_number,))
        
        record = cursor.fetchone()
        if not record:
            print(f"Record {record_number} not found")
            continue
        
        record_data = dict(record)
        print(f"\n--- Researching Record #{record_number}: {record_data.get('title', 'Unknown')} ---")
        
        # Perform grounded research
        research_results, cached = perform_grounded_research(record_data, cache)
        
        if 'error' in research_results:
            print(f"❌ Research failed: {research_results['error']}")
            continue
        
        # Apply research to database
        updates_applied = apply_research_to_record(record_data['id'], research_results, conn)
        
        print(f"✅ Research completed ({'cached' if cached else 'new'})")
        print(f"📊 Updates applied: {updates_applied} fields")
        
        # Show sample of research results
        if 'verified_data' in research_results:
            verified = research_results['verified_data']
            print(f"Verified: {verified.get('title', 'No title')} by {verified.get('author', 'Unknown')}")
        
        if 'enriched_data' in research_results:
            enriched = research_results['enriched_data']
            if enriched.get('dewey_decimal'):
                print(f"Classification: {enriched['dewey_decimal']}")
        
        # Brief delay between researches
        time.sleep(2)
    
    conn.close()
    print(f"\n🎉 Grounded research test completed!")

if __name__ == "__main__":
    test_grounded_research()