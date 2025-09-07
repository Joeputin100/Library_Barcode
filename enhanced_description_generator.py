#!/usr/bin/env python3
"""
Enhanced Description Generator
Uses DeepSeek API to create comprehensive book descriptions
by combining multiple fields from Mangle processing
"""
import json
import os
import time
import requests
from typing import Dict, List, Optional

# DeepSeek API configuration (from user's CLAUDE.md instructions)
DEEPSEEK_API_KEY = os.environ.get('DEEPSEEK_API_KEY')
DEEPSEEK_BASE_URL = "https://api.deepseek.com/v1"
DEEPSEEK_MODEL = "deepseek-chat"

def generate_enhanced_description(record: Dict) -> Optional[str]:
    """
    Generate enhanced description using DeepSeek API
    """
    if not DEEPSEEK_API_KEY:
        print("⚠️  DeepSeek API key not found. Set DEEPSEEK_API_KEY environment variable.")
        return None
    
    # Extract relevant fields
    title = record.get('final_title', '')
    author = record.get('final_author', '')
    classification = record.get('final_classification', '')
    subjects = record.get('final_subjects', '')
    series_name = record.get('final_series_name', '')
    series_volume = record.get('final_series_volume', '')
    publication_year = record.get('final_publication_year', '')
    publisher = record.get('final_publisher', '')
    awards = record.get('final_awards', '')
    existing_description = record.get('final_description', '')
    
    # Prepare prompt for the language model
    prompt = f"""
Create a comprehensive and engaging book description by combining information from multiple sources.

BOOK INFORMATION:
Title: {title}
Author: {author}
Publication Year: {publication_year}
Publisher: {publisher}
Classification: {classification}
Subjects/Genres: {subjects}
Series: {series_name} {series_volume if series_volume else ''}
Awards: {awards}
Existing Description: {existing_description}

INSTRUCTIONS:
1. Create a compelling 2-3 paragraph description that would appeal to library patrons
2. Incorporate all relevant genre/subject information naturally
3. Mention series information if applicable
4. Include awards or notable achievements if mentioned
5. Maintain a professional but engaging tone
6. If the existing description is useful, incorporate it but improve upon it
7. Focus on what makes this book interesting or valuable
8. Keep it concise but informative (150-250 words)

OUTPUT ONLY the enhanced description, no additional commentary.
"""
    
    try:
        response = requests.post(
            f"{DEEPSEEK_BASE_URL}/chat/completions",
            headers={
                "Authorization": f"Bearer {DEEPSEEK_API_KEY}",
                "Content-Type": "application/json"
            },
            json={
                "model": DEEPSEEK_MODEL,
                "messages": [
                    {"role": "system", "content": "You are a helpful assistant that creates compelling book descriptions for library catalogs."},
                    {"role": "user", "content": prompt}
                ],
                "temperature": 0.7,
                "max_tokens": 500
            },
            timeout=30
        )
        
        if response.status_code == 200:
            result = response.json()
            return result['choices'][0]['message']['content'].strip()
        else:
            print(f"API Error: {response.status_code} - {response.text}")
            return None
            
    except Exception as e:
        print(f"Error calling DeepSeek API: {e}")
        return None

def process_records_batch(records: List[Dict], batch_size: int = 10) -> List[Dict]:
    """
    Process records in batches with enhanced descriptions
    """
    processed_records = []
    
    for i, record in enumerate(records):
        print(f"Processing record {i+1}/{len(records)}: {record.get('final_title', 'Unknown')}")
        
        # Generate enhanced description
        enhanced_desc = generate_enhanced_description(record)
        
        if enhanced_desc:
            # Add enhanced description to record
            record['enhanced_description'] = enhanced_desc
            record['description_generation_timestamp'] = time.strftime('%Y-%m-%dT%H:%M:%SZ')
            record['description_source'] = "deepseek_enhanced"
        else:
            # Fallback to existing description
            record['enhanced_description'] = record.get('final_description', '')
            record['description_source'] = "original"
        
        processed_records.append(record)
        
        # Rate limiting
        if (i + 1) % batch_size == 0:
            print(f"Processed {i+1} records, pausing...")
            time.sleep(2)
    
    return processed_records

def main():
    """Main function to enhance descriptions"""
    print("🚀 Starting Enhanced Description Generation...")
    
    # Load Mangle processed results
    try:
        with open('mangle_processed_results.json', 'r') as f:
            data = json.load(f)
        records = data.get('results', [])
        print(f"Loaded {len(records)} Mangle-processed records")
    except Exception as e:
        print(f"Error loading Mangle results: {e}")
        return
    
    if not records:
        print("No records found to process")
        return
    
    # Process records with enhanced descriptions
    enhanced_records = process_records_batch(records)
    
    # Save results
    output_data = {
        "processed_timestamp": time.strftime('%Y-%m-%dT%H:%M:%SZ'),
        "total_records": len(enhanced_records),
        "records_with_enhanced_descriptions": sum(1 for r in enhanced_records if r.get('enhanced_description')),
        "results": enhanced_records
    }
    
    try:
        with open('enhanced_descriptions_results.json', 'w') as f:
            json.dump(output_data, f, indent=2)
        print(f"✅ Saved {len(enhanced_records)} enhanced records to enhanced_descriptions_results.json")
    except Exception as e:
        print(f"Error saving results: {e}")

if __name__ == "__main__":
    main()