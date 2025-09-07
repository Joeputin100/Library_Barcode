#!/usr/bin/env python3
"""
Enhanced Description Generator (Simple Version)
Creates comprehensive book descriptions by combining multiple fields
without external API calls for testing
"""
import json
import time
from typing import Dict, List

def generate_enhanced_description(record: Dict) -> str:
    """
    Generate enhanced description by combining available fields
    """
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
    
    # Build enhanced description
    description_parts = []
    
    # Start with title and author
    if title and author:
        description_parts.append(f"{title} by {author}")
    
    # Add publication info
    if publication_year:
        description_parts.append(f"Published in {publication_year}")
    if publisher:
        description_parts.append(f"by {publisher}")
    
    # Add classification and subjects
    if classification:
        description_parts.append(f"Classification: {classification}")
    if subjects:
        # Clean up subjects (remove duplicates, normalize)
        subject_list = [s.strip() for s in subjects.split(',') if s.strip()]
        unique_subjects = list(dict.fromkeys(subject_list))
        if unique_subjects:
            description_parts.append(f"Genres: {', '.join(unique_subjects)}")
    
    # Add series information
    if series_name:
        series_info = f"Part of the {series_name} series"
        if series_volume and series_volume != publication_year:  # Avoid duplicate year
            series_info += f" (Volume {series_volume})"
        description_parts.append(series_info)
    
    # Add awards
    if awards:
        description_parts.append(f"Awards: {awards}")
    
    # Add existing description if available
    if existing_description and existing_description not in ['', publication_year]:
        description_parts.append(f"Description: {existing_description}")
    
    # Combine all parts
    enhanced_description = ". ".join(description_parts)
    
    # Ensure it ends with a period
    if enhanced_description and not enhanced_description.endswith('.'):
        enhanced_description += '.'
    
    return enhanced_description if enhanced_description.strip() else "Description not available."

def process_records_batch(records: List[Dict]) -> List[Dict]:
    """
    Process records with enhanced descriptions
    """
    processed_records = []
    
    for i, record in enumerate(records):
        if (i + 1) % 100 == 0:
            print(f"Processing record {i+1}/{len(records)}")
        
        # Generate enhanced description
        enhanced_desc = generate_enhanced_description(record)
        
        # Add enhanced description to record
        record['enhanced_description'] = enhanced_desc
        record['description_generation_timestamp'] = time.strftime('%Y-%m-%dT%H:%M:%SZ')
        record['description_source'] = "combined_fields"
        
        processed_records.append(record)
    
    return processed_records

def main():
    """Main function to enhance descriptions"""
    print("🚀 Starting Enhanced Description Generation (Simple Version)...")
    
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
        "records_with_enhanced_descriptions": len(enhanced_records),
        "results": enhanced_records
    }
    
    try:
        with open('enhanced_descriptions_results.json', 'w') as f:
            json.dump(output_data, f, indent=2)
        print(f"✅ Saved {len(enhanced_records)} enhanced records to enhanced_descriptions_results.json")
        
        # Show sample enhanced descriptions
        print("\n📋 Sample Enhanced Descriptions:")
        for i, record in enumerate(enhanced_records[:3]):
            print(f"\n{i+1}. {record.get('final_title', 'Unknown')}")
            print(f"   Enhanced Description: {record.get('enhanced_description', 'None')[:150]}...")
            
    except Exception as e:
        print(f"Error saving results: {e}")

if __name__ == "__main__":
    main()