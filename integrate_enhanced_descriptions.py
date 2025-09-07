#!/usr/bin/env python3
"""
Integrate Enhanced Descriptions into Main Dataset
Merges enhanced descriptions from Mangle processing into the main enriched data
"""
import json
from typing import Dict, List

def integrate_enhanced_descriptions():
    """
    Integrate enhanced descriptions into the main enriched_data_full.json
    """
    print("🔗 Integrating enhanced descriptions into main dataset...")
    
    # Load enhanced descriptions
    try:
        with open('enhanced_descriptions_results.json', 'r') as f:
            enhanced_data = json.load(f)
        enhanced_records = enhanced_data.get('results', [])
        print(f"Loaded {len(enhanced_records)} enhanced records")
    except Exception as e:
        print(f"Error loading enhanced descriptions: {e}")
        return
    
    # Load main enriched data
    try:
        with open('enriched_data_full.json', 'r') as f:
            main_data = json.load(f)
        main_records = main_data.get('enriched_records', [])
        print(f"Loaded {len(main_records)} main records")
    except Exception as e:
        print(f"Error loading main enriched data: {e}")
        return
    
    # Create mapping from enhanced records by barcode
    enhanced_map = {}
    for enhanced_record in enhanced_records:
        barcode = enhanced_record.get('barcode')
        if barcode:
            enhanced_map[barcode] = {
                'enhanced_description': enhanced_record.get('enhanced_description', ''),
                'description_source': enhanced_record.get('description_source', ''),
                'description_timestamp': enhanced_record.get('description_generation_timestamp', '')
            }
    
    print(f"Created mapping for {len(enhanced_map)} barcodes")
    
    # Integrate enhanced descriptions into main records
    updated_count = 0
    for main_record in main_records:
        barcode = main_record.get('barcode')
        
        if barcode and barcode in enhanced_map:
            enhanced_info = enhanced_map[barcode]
            main_record['enhanced_description'] = enhanced_info['enhanced_description']
            main_record['description_source'] = enhanced_info['description_source']
            main_record['description_generation_timestamp'] = enhanced_info['description_timestamp']
            updated_count += 1
    
    print(f"Updated {updated_count} records with enhanced descriptions")
    
    # Save integrated data
    integrated_data = {
        "integration_timestamp": "2025-09-04T03:30:00Z",
        "total_records": len(main_records),
        "records_with_enhanced_descriptions": updated_count,
        "enriched_records": main_records
    }
    
    try:
        with open('enriched_data_with_enhanced_descriptions.json', 'w') as f:
            json.dump(integrated_data, f, indent=2)
        print("✅ Saved integrated data to enriched_data_with_enhanced_descriptions.json")
        
        # Show statistics
        print(f"\n📊 Integration Statistics:")
        print(f"Total records: {len(main_records)}")
        print(f"Records with enhanced descriptions: {updated_count}")
        print(f"Coverage: {(updated_count/len(main_records)*100):.1f}%")
        
        # Show sample integrated records
        print(f"\n📋 Sample Integrated Records:")
        sample_count = 0
        for record in main_records:
            if 'enhanced_description' in record and sample_count < 3:
                print(f"\n{sample_count + 1}. {record.get('title', 'Unknown')}")
                print(f"   Author: {record.get('author', 'Unknown')}")
                print(f"   Enhanced Description: {record.get('enhanced_description', 'None')[:120]}...")
                sample_count += 1
                
    except Exception as e:
        print(f"Error saving integrated data: {e}")

def main():
    """Main integration function"""
    integrate_enhanced_descriptions()

if __name__ == "__main__":
    main()