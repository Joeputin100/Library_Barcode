#!/usr/bin/env python3
"""
Filter MARC records to only include those with barcodes starting with 'B'
"""

import json

def filter_b_records():
    """Filter MARC records to only include barcodes starting with B"""
    
    # Load original MARC data
    with open("extracted_marc_data.json", "r") as f:
        marc_data = json.load(f)
    
    print(f"Original MARC records: {len(marc_data)}")
    
    # Filter to only records with barcodes starting with B
    b_records = [r for r in marc_data if r.get("barcode", "").startswith("B")]
    
    print(f"Records starting with B: {len(b_records)}")
    print(f"Records excluded: {len(marc_data) - len(b_records)}")
    
    # Save filtered data
    with open("extracted_marc_data_filtered.json", "w") as f:
        json.dump(b_records, f, indent=2)
    
    print("Filtered data saved to extracted_marc_data_filtered.json")
    
    return b_records

def update_migration_to_use_filtered():
    """Update production integration to use filtered data"""
    
    # Read the production integration file
    with open("production_integration.py", "r") as f:
        content = f.read()
    
    # Replace the file path to use filtered data
    new_content = content.replace(
        "extracted_marc_data.json", 
        "extracted_marc_data_filtered.json"
    )
    
    # Write the updated content
    with open("production_integration.py", "w") as f:
        f.write(new_content)
    
    print("Updated production_integration.py to use filtered data")

if __name__ == "__main__":
    filter_b_records()
    update_migration_to_use_filtered()