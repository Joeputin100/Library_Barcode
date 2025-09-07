#!/usr/bin/env python3
"""
MARC Pre-processor to clean data before Atriuum import
Fixes issues that Atriuum import rules cannot handle
"""

from pymarc import Record, Field, MARCReader
import re

def clean_marc_record(record):
    """Clean a MARC record for Atriuum import compatibility"""
    
    # Fix 260 field - remove trailing commas
    for field in record.get_fields('260'):
        for i, subfield in enumerate(field.subfields):
            if subfield in ['a', 'b'] and i + 1 < len(field.subfields):
                # Remove trailing commas from publisher and place
                if field.subfields[i + 1].endswith(','):
                    field.subfields[i + 1] = field.subfields[i + 1].rstrip(',')
    
    # Fix 260c - clean copyright date (remove 'c' and trailing period)
    for field in record.get_fields('260'):
        for i, subfield in enumerate(field.subfields):
            if subfield == 'c' and i + 1 < len(field.subfields):
                date_value = field.subfields[i + 1]
                # Remove 'c' prefix and trailing period
                cleaned_date = re.sub(r'^c|\.$', '', date_value)
                field.subfields[i + 1] = cleaned_date
    
    # Fix barcode in 001 field - add 'B' prefix if missing
    for field in record.get_fields('001'):
        if field.data and not field.data.startswith('B'):
            field.data = 'B' + field.data
    
    # Fix call number - add year from 260c to 852j if missing
    year_from_260 = None
    for field in record.get_fields('260'):
        for i, subfield in enumerate(field.subfields):
            if subfield == 'c' and i + 1 < len(field.subfields):
                year_match = re.search(r'\b(\d{4})\b', field.subfields[i + 1])
                if year_match:
                    year_from_260 = year_match.group(1)
    
    if year_from_260:
        for field in record.get_fields('852'):
            # Check if 852j (year) subfield exists
            has_year = any(subfield == 'j' for subfield in field.subfields)
            if not has_year:
                # Add year subfield
                field.subfields.extend(['j', year_from_260])
    
    return record

def process_marc_file(input_file, output_file):
    """Process a MARC file and create cleaned version"""
    
    with open(input_file, 'rb') as infile:
        reader = MARCReader(infile)
        records = []
        
        for record in reader:
            if record:
                cleaned_record = clean_marc_record(record)
                records.append(cleaned_record)
    
    # Write cleaned records to output file
    with open(output_file, 'wb') as outfile:
        for record in records:
            outfile.write(record.as_marc())
    
    print(f"✓ Processed {len(records)} records")
    print(f"✓ Output saved to {output_file}")

def main():
    # Process the simple test mapping file
    input_file = "simple_test_mapping.mrc"
    output_file = "cleaned_test_mapping.mrc"
    
    process_marc_file(input_file, output_file)
    
    print("\nCleaning operations performed:")
    print("- Removed trailing commas from 260a and 260b")
    print("- Cleaned copyright date in 260c (removed 'c' and periods)")
    print("- Added 'B' prefix to barcode in 001 field")
    print("- Added year to call number in 852j subfield")

if __name__ == "__main__":
    main()