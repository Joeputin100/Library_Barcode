#!/usr/bin/env python3
"""
Generate a fixed MARC record with all the corrections needed for proper Atriuum import
"""

from pymarc import Record, Field

def create_fixed_marc_record():
    """Create a corrected MARC record for Atriuum import"""
    record = Record()
    
    # Control Fields
    record.add_field(Field(tag="001", data="B000999"))
    record.add_field(Field(tag="003", data="OCoLC"))
    record.add_field(Field(tag="005", data="20230101000000.0"))
    record.add_field(Field(tag="008", data="230101s2023    xxu           000 0 eng d"))
    
    # 020 - ISBN with Price
    record.add_field(Field(
        tag="020",
        indicators=[" ", " "],
        subfields=["a", "9780123456789", "c", "$29.99"]
    ))
    
    # 245 - Title Statement
    record.add_field(Field(
        tag="245",
        indicators=["1", "0"],
        subfields=["a", "The Complete Guide to MARC Record Enrichment"]
    ))
    
    # 100 - Main Entry - Personal Name
    record.add_field(Field(
        tag="100",
        indicators=["1", " "],
        subfields=["a", "Smith, John"]
    ))
    
    # 264 - Production, Publication, Distribution, Manufacture - FIXED
    record.add_field(Field(
        tag="264",
        indicators=[" ", "1"],
        subfields=["a", "Technical Publishing Press", "b", "New York", "c", "2023"]
    ))
    
    # 010 - LCCN - ADDED
    record.add_field(Field(
        tag="010",
        indicators=[" ", " "],
        subfields=["a", "2023456789"]
    ))
    
    # 300 - Physical Description
    record.add_field(Field(
        tag="300",
        indicators=[" ", " "],
        subfields=["a", "xv, 350 pages : illustrations ; 24 cm"]
    ))
    
    # 336-338 - RDA Types
    record.add_field(Field(
        tag="336",
        indicators=[" ", " "],
        subfields=["a", "text", "b", "txt", "2", "rdacontent"]
    ))
    record.add_field(Field(
        tag="337",
        indicators=[" ", " "],
        subfields=["a", "unmediated", "b", "n", "2", "rdamedia"]
    ))
    record.add_field(Field(
        tag="338",
        indicators=[" ", " "],
        subfields=["a", "volume", "b", "nc", "2", "rdacarrier"]
    ))
    
    # 490 - Series Statement
    record.add_field(Field(
        tag="490",
        indicators=["1", " "],
        subfields=["a", "Library Technology Series", "v", "45"]
    ))
    
    # 520 - Summary, etc. - FIXED (combined with notes)
    record.add_field(Field(
        tag="520",
        indicators=[" ", " "],
        subfields=["a", "Comprehensive guide to MARC record enrichment using modern APIs and data validation techniques. Covers Google Books API, Library of Congress integration, data quality checks, and automated enrichment workflows for library systems. GENRE: Library science; LANGUAGE: English; MATERIAL: Standard print; INSURANCE_VALUE: $29.99; AWARD: Best Technical Publication 2023"]
    ))
    
    # 500 - General Note - SIMPLIFIED
    record.add_field(Field(
        tag="500",
        indicators=[" ", " "],
        subfields=["a", "Library Science"]
    ))
    
    # 650 - Subject Added Entry - Topical Term
    subjects = [
        "MARC formats",
        "Cataloging -- Data processing", 
        "Metadata enrichment",
        "Library automation",
        "Application program interfaces (Computer software)"
    ]
    
    for subject in subjects:
        record.add_field(Field(
            tag="650",
            indicators=[" ", "0"],
            subfields=["a", subject]
        ))
    
    # 852 - Location - FIXED call number structure
    record.add_field(Field(
        tag="852",
        indicators=["8", " "],
        subfields=["b", "s", "h", "025.3", "i", "SMI", "j", "2023", "p", "B000999"]
    ))
    
    # 090 - Local Call Number (LOC)
    record.add_field(Field(
        tag="090",
        indicators=[" ", " "],
        subfields=["a", "Z699.4.M37 S65 2023"]
    ))
    
    return record

def generate_import_rules():
    """Generate custom import rules for Atriuum"""
    rules = """# Atriuum Custom MARC Import Rules

## Required Fixes for Your Import:

### 1. Publisher Field Mapping
**Problem**: Publisher missing from 264 field
**Solution**: Ensure 264 ‡a maps to Publisher field in Atriuum

### 2. Date Cleaning Rules
**Problem**: "c2023." and "2023." need cleaning to "2023"
**Solution**: Create data cleaning rule to remove "c" prefix and trailing periods

### 3. LCCN Field
**Problem**: LCCN field (010) was missing
**Solution**: Added 010 ‡a with sample LCCN: 2023456789

### 4. Notes Field Simplification
**Problem**: Complex notes field with multiple metadata
**Solution**: 
- Simplified 500 ‡a to just "Library Science" 
- Moved other metadata to 520 summary field

### 5. Call Number Structure
**Problem**: Call number should be "025.3\\SMI\\2023"
**Solution**: Used 852 subfields:
- ‡h = "025.3" (classification)
- ‡i = "SMI" (cutter)  
- ‡j = "2023" (date)

### 6. Cost Field Mapping
**Problem**: Price not mapping from 020 ‡c
**Solution**: Ensure 020 ‡c maps to Cost field in holdings

## Recommended Atriuum Import Rule Changes:

1. **Bibliographic Field Mapping**:
   - 264 ‡a → Publisher
   - 264 ‡b → Publication Place  
   - 264 ‡c → Copyright Date (with cleaning rule)
   - 010 ‡a → LCCN
   - 500 ‡a → Genre/Notes (simple values only)

2. **Holdings Field Mapping**:
   - 020 ‡c → Cost
   - 852 ‡h → Classification part of call number
   - 852 ‡i → Cutter part of call number
   - 852 ‡j → Date part of call number
   - 852 ‡p → Barcode

3. **Data Cleaning Rules**:
   - Remove "c" prefix from dates
   - Remove trailing periods from dates
   - Trim whitespace from all fields
"""
    
    return rules

def main():
    # Create the fixed MARC record
    record = create_fixed_marc_record()
    
    # Write fixed MARC record to file
    with open("fixed_marc_sample.mrc", "wb") as out:
        out.write(record.as_marc())
    
    # Generate and write import rules
    rules = generate_import_rules()
    with open("atriuum_import_rules.md", "w") as rules_file:
        rules_file.write(rules)
    
    print("✓ Generated fixed_marc_sample.mrc with all corrections")
    print("✓ Generated atriuum_import_rules.md with custom import rules")
    print("\nThe fixed record includes:")
    print("- Added publisher (264 ‡a)")
    print("- Added LCCN (010 ‡a)") 
    print("- Fixed call number structure (852 ‡h, ‡i, ‡j)")
    print("- Simplified notes field (500 ‡a)")
    print("- Combined metadata into summary (520 ‡a)")

if __name__ == "__main__":
    main()