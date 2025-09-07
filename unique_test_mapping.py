#!/usr/bin/env python3
"""
Create a unique test MARC record with distinctive values
"""

from pymarc import Record, Field

def create_unique_test_record():
    """Create a unique test record with very distinctive values"""
    record = Record()
    
    # Control Fields - UNIQUE BARCODE
    record.add_field(Field(tag="001", data="UNIQUE001"))
    record.add_field(Field(tag="003", data="OCoLC"))
    record.add_field(Field(tag="005", data="20230101000000.0"))
    record.add_field(Field(tag="008", data="230101s2023    xxu           000 0 eng d"))
    
    # 020 - ISBN
    record.add_field(Field(
        tag="020",
        indicators=[" ", " "],
        subfields=["a", "9780123456789"]
    ))
    
    # 245 - Title Statement - UNIQUE
    record.add_field(Field(
        tag="245",
        indicators=["1", "0"],
        subfields=["a", "UNIQUE TEST RECORD"]
    ))
    
    # 100 - Main Entry - UNIQUE
    record.add_field(Field(
        tag="100",
        indicators=["1", " "],
        subfields=["a", "Unique, Test Author"]
    ))
    
    # 260 - Publication Info - CLEANED VALUES
    record.add_field(Field(
        tag="260",
        indicators=[" ", " "],
        subfields=["a", "Unique Publisher", "b", "Unique City", "c", "2023"]
    ))
    
    # 300 - Physical Description
    record.add_field(Field(
        tag="300",
        indicators=[" ", " "],
        subfields=["a", "350 pages"]
    ))
    
    # 500 - Notes
    record.add_field(Field(
        tag="500",
        indicators=[" ", " "],
        subfields=["a", "Unique test note"]
    ))
    
    # 520 - Summary
    record.add_field(Field(
        tag="520",
        indicators=[" ", " "],
        subfields=["a", "Unique test summary for verification"]
    ))
    
    # 852 - Location - WITH YEAR
    record.add_field(Field(
        tag="852",
        indicators=["8", " "],
        subfields=["b", "s", "h", "025.3", "i", "UNIQUE", "j", "2023"]
    ))
    
    return record

def main():
    # Create the unique test record
    record = create_unique_test_record()
    
    # Write test record to file
    with open("unique_test_mapping.mrc", "wb") as out:
        out.write(record.as_marc())
    
    print("✓ Generated unique_test_mapping.mrc")
    print("\nThis record has unique values for easy verification:")
    print("- Barcode: UNIQUE001")
    print("- Title: UNIQUE TEST RECORD") 
    print("- Author: Unique, Test Author")
    print("- Publisher: Unique Publisher (no comma)")
    print("- Place: Unique City (no comma)")
    print("- Call Number: 025.3 UNIQUE 2023")

if __name__ == "__main__":
    main()