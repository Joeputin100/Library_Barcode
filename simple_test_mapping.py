#!/usr/bin/env python3
"""
Create a simpler test MARC record with basic field structure
"""

from pymarc import Record, Field

def create_simple_test_record():
    """Create a simple test record with basic fields"""
    record = Record()
    
    # Control Fields
    record.add_field(Field(tag="001", data="TEST001"))
    record.add_field(Field(tag="003", data="OCoLC"))
    record.add_field(Field(tag="005", data="20230101000000.0"))
    record.add_field(Field(tag="008", data="230101s2023    xxu           000 0 eng d"))
    
    # 020 - ISBN
    record.add_field(Field(
        tag="020",
        indicators=[" ", " "],
        subfields=["a", "9780123456789"]
    ))
    
    # 245 - Title Statement
    record.add_field(Field(
        tag="245",
        indicators=["1", "0"],
        subfields=["a", "Simple Test Record"]
    ))
    
    # 100 - Main Entry
    record.add_field(Field(
        tag="100",
        indicators=["1", " "],
        subfields=["a", "Test, Author"]
    ))
    
    # 260 - Publication Info (older but more compatible field)
    record.add_field(Field(
        tag="260",
        indicators=[" ", " "],
        subfields=["a", "Test Publisher,", "b", "Test City,", "c", "2023"]
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
        subfields=["a", "Test note"]
    ))
    
    # 520 - Summary
    record.add_field(Field(
        tag="520",
        indicators=[" ", " "],
        subfields=["a", "Test summary for simple record"]
    ))
    
    # 852 - Location
    record.add_field(Field(
        tag="852",
        indicators=["8", " "],
        subfields=["b", "s", "h", "025.3", "i", "TEST"]
    ))
    
    return record

def main():
    # Create the simple test record
    record = create_simple_test_record()
    
    # Write test record to file
    with open("simple_test_mapping.mrc", "wb") as out:
        out.write(record.as_marc())
    
    print("✓ Generated simple_test_mapping.mrc")
    print("\nThis record uses 260 field instead of 264 for better compatibility:")
    print("- 260 ‡a: 'Test Publisher,'")
    print("- 260 ‡b: 'Test City,'")
    print("- 260 ‡c: '2023'")

if __name__ == "__main__":
    main()