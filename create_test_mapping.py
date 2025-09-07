#!/usr/bin/env python3
"""
Create a test MARC record with clear field labels to diagnose Atriuum mapping
"""

from pymarc import Record, Field

def create_test_mapping_record():
    """Create a test record with clearly labeled fields"""
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
        subfields=["a", "Field Mapping Test"]
    ))
    
    # 100 - Main Entry - Personal Name
    record.add_field(Field(
        tag="100",
        indicators=["1", " "],
        subfields=["a", "Test, Author"]
    ))
    
    # 264 - Production, Publication, Distribution, Manufacture - CLEAR LABELS
    record.add_field(Field(
        tag="264",
        indicators=[" ", "1"],
        subfields=["a", "PUBLISHER-SHOULD-BE-HERE", "b", "PLACE-SHOULD-BE-HERE", "c", "2023"]
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
    
    # 520 - Summary
    record.add_field(Field(
        tag="520",
        indicators=[" ", " "],
        subfields=["a", "Test record to verify field mapping in Atriuum"]
    ))
    
    # 500 - Notes - SIMPLIFIED
    record.add_field(Field(
        tag="500",
        indicators=[" ", " "],
        subfields=["a", "Library Science"]
    ))
    
    # 852 - Location - CLEAR STRUCTURE
    record.add_field(Field(
        tag="852",
        indicators=["8", " "],
        subfields=["b", "s", "h", "025.3", "i", "TEST", "j", "2023", "p", "B000999"]
    ))
    
    # 090 - Local Call Number (LOC)
    record.add_field(Field(
        tag="090",
        indicators=[" ", " "],
        subfields=["a", "Z699.4.M37 T47 2023"]
    ))
    
    return record

def main():
    # Create the test record
    record = create_test_mapping_record()
    
    # Write test record to file
    with open("test_field_mapping.mrc", "wb") as out:
        out.write(record.as_marc())
    
    print("✓ Generated test_field_mapping.mrc")
    print("\nThis test record has clear labels:")
    print("- 264 ‡a: 'PUBLISHER-SHOULD-BE-HERE'")
    print("- 264 ‡b: 'PLACE-SHOULD-BE-HERE'")
    print("\nImport this with your minimal rules and see which field")
    print("shows which value in Atriuum's UI to diagnose the mapping.")

if __name__ == "__main__":
    main()