#!/usr/bin/env python3
"""
Generate a comprehensive sample MARC record with full field mapping documentation
"""

from pymarc import Record, Field

def create_comprehensive_marc_record():
    """Create a fully described MARC record for testing"""
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
    
    # 264 - Production, Publication, Distribution, Manufacture
    record.add_field(Field(
        tag="264",
        indicators=[" ", "1"],
        subfields=["c", "2023"]
    ))
    
    # 300 - Physical Description
    record.add_field(Field(
        tag="300",
        indicators=[" ", " "],
        subfields=["a", "xv, 350 pages : illustrations ; 24 cm"]
    ))
    
    # 336 - Content Type
    record.add_field(Field(
        tag="336",
        indicators=[" ", " "],
        subfields=["a", "text", "b", "txt", "2", "rdacontent"]
    ))
    
    # 337 - Media Type
    record.add_field(Field(
        tag="337",
        indicators=[" ", " "],
        subfields=["a", "unmediated", "b", "n", "2", "rdamedia"]
    ))
    
    # 338 - Carrier Type
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
    
    # 520 - Summary, etc.
    record.add_field(Field(
        tag="520",
        indicators=[" ", " "],
        subfields=["a", "Comprehensive guide to MARC record enrichment using modern APIs and data validation techniques. Covers Google Books API, Library of Congress integration, data quality checks, and automated enrichment workflows for library systems."]
    ))
    
    # 500 - General Note
    record.add_field(Field(
        tag="500",
        indicators=[" ", " "],
        subfields=["a", "GENRE: Library science, Technical manual; LANGUAGE: English; MATERIAL: Standard print; INSURANCE_VALUE: $29.99; AWARD: Best Technical Publication 2023"]
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
    
    # 852 - Location - Your local holding information
    record.add_field(Field(
        tag="852",
        indicators=["8", " "],
        subfields=["b", "s", "h", "025.3 SMI", "p", "B000999"]
    ))
    
    # 090 - Local Call Number (LOC)
    record.add_field(Field(
        tag="090",
        indicators=[" ", " "],
        subfields=["a", "Z699.4.M37 S65 2023"]
    ))
    
    return record

def generate_field_mapping_documentation():
    """Generate comprehensive field mapping documentation"""
    documentation = """# MARC Field Mapping Documentation

## Control Fields
- **001**: Control Number (B000999) - Unique identifier for this record
- **003**: Control Number Identifier (OCoLC) - Organization that created the control number
- **005**: Date and Time of Latest Transaction (20230101000000.0) - Timestamp of last modification
- **008**: Fixed-Length Data Elements - General information about the record

## Data Fields
### 020 - International Standard Book Number
- **Subfield a**: ISBN (9780123456789)
- **Subfield c**: Terms of availability ($29.99)

### 100 - Main Entry - Personal Name
- **Subfield a**: Personal name (Smith, John) - Primary author

### 245 - Title Statement
- **Indicator 1**: Title added entry (1 = Added entry)
- **Indicator 0**: Nonfiling characters (0 = No nonfiling characters)
- **Subfield a**: Title (The Complete Guide to MARC Record Enrichment)

### 264 - Production, Publication, Distribution, Manufacture
- **Indicator 2**: Sequence of statements (1 = First)
- **Subfield c**: Date of publication, distribution, etc. (2023)

### 300 - Physical Description
- **Subfield a**: Extent (xv, 350 pages : illustrations ; 24 cm)

### 336-338 - RDA Content, Media, and Carrier Types
- **336**: Content type (text/txt - textual content)
- **337**: Media type (unmediated/n - direct perception)
- **338**: Carrier type (volume/nc - volume carrier)

### 490 - Series Statement
- **Indicator 1**: Series traced differently (1 = Traced differently)
- **Subfield a**: Series title (Library Technology Series)
- **Subfield v**: Volume number/sequential designation (45)

### 520 - Summary, etc.
- **Subfield a**: Summary (Comprehensive guide description)

### 500 - General Note
- **Subfield a**: General note (GENRE, LANGUAGE, MATERIAL, INSURANCE_VALUE, AWARD metadata)

### 650 - Subject Added Entry - Topical Term
- **Indicator 2**: Subject heading system/thesaurus (0 = Library of Congress Subject Headings)
- **Subfield a**: Topical term or geographic name entry element

### 852 - Location
- **Indicator 1**: Shelving scheme (8 = Other scheme)
- **Subfield b**: Shelving location (s = stacks)
- **Subfield h**: Classification part (025.3 SMI - local call number)
- **Subfield p**: Piece designation (B000999 - barcode)

### 090 - Local Call Number (LOC)
- **Subfield a**: Local call number (Z699.4.M37 S65 2023 - Library of Congress classification)

## Field Groupings
- **Bibliographic Description**: 020, 245, 264, 300
- **Physical Characteristics**: 336, 337, 338  
- **Intellectual Content**: 100, 490, 520, 650
- **Administrative**: 001, 003, 005, 008, 500
- **Holding Information**: 852, 090

## Data Sources Mapping
- **Google Books API**: Title, Author, ISBN, Publication Year, Physical Description
- **Library of Congress**: Subject Headings, Classification Number
- **Local System**: Barcode, Local Call Number, Location
- **Manual Enrichment**: Summary, Notes, Series Information
"""
    
    return documentation

def main():
    # Create the MARC record
    record = create_comprehensive_marc_record()
    
    # Write MARC record to file
    with open("comprehensive_marc_sample.mrc", "wb") as out:
        out.write(record.as_marc())
    
    # Generate and write documentation
    docs = generate_field_mapping_documentation()
    with open("marc_field_mapping_documentation.md", "w") as doc_file:
        doc_file.write(docs)
    
    print("✓ Generated comprehensive_marc_sample.mrc")
    print("✓ Generated marc_field_mapping_documentation.md")
    print("\nThe MARC record contains a complete bibliographic record with one holding.")
    print("Use this record to test import into Atriuum and compare field mappings.")

if __name__ == "__main__":
    main()