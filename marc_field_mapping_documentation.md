# MARC Field Mapping Documentation

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
