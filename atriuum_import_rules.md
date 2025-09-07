# Atriuum Custom MARC Import Rules

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
**Problem**: Call number should be "025.3\SMI\2023"
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
