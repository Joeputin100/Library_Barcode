#!/usr/bin/env python3
"""Test the library notes and call number enhancements"""

import sys
sys.path.append('review_app')

from comprehensive_review_app import generate_library_notes, generate_library_call_number

# Test record #1 data
record_1 = {
    'title': 'Treasures A Novel',
    'author': 'Plain, Belva',
    'genre': 'Ambition',
    'subjects': 'Ambition',
    'publication_date': '1992',
    'language': 'en'
}

# Test the enhancements
print("Testing library notes generation:")
notes = generate_library_notes(record_1)
print(f"Notes: {notes}")

print("\nTesting call number generation:")
call_number = generate_library_call_number(record_1)
print(f"Call Number: {call_number}")

# Test with different record
record_2 = {
    'title': 'Computer Programming Basics',
    'author': 'John Smith',
    'genre': 'Computers',
    'subjects': 'Programming, Technology',
    'publication_date': '2020',
    'language': 'en'
}

print("\nTesting non-fiction record:")
notes2 = generate_library_notes(record_2)
print(f"Notes: {notes2}")

call_number2 = generate_library_call_number(record_2)
print(f"Call Number: {call_number2}")