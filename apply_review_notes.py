#!/usr/bin/env python3
"""
Apply specific changes from review notes for records 1-7
"""

import sqlite3
import re

def apply_review_notes():
    """Apply the specific changes from review notes"""
    
    conn = sqlite3.connect('review_app/data/reviews.db')
    cursor = conn.cursor()
    
    # Record 2: Correct author name and add ISBN
    cursor.execute("""
        UPDATE records SET 
            author = 'Plain, Belva',
            isbn = '9781473627598',
            notes = 'NF'
        WHERE record_number = 2
    """)
    
    # Record 3: Remove qualifying statement from description
    cursor.execute("SELECT description FROM records WHERE record_number = 3")
    result = cursor.fetchone()
    if result:
        description = result[0]
        # Remove the specific qualifying statement
        new_description = description.replace(
            "Formal critical reviews from major literary outlets are rare for books of this nature, which are primarily distributed as free Dharma teachings by a monastic institution. However, ",
            ""
        )
        cursor.execute("UPDATE records SET description = ?, isbn = '9781870205443' WHERE record_number = 3", (new_description,))
    
    # Record 4: Add ISBN
    cursor.execute("UPDATE records SET isbn = '9780679603054' WHERE record_number = 4")
    
    # Record 5: Add ISBN
    cursor.execute("UPDATE records SET isbn = '9781735187518' WHERE record_number = 5")
    
    # Record 6: Fix call number (will be handled by the enhanced call number generation)
    # Record 7: Fix call number (will be handled by the enhanced call number generation)
    
    conn.commit()
    conn.close()
    
    print("Applied specific review note changes to records 1-7")

def regenerate_call_numbers():
    """Regenerate call numbers for all records using the improved algorithm"""
    
    # Import the enhanced function
    import sys
    sys.path.append('/data/data/com.termux/files/home/projects/barcode/review_app')
    from comprehensive_review_app import generate_library_call_number
    
    conn = sqlite3.connect('review_app/data/reviews.db')
    cursor = conn.cursor()
    
    # Get all records
    cursor.execute("SELECT id, record_number, title, author, genre, subjects, dewey_decimal, publication_date FROM records")
    records = cursor.fetchall()
    
    for record_id, record_number, title, author, genre, subjects, dewey_decimal, publication_date in records:
        record_dict = {
            'title': title,
            'author': author,
            'genre': genre,
            'subjects': subjects,
            'dewey_decimal': dewey_decimal,
            'publication_date': publication_date
        }
        
        new_call_number = generate_library_call_number(record_dict)
        
        cursor.execute("UPDATE records SET call_number = ? WHERE id = ?", (new_call_number, record_id))
        
        if record_number <= 7:
            print(f"Record #{record_number}: {new_call_number}")
    
    conn.commit()
    conn.close()
    
    print("Regenerated call numbers for all records")

if __name__ == "__main__":
    apply_review_notes()
    regenerate_call_numbers()