#!/usr/bin/env python3
"""
Apply library notes and call number enhancements to all records in database
"""

import sqlite3
import sys
import os

# Add the review_app directory to path
sys.path.append('review_app')

# Import enhancement functions
try:
    from comprehensive_review_app import generate_library_notes, generate_library_call_number
    print("✅ Successfully imported enhancement functions")
except ImportError as e:
    print(f"❌ Error importing functions: {e}")
    sys.exit(1)

def apply_enhancements_to_database():
    """Apply library notes and call number enhancements to all records"""
    
    db_path = 'review_app/data/reviews.db'
    
    if not os.path.exists(db_path):
        print(f"❌ Database not found: {db_path}")
        return
    
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row  # Enable column access by name
    cursor = conn.cursor()
    
    # Get all records
    cursor.execute('SELECT * FROM records')
    records = cursor.fetchall()
    
    print(f"Found {len(records)} records to enhance")
    
    updated_count = 0
    
    for record in records:
        record_dict = dict(record)
        
        # Generate enhancements
        library_notes = generate_library_notes(record_dict)
        call_number = generate_library_call_number(record_dict)
        
        # Check if updates are needed
        needs_update = False
        update_fields = {}
        
        if library_notes and (not record_dict.get('notes') or record_dict.get('notes') != library_notes):
            update_fields['notes'] = library_notes
            needs_update = True
        
        if call_number and (not record_dict.get('call_number') or record_dict.get('call_number') != call_number):
            update_fields['call_number'] = call_number
            needs_update = True
        
        if needs_update:
            # Build SQL update
            set_clauses = []
            params = []
            
            for field, value in update_fields.items():
                set_clauses.append(f"{field} = ?")
                params.append(value)
            
            params.append(record_dict['id'])
            
            cursor.execute(f'''
                UPDATE records SET {', '.join(set_clauses)}
                WHERE id = ?
            ''', params)
            
            updated_count += 1
            
            if updated_count % 50 == 0:
                print(f"Enhanced {updated_count} records...")
    
    # Commit changes
    conn.commit()
    conn.close()
    
    print(f"✅ Enhanced {updated_count} records with library notes and call numbers")
    return updated_count

def test_enhancements():
    """Test enhancements on a few sample records"""
    
    db_path = 'review_app/data/reviews.db'
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    # Test record #1
    cursor.execute('SELECT * FROM records WHERE record_number = 1')
    record_1 = cursor.fetchone()
    
    if record_1:
        record_dict = dict(record_1)
        print(f"\nTesting Record #1: {record_dict['title']}")
        
        notes = generate_library_notes(record_dict)
        call_number = generate_library_call_number(record_dict)
        
        print(f"Generated Notes: {notes}")
        print(f"Generated Call Number: {call_number}")
        print(f"Current Notes: {record_dict.get('notes', 'None')}")
        print(f"Current Call Number: {record_dict.get('call_number', 'None')}")
    
    conn.close()

if __name__ == "__main__":
    print("Applying library enhancements to database...")
    
    # First test enhancements
    test_enhancements()
    
    # Then apply to all records
    updated = apply_enhancements_to_database()
    
    print(f"\n🎉 Enhancement process complete! {updated} records updated.")