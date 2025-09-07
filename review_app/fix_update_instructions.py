#!/usr/bin/env python3
"""
Fix records that have update instructions in their notes field instead of actual notes
"""

import sqlite3
import re

def fix_update_instructions():
    """Fix records with update instructions in notes field"""
    
    conn = sqlite3.connect('data/reviews.db')
    cursor = conn.cursor()
    
    # Get all records with update instructions in notes
    cursor.execute("""
        SELECT record_number, notes 
        FROM records 
        WHERE notes LIKE 'Update Notes%' 
           OR notes LIKE 'Update notes%'
           OR notes LIKE 'Update Note%'
    """)
    
    records = cursor.fetchall()
    print(f"Found {len(records)} records with update instructions in notes field")
    
    updated_count = 0
    
    for record_number, notes in records:
        try:
            # Extract the new notes value from the instruction
            if "Update Notes(500) field to" in notes:
                match = re.search(r'Update Notes\(500\) field to "([^"]+)"', notes)
                if match:
                    new_notes = match.group(1)
            elif "Update Notes to" in notes:
                match = re.search(r'Update Notes to "([^"]+)"', notes)
                if match:
                    new_notes = match.group(1)
            elif "Update notes to" in notes:
                match = re.search(r'Update notes to "([^"]+)"', notes)
                if match:
                    new_notes = match.group(1)
            else:
                print(f"❌ Record #{record_number}: Unknown instruction format: '{notes}'")
                continue
            
            # Update the record with the correct notes
            cursor.execute(
                "UPDATE records SET notes = ? WHERE record_number = ?",
                (new_notes, record_number)
            )
            
            if cursor.rowcount > 0:
                print(f"✅ Record #{record_number}: '{notes}' → '{new_notes}'")
                updated_count += 1
            
        except Exception as e:
            print(f"❌ Record #{record_number}: Error processing - {e}")
    
    conn.commit()
    conn.close()
    
    print(f"✅ Fixed {updated_count} records with update instructions in notes field")
    return updated_count

if __name__ == "__main__":
    fix_update_instructions()