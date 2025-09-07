#!/usr/bin/env python3
"""
Process remaining Needs Info records 1-100 that have update instructions in notes
"""

import sqlite3
import re

def process_remaining_needs_info():
    """Process remaining needs_info records with update instructions"""
    
    conn = sqlite3.connect('data/reviews.db')
    cursor = conn.cursor()
    
    # Get all needs_info records with update instructions
    cursor.execute("""
        SELECT record_number, title, notes 
        FROM records 
        WHERE record_number BETWEEN 1 AND 100 
        AND status = 'needs_info'
        AND notes LIKE 'Update%'
    """)
    
    needs_info_records = cursor.fetchall()
    print(f"Found {len(needs_info_records)} records with update instructions")
    
    updated_count = 0
    
    for record_number, title, notes in needs_info_records:
        try:
            # Parse the update instructions
            if "Update Notes to" in notes:
                # Extract the new notes value
                match = re.search(r'Update Notes to "([^"]+)"', notes)
                if match:
                    new_notes = match.group(1)
                    cursor.execute(
                        "UPDATE records SET notes = ? WHERE record_number = ?",
                        (new_notes, record_number)
                    )
                    print(f"✅ Record #{record_number}: Updated notes to '{new_notes}'")
                    updated_count += 1
            
            elif "Update author to" in notes:
                # Extract the new author value
                match = re.search(r'Update author to "([^"]+)"', notes)
                if match:
                    new_author = match.group(1)
                    cursor.execute(
                        "UPDATE records SET author = ? WHERE record_number = ?",
                        (new_author, record_number)
                    )
                    print(f"✅ Record #{record_number}: Updated author to '{new_author}'")
                    updated_count += 1
            
            elif "Update Call Number to" in notes:
                # Extract the new call number value
                match = re.search(r'Update Call Number to "([^"]+)"', notes)
                if match:
                    new_call_number = match.group(1)
                    cursor.execute(
                        "UPDATE records SET call_number = ? WHERE record_number = ?",
                        (new_call_number, record_number)
                    )
                    print(f"✅ Record #{record_number}: Updated call number to '{new_call_number}'")
                    updated_count += 1
            
            elif "Update Notes(500) TO" in notes:
                # Extract the new notes value
                match = re.search(r'Update Notes\(500\) TO "([^"]+)"', notes)
                if match:
                    new_notes = match.group(1)
                    cursor.execute(
                        "UPDATE records SET notes = ? WHERE record_number = ?",
                        (new_notes, record_number)
                    )
                    print(f"✅ Record #{record_number}: Updated notes to '{new_notes}'")
                    updated_count += 1
            
            # Reset status to NULL after processing
            cursor.execute(
                "UPDATE records SET status = NULL, reviewed_at = NULL WHERE record_number = ?",
                (record_number,)
            )
            
        except Exception as e:
            print(f"❌ Record #{record_number}: Error processing - {e}")
    
    conn.commit()
    conn.close()
    
    print(f"✅ Processed {updated_count} remaining needs_info records")
    return updated_count

if __name__ == "__main__":
    process_remaining_needs_info()