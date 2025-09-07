#!/usr/bin/env python3
"""
Apply Needs Info instructions from records 1-50
"""

import sqlite3
import re

def apply_needs_info_updates():
    """Apply all Needs Info instructions"""
    
    conn = sqlite3.connect('data/reviews.db')
    cursor = conn.cursor()
    
    # Define the specific updates needed
    updates = [
        (3, "UPDATE records SET notes = 'NF Buddhism' WHERE record_number = 3"),
        (4, "UPDATE records SET notes = 'FIC Historical' WHERE record_number = 4"),
        (6, "UPDATE records SET notes = 'NF' WHERE record_number = 6"),
        (7, "UPDATE records SET notes = 'NF' WHERE record_number = 7"),
        (9, "UPDATE records SET notes = 'FIC Christian' WHERE record_number = 9"),
        (10, "UPDATE records SET notes = 'FIC Thriller Spanish' WHERE record_number = 10"),
        (11, "UPDATE records SET notes = 'NF Manga' WHERE record_number = 11"),
        (12, "UPDATE records SET notes = 'FIC Juvenile' WHERE record_number = 12"),
        (14, "UPDATE records SET notes = 'NF Programming' WHERE record_number = 14"),
        (16, "UPDATE records SET notes = 'NF Art' WHERE record_number = 16"),
        (17, "UPDATE records SET call_number = '681 SEI 2021' WHERE record_number = 17"),
        (18, "UPDATE records SET notes = 'NF Medical History' WHERE record_number = 18"),
        (19, "UPDATE records SET notes = 'NF Christianity' WHERE record_number = 19"),
        (20, "UPDATE records SET title = 'Thinking, Fast and Slow', notes = 'NF Psychology' WHERE record_number = 20"),
        (22, "UPDATE records SET notes = 'NF Programming' WHERE record_number = 22"),
        (23, "UPDATE records SET notes = 'NF Self-Help' WHERE record_number = 23"),
        (24, "UPDATE records SET notes = 'NF Calligraphy' WHERE record_number = 24"),
        (25, "UPDATE records SET notes = 'NF Business' WHERE record_number = 25"),
        (26, "UPDATE records SET notes = 'NF Cooking' WHERE record_number = 26"),
        (27, "UPDATE records SET notes = 'NF Christianity' WHERE record_number = 27"),
        (28, "UPDATE records SET title = 'Leadership in the Digital Age: The Power of Trust and Collaboration', author = 'Obeid, Elias T.', call_number = '658.4 OBE 2017', notes = 'NF Business' WHERE record_number = 28"),
        (29, "UPDATE records SET notes = 'FIC Christian' WHERE record_number = 29"),
        (31, "UPDATE records SET notes = 'NF Children' WHERE record_number = 31"),
        (32, "UPDATE records SET author = 'Frey, Kate and LeBuhn, Gretchen', call_number = '635 FRE 2022', notes = 'NF Gardening' WHERE record_number = 32"),
        (33, "UPDATE records SET notes = 'NF Business' WHERE record_number = 33"),
        (34, "UPDATE records SET notes = 'NF Philosophy' WHERE record_number = 34"),
        (35, "UPDATE records SET notes = 'FIC Fantasy' WHERE record_number = 35"),
        (39, "UPDATE records SET call_number = '641.59 AME 2006', notes = 'NF Cooking' WHERE record_number = 39"),
        (41, "UPDATE records SET notes = 'NF Christianity' WHERE record_number = 41"),
        (42, "UPDATE records SET title = 'Leading with Vision: The Leader''s Toolkit for Success', author = 'Naylor, John S', call_number = '658.4 NAY 2005' WHERE record_number = 42"),
        (46, "UPDATE records SET notes = 'NF Law' WHERE record_number = 46"),
        (47, "UPDATE records SET title = 'The Great Adventure of Faith' WHERE record_number = 47"),
        (49, "UPDATE records SET notes = 'NF Programming' WHERE record_number = 49"),
    ]
    
    updated_count = 0
    
    for record_number, update_query in updates:
        try:
            cursor.execute(update_query)
            print(f"✅ Record #{record_number}: Applied Needs Info update")
            updated_count += 1
        except Exception as e:
            print(f"❌ Record #{record_number}: Error applying update - {e}")
    
    # Reset status to NULL (unreviewed) for all these records
    cursor.execute("""
        UPDATE records SET status = NULL, reviewed_at = NULL 
        WHERE status = 'needs_info' AND record_number <= 50
    """)
    
    reset_count = cursor.rowcount
    print(f"✅ Reset {reset_count} records from 'needs_info' to unreviewed status")
    
    conn.commit()
    conn.close()
    
    print(f"✅ Applied {updated_count} Needs Info updates and reset {reset_count} records")
    return updated_count

if __name__ == "__main__":
    apply_needs_info_updates()