#!/usr/bin/env python3
"""
Resolve Call Number vs Notes vs Genre conflicts for records 101-808
"""

import sqlite3

def resolve_field_conflicts():
    """Resolve conflicts between Call Number, Notes, and Genre fields"""
    
    conn = sqlite3.connect('data/reviews.db')
    cursor = conn.cursor()
    
    # Rule 1: Call Number "FIC" but Notes "NF" → Update Notes to match Call Number
    cursor.execute("""
        UPDATE records 
        SET notes = 'FIC' 
        WHERE record_number BETWEEN 101 AND 808 
        AND call_number LIKE 'FIC%' 
        AND notes LIKE 'NF%'
    """)
    rule1_count = cursor.rowcount
    print(f"✅ Rule 1: Fixed {rule1_count} records where Call Number=FIC but Notes=NF")
    
    # Rule 2: Call Number not "FIC" but Notes "FIC" → Update Notes to "NF"
    cursor.execute("""
        UPDATE records 
        SET notes = 'NF' 
        WHERE record_number BETWEEN 101 AND 808 
        AND call_number NOT LIKE 'FIC%' 
        AND notes LIKE 'FIC%'
    """)
    rule2_count = cursor.rowcount
    print(f"✅ Rule 2: Fixed {rule2_count} records where Call Number≠FIC but Notes=FIC")
    
    # Rule 3: Use Genre to resolve ambiguous cases - Notes=FIC but Genre suggests NF
    cursor.execute("""
        UPDATE records 
        SET notes = 'NF' 
        WHERE record_number BETWEEN 101 AND 808 
        AND notes LIKE 'FIC%' 
        AND (genre NOT LIKE '%Fiction%' 
             AND genre NOT LIKE '%Novel%' 
             AND genre NOT LIKE '%Story%' 
             AND genre NOT LIKE '%Fantasy%' 
             AND genre NOT LIKE '%Mystery%' 
             AND genre NOT LIKE '%Romance%' 
             AND genre NOT LIKE '%Thriller%' 
             AND genre NOT LIKE '%Adventure%' 
             AND genre NOT LIKE '%Horror%')
    """)
    rule3_count = cursor.rowcount
    print(f"✅ Rule 3: Fixed {rule3_count} records where Notes=FIC but Genre suggests NF")
    
    # Rule 4: Use Genre to resolve ambiguous cases - Notes=NF but Genre suggests FIC
    cursor.execute("""
        UPDATE records 
        SET notes = 'FIC' 
        WHERE record_number BETWEEN 101 AND 808 
        AND notes LIKE 'NF%' 
        AND (genre LIKE '%Fiction%' 
             OR genre LIKE '%Novel%' 
             OR genre LIKE '%Story%' 
             OR genre LIKE '%Fantasy%' 
             OR genre LIKE '%Mystery%' 
             OR genre LIKE '%Romance%' 
             OR genre LIKE '%Thriller%' 
             OR genre LIKE '%Adventure%' 
             OR genre LIKE '%Horror%')
    """)
    rule4_count = cursor.rowcount
    print(f"✅ Rule 4: Fixed {rule4_count} records where Notes=NF but Genre suggests FIC")
    
    conn.commit()
    conn.close()
    
    total_fixed = rule1_count + rule2_count + rule3_count + rule4_count
    print(f"✅ Total conflicts resolved: {total_fixed} records")
    return total_fixed

if __name__ == "__main__":
    resolve_field_conflicts()