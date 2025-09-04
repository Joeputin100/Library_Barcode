#!/usr/bin/env python3
"""
Database migration script to add enhanced fields to existing records table
"""
import sqlite3
import os

def migrate_database():
    """Add enhanced fields to the records table"""
    db_path = 'data/reviews.db'
    
    if not os.path.exists(db_path):
        print("Database file not found. No migration needed.")
        return
    
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        
        # Check if enhanced columns already exist
        cursor.execute("PRAGMA table_info(records)")
        columns = [col[1] for col in cursor.fetchall()]
        
        # Columns to add for enhanced descriptions and MARC fields
        columns_to_add = [
            'enhanced_description TEXT',
            'description_source TEXT',
            'description_timestamp TEXT',
            # Essential MARC fields
            'publisher TEXT',
            'publication_date TEXT', 
            'physical_description TEXT',
            'subjects TEXT',
            'genre TEXT',
            'series TEXT',
            'series_volume TEXT',
            'language TEXT',
            'edition TEXT',
            'lccn TEXT',
            'dewey_decimal TEXT'
        ]
        
        added_columns = []
        for column_def in columns_to_add:
            column_name = column_def.split()[0]
            if column_name not in columns:
                try:
                    cursor.execute(f"ALTER TABLE records ADD COLUMN {column_def}")
                    added_columns.append(column_name)
                    print(f"Added column: {column_name}")
                except sqlite3.Error as e:
                    print(f"Error adding column {column_name}: {e}")
        
        if added_columns:
            print(f"Successfully added {len(added_columns)} columns: {', '.join(added_columns)}")
        else:
            print("All enhanced columns already exist. No migration needed.")

if __name__ == "__main__":
    migrate_database()