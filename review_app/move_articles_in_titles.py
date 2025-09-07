#!/usr/bin/env python3
"""
Move articles (a, an, the) from beginning to end of titles
"""

import sqlite3
import re

def move_articles_in_titles():
    """Move articles from beginning to end of titles"""
    
    conn = sqlite3.connect('data/reviews.db')
    cursor = conn.cursor()
    
    # Get all records with titles
    cursor.execute("SELECT record_number, title FROM records WHERE title IS NOT NULL AND title != ''")
    records = cursor.fetchall()
    
    updated_count = 0
    articles = ['the', 'a', 'an']
    
    for record_number, title in records:
        if not title:
            continue
            
        # Check if title starts with an article
        title_lower = title.lower().strip()
        moved_article = None
        
        for article in articles:
            # Check if title starts with article followed by space
            if title_lower.startswith(article + ' '):
                # Remove the article from beginning
                remaining_title = title[len(article):].strip()
                # Move article to end with comma
                new_title = f"{remaining_title}, {article.capitalize()}"
                
                # Update the record
                cursor.execute(
                    "UPDATE records SET title = ? WHERE record_number = ?",
                    (new_title, record_number)
                )
                
                if cursor.rowcount > 0:
                    print(f"✅ Record #{record_number}: '{title}' → '{new_title}'")
                    updated_count += 1
                
                moved_article = article
                break
        
        # Also handle articles with quotes or parentheses
        if not moved_article:
            # Check for patterns like "A Title" or 'A Title'
            quote_patterns = [
                (r'^"([Aa]n?) ([^"]+)"$', lambda m: f'"{m.group(2)}, {m.group(1).capitalize()}"'),
                (r'^"([Tt]he) ([^"]+)"$', lambda m: f'"{m.group(2)}, {m.group(1).capitalize()}"'),
                (r"^'([Aa]n?) ([^']+)'$", lambda m: f"'{m.group(2)}, {m.group(1).capitalize()}'"),
                (r"^'([Tt]he) ([^']+)'$", lambda m: f"'{m.group(2)}, {m.group(1).capitalize()}'"),
            ]
            
            for pattern, replacement in quote_patterns:
                match = re.match(pattern, title)
                if match:
                    new_title = replacement(match)
                    cursor.execute(
                        "UPDATE records SET title = ? WHERE record_number = ?",
                        (new_title, record_number)
                    )
                    
                    if cursor.rowcount > 0:
                        print(f"✅ Record #{record_number}: '{title}' → '{new_title}'")
                        updated_count += 1
                    break
    
    conn.commit()
    conn.close()
    
    print(f"✅ Updated {updated_count} titles with articles moved to end")
    return updated_count

if __name__ == "__main__":
    move_articles_in_titles()