#!/usr/bin/env python3
"""
Batch enrichment processor to work towards 100% completion on all 8 sources
"""

import time
from multi_source_enricher import enrich_with_multiple_sources
from caching import load_cache, save_cache

def batch_enrich_books(book_list, batch_size=10, delay=1):
    """Process a batch of books with multi-source enrichment"""
    cache = load_cache()
    processed = 0
    
    print(f"🚀 Starting batch enrichment of {len(book_list)} books")
    print(f"📦 Batch size: {batch_size}, Delay: {delay}s between batches")
    print("=" * 60)
    
    for i, (title, author, isbn) in enumerate(book_list):
        try:
            # Enrich the book
            result = enrich_with_multiple_sources(title, author, isbn, "", cache)
            
            print(f"{i+1:3d}. {title[:30]:30} Quality: {result['quality_score']:.2f}")
            
            processed += 1
            
            # Save cache every batch
            if (i + 1) % batch_size == 0:
                save_cache(cache)
                print(f"💾 Saved cache after {i+1} records")
                time.sleep(delay)
                
        except Exception as e:
            print(f"❌ Error processing {title}: {e}")
    
    # Final save
    save_cache(cache)
    print(f"\n✅ Batch complete! Processed {processed} books")
    return processed

# Sample book list for enrichment
BOOKS_TO_ENRICH = [
    ("Moby Dick", "Herman Melville", ""),
    ("War and Peace", "Leo Tolstoy", ""),
    ("The Catcher in the Rye", "J.D. Salinger", ""),
    ("The Lord of the Rings", "J.R.R. Tolkien", ""),
    ("Pride and Prejudice", "Jane Austen", ""),
    ("The Odyssey", "Homer", ""),
    ("Crime and Punishment", "Fyodor Dostoevsky", ""),
    ("The Brothers Karamazov", "Fyodor Dostoevsky", ""),
    ("Anna Karenina", "Leo Tolstoy", ""),
    ("The Iliad", "Homer", ""),
    ("Brave New World", "Aldous Huxley", ""),
    ("The Picture of Dorian Gray", "Oscar Wilde", ""),
    ("Frankenstein", "Mary Shelley", ""),
    ("Dracula", "Bram Stoker", ""),
    ("The Scarlet Letter", "Nathaniel Hawthorne", ""),
    ("Wuthering Heights", "Emily Brontë", ""),
    ("Jane Eyre", "Charlotte Brontë", ""),
    ("Great Expectations", "Charles Dickens", ""),
    ("Les Misérables", "Victor Hugo", ""),
    ("The Count of Monte Cristo", "Alexandre Dumas", ""),
]

if __name__ == "__main__":
    batch_enrich_books(BOOKS_TO_ENRICH, batch_size=5, delay=2)