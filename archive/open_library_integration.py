"""
Open Library API Integration
"""
import requests
import json
import logging
from typing import Dict, Optional, List

logger = logging.getLogger(__name__)

class OpenLibraryAPI:
    BASE_URL = "https://openlibrary.org"
    COVERS_URL = "https://covers.openlibrary.org"
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'MangleEnrichment/1.0',
            'Accept': 'application/json'
        })
    
    def search_by_isbn(self, isbn: str) -> Optional[Dict]:
        """Search Open Library by ISBN"""
        if not isbn:
            return None
            
        try:
            # Clean ISBN
            clean_isbn = isbn.replace('-', '').replace(' ', '')
            
            url = f"{self.BASE_URL}/isbn/{clean_isbn}.json"
            
            response = self.session.get(url, timeout=10)
            
            if response.status_code == 404:
                return None
                
            response.raise_for_status()
            
            book_data = response.json()
            return self._parse_book_data(book_data, isbn)
            
        except Exception as e:
            logger.error(f"Open Library ISBN search failed for {isbn}: {e}")
            return None
    
    def search_by_title_author(self, title: str, author: str = "") -> Optional[Dict]:
        """Search by title and optionally author"""
        if not title:
            return None
            
        try:
            query = title
            if author:
                query += f" {author}"
            
            # URL encode the query
            import urllib.parse
            encoded_query = urllib.parse.quote(query)
            
            url = f"{self.BASE_URL}/search.json?title={encoded_query}"
            if author:
                url += f"&author={urllib.parse.quote(author)}"
            
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            
            search_results = response.json()
            
            if search_results.get('docs') and len(search_results['docs']) > 0:
                # Return the first result for now
                return self._parse_search_result(search_results['docs'][0])
            
            return None
            
        except Exception as e:
            logger.error(f"Open Library search failed for {title}/{author}: {e}")
            return None
    
    def get_book_cover(self, isbn: str, size: str = "M") -> Optional[str]:
        """Get book cover URL"""
        if not isbn:
            return None
            
        try:
            clean_isbn = isbn.replace('-', '').replace(' ', '')
            
            # Size can be S (small), M (medium), L (large)
            cover_url = f"{self.COVERS_URL}/b/isbn/{clean_isbn}-{size}.jpg"
            
            # Verify the cover exists
            response = self.session.head(cover_url, timeout=5)
            
            if response.status_code == 200:
                return cover_url
            
            return None
            
        except Exception as e:
            logger.error(f"Open Library cover fetch failed for {isbn}: {e}")
            return None
    
    def _parse_book_data(self, book_data: Dict, original_isbn: str) -> Optional[Dict]:
        """Parse Open Library book data"""
        try:
            # Extract basic information
            title = book_data.get('title', '')
            
            # Extract authors
            authors = []
            author_keys = book_data.get('authors', [])
            for author_key in author_keys:
                if isinstance(author_key, dict):
                    author_name = author_key.get('key', '').split('/')[-1].replace('_', ' ')
                    authors.append(author_name.title())
            
            author = ', '.join(authors) if authors else ""
            
            # Extract publication info
            publish_date = book_data.get('publish_date', '')
            publication_year = publish_date[:4] if publish_date and len(publish_date) >= 4 else ""
            
            publisher = book_data.get('publishers', [''])[0] if book_data.get('publishers') else ""
            
            # Extract identifiers
            isbn_10 = book_data.get('isbn_10', [''])[0] if book_data.get('isbn_10') else ""
            isbn_13 = book_data.get('isbn_13', [''])[0] if book_data.get('isbn_13') else ""
            
            # Use the best available ISBN
            best_isbn = isbn_13 or isbn_10 or original_isbn
            
            # Extract subjects and description
            subjects = book_data.get('subjects', [])
            description = book_data.get('description', '')
            if isinstance(description, dict):
                description = description.get('value', '')
            
            # Get cover URL
            cover_url = self.get_book_cover(best_isbn)
            
            return {
                'title': title,
                'author': author,
                'publication_year': publication_year,
                'isbn': best_isbn,
                'publisher': publisher,
                'subjects': subjects,
                'description': description,
                'cover_url': cover_url,
                'page_count': book_data.get('number_of_pages'),
                'source': 'OPEN_LIBRARY',
                'source_url': f"{self.BASE_URL}{book_data.get('key', '')}",
                'confidence': 0.9
            }
            
        except Exception as e:
            logger.error(f"Failed to parse Open Library data: {e}")
            return None
    
    def _parse_search_result(self, search_result: Dict) -> Optional[Dict]:
        """Parse search result"""
        try:
            title = search_result.get('title', '')
            author = search_result.get('author_name', [''])[0] if search_result.get('author_name') else ""
            
            isbn_list = search_result.get('isbn', [])
            isbn = isbn_list[0] if isbn_list else ""
            
            publication_year = search_result.get('first_publish_year', '')
            if publication_year:
                publication_year = str(publication_year)
            
            return {
                'title': title,
                'author': author,
                'publication_year': publication_year,
                'isbn': isbn,
                'publisher': search_result.get('publisher', [''])[0] if search_result.get('publisher') else "",
                'subjects': search_result.get('subject', []),
                'source': 'OPEN_LIBRARY',
                'source_url': f"{self.BASE_URL}{search_result.get('key', '')}",
                'confidence': 0.8
            }
            
        except Exception as e:
            logger.error(f"Failed to parse Open Library search result: {e}")
            return None

def get_open_library_data(marc_record: Dict) -> Optional[Dict]:
    """Get Open Library data for a MARC record"""
    api = OpenLibraryAPI()
    
    # Try ISBN search first (most reliable)
    if marc_record.get('isbn'):
        result = api.search_by_isbn(marc_record['isbn'])
        if result:
            return result
    
    # Fall back to title/author search
    if marc_record.get('title'):
        result = api.search_by_title_author(
            marc_record['title'], 
            marc_record.get('author', '')
        )
        if result:
            return result
    
    return None