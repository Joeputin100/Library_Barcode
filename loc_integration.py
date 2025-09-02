"""
Library of Congress API Integration
"""
import requests
import json
import logging
from typing import Dict, Optional

logger = logging.getLogger(__name__)

class LibraryOfCongressAPI:
    BASE_URL = "http://lx2.loc.gov:210/LCDB"
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'MangleEnrichment/1.0',
            'Accept': 'application/json'
        })
    
    def search_by_isbn(self, isbn: str) -> Optional[Dict]:
        """Search Library of Congress by ISBN using SRU interface"""
        if not isbn:
            return None
            
        try:
            # Clean ISBN (remove dashes, spaces)
            clean_isbn = isbn.replace('-', '').replace(' ', '')
            
            # SRU query format
            query = f'bath.isbn="{clean_isbn}"'
            params = {
                "version": "1.1",
                "operation": "searchRetrieve",
                "query": query,
                "maximumRecords": "1",
                "recordSchema": "marcxml",
            }
            
            response = self.session.get(self.BASE_URL, params=params, timeout=15)
            response.raise_for_status()
            
            return self._parse_sru_response(response.content)
            
        except Exception as e:
            logger.error(f"LOC ISBN search failed for {isbn}: {e}")
            return None
    
    def search_by_lccn(self, lccn: str) -> Optional[Dict]:
        """Search Library of Congress by LCCN using SRU interface"""
        if not lccn:
            return None
            
        try:
            # Clean LCCN
            clean_lccn = lccn.strip().upper()
            
            # SRU query format
            query = f'bath.lccn="{clean_lccn}"'
            params = {
                "version": "1.1",
                "operation": "searchRetrieve",
                "query": query,
                "maximumRecords": "1",
                "recordSchema": "marcxml",
            }
            
            response = self.session.get(self.BASE_URL, params=params, timeout=15)
            response.raise_for_status()
            
            return self._parse_sru_response(response.content)
            
        except Exception as e:
            logger.error(f"LOC LCCN search failed for {lccn}: {e}")
            return None
    
    def search_by_title_author(self, title: str, author: str = "") -> Optional[Dict]:
        """Search by title and optionally author using SRU interface"""
        if not title:
            return None
            
        try:
            # Clean title and author
            safe_title = title.replace('"', '').replace("'", "")
            safe_author = author.replace('"', '').replace("'", "") if author else ""
            
            # SRU query format
            if safe_author:
                query = f'bath.title="{safe_title}" and bath.author="{safe_author}"'
            else:
                query = f'bath.title="{safe_title}"'
            
            params = {
                "version": "1.1",
                "operation": "searchRetrieve",
                "query": query,
                "maximumRecords": "1",
                "recordSchema": "marcxml",
            }
            
            response = self.session.get(self.BASE_URL, params=params, timeout=15)
            response.raise_for_status()
            
            return self._parse_sru_response(response.content)
            
        except Exception as e:
            logger.error(f"LOC title/author search failed for {title}/{author}: {e}")
            return None
    
    def _parse_sru_response(self, xml_content: bytes) -> Optional[Dict]:
        """Parse SRU XML response into standardized format"""
        try:
            import xml.etree.ElementTree as ET
            
            # Parse XML
            root = ET.fromstring(xml_content)
            
            # Check for errors
            ns_diag = {"diag": "http://www.loc.gov/zing/srw/diagnostic/"}
            error_message = root.find(".//diag:message", ns_diag)
            if error_message is not None:
                logger.warning(f"LOC SRU error: {error_message.text}")
                return None
            
            # Extract MARC data
            ns_marc = {"marc": "http://www.loc.gov/MARC21/slim"}
            
            # Extract title
            title_node = root.find('.//marc:datafield[@tag="245"]/marc:subfield[@code="a"]', ns_marc)
            title = title_node.text.strip() if title_node is not None and title_node.text else ""
            
            # Extract author
            author_node = root.find('.//marc:datafield[@tag="100"]/marc:subfield[@code="a"]', ns_marc)
            author = author_node.text.strip() if author_node is not None and author_node.text else ""
            
            # Extract classification
            classification_node = root.find('.//marc:datafield[@tag="082"]/marc:subfield[@code="a"]', ns_marc)
            classification = classification_node.text.strip() if classification_node is not None and classification_node.text else ""
            
            # Extract publication year
            pub_year_node = root.find('.//marc:datafield[@tag="264"]/marc:subfield[@code="c"]', ns_marc)
            if pub_year_node is None:
                pub_year_node = root.find('.//marc:datafield[@tag="260"]/marc:subfield[@code="c"]', ns_marc)
            publication_year = ""
            if pub_year_node is not None and pub_year_node.text:
                import re
                years = re.findall(r"(1[7-9]\d{2}|20\d{2})", pub_year_node.text)
                if years:
                    publication_year = str(min([int(y) for y in years]))
            
            # Extract subjects
            subjects = []
            genre_nodes = root.findall('.//marc:datafield[@tag="655"]/marc:subfield[@code="a"]', ns_marc)
            for genre_node in genre_nodes:
                if genre_node.text:
                    subjects.append(genre_node.text.strip().rstrip("."))
            
            # Extract ISBN
            isbn_nodes = root.findall('.//marc:datafield[@tag="020"]/marc:subfield[@code="a"]', ns_marc)
            isbn = isbn_nodes[0].text.strip() if isbn_nodes and isbn_nodes[0].text else ""
            
            # Extract LCCN
            lccn_nodes = root.findall('.//marc:datafield[@tag="010"]/marc:subfield[@code="a"]', ns_marc)
            lccn = lccn_nodes[0].text.strip() if lccn_nodes and lccn_nodes[0].text else ""
            
            # Extract publisher
            publisher_node = root.find('.//marc:datafield[@tag="264"]/marc:subfield[@code="b"]', ns_marc)
            if publisher_node is None:
                publisher_node = root.find('.//marc:datafield[@tag="260"]/marc:subfield[@code="b"]', ns_marc)
            publisher = publisher_node.text.strip() if publisher_node is not None and publisher_node.text else ""
            
            if not title:  # If no data found, return None
                return None
            
            return {
                'title': title,
                'author': author,
                'publication_year': publication_year,
                'isbn': isbn,
                'lccn': lccn,
                'subjects': subjects,
                'classification': classification,
                'publisher': publisher,
                'description': "",
                'source': 'LIBRARY_OF_CONGRESS',
                'source_url': "",
                'confidence': 0.95  # High confidence for LOC data
            }
            
        except Exception as e:
            logger.error(f"Failed to parse SRU response: {e}")
            return None
    
    def _extract_classification(self, subjects: list) -> str:
        """Extract classification from subjects"""
        if not subjects:
            return ""
        
        # Look for common classification patterns
        for subject in subjects:
            if isinstance(subject, str):
                subject_lower = subject.lower()
                
                # Dewey Decimal patterns
                if any(keyword in subject_lower for keyword in ['dewey', 'ddc']):
                    # Extract numbers like "823.912"
                    import re
                    dewey_match = re.search(r'\b\d{3}\.?\d*\b', subject)
                    if dewey_match:
                        return f"DDC:{dewey_match.group()}"
                
                # LC Classification patterns
                elif any(keyword in subject_lower for keyword in ['lc classification', 'library of congress']):
                    # Extract patterns like "PS3557.I58"
                    lc_match = re.search(r'\b[A-Z]{1,3}\d+(\.\w+)*\b', subject)
                    if lc_match:
                        return f"LC:{lc_match.group()}"
                
                # General fiction/non-fiction
                elif 'fiction' in subject_lower:
                    return "FIC"
                elif 'nonfiction' in subject_lower or 'non-fiction' in subject_lower:
                    return "NF"
        
        return ""

def get_loc_data(marc_record: Dict) -> Optional[Dict]:
    """Get Library of Congress data for a MARC record"""
    api = LibraryOfCongressAPI()
    
    # Try different search strategies in order of reliability
    if marc_record.get('lccn'):
        result = api.search_by_lccn(marc_record['lccn'])
        if result:
            return result
    
    if marc_record.get('isbn'):
        result = api.search_by_isbn(marc_record['isbn'])
        if result:
            return result
    
    if marc_record.get('title'):
        result = api.search_by_title_author(
            marc_record['title'], 
            marc_record.get('author', '')
        )
        if result:
            return result
    
    return None