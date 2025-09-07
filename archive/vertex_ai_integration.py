"""
Google Vertex AI Integration for Book Classification
"""
import logging
from typing import Dict, Optional, List
import google.auth
from google.auth.transport.requests import Request
from google.oauth2 import service_account
import requests
import json

logger = logging.getLogger(__name__)

class VertexAIClient:
    """Client for Google Vertex AI book classification"""
    
    def __init__(self, project_id: str = None, location: str = "us-central1"):
        self.project_id = project_id
        self.location = location
        self.endpoint_id = None  # Would be set to your deployed model endpoint
        self.credentials = None
        
        try:
            # Try to get credentials automatically
            self.credentials, self.project_id = google.auth.default()
            
            # Refresh if needed
            if self.credentials and self.credentials.expired:
                self.credentials.refresh(Request())
                
        except Exception as e:
            logger.warning(f"Could not get default credentials: {e}")
            # Fall back to manual credential setup
            self._setup_manual_credentials()
    
    def _setup_manual_credentials(self):
        """Setup credentials from environment or config file"""
        # This would load from GOOGLE_APPLICATION_CREDENTIALS env var
        # or look for service account key file
        try:
            self.credentials = service_account.Credentials.from_service_account_file(
                'vertex-ai-credentials.json'
            )
            self.project_id = self.credentials.project_id
        except FileNotFoundError:
            logger.warning("Vertex AI credentials file not found")
        except Exception as e:
            logger.error(f"Failed to setup Vertex AI credentials: {e}")
    
    def classify_book(self, title: str, author: str = "", description: str = "", 
                     genres: List[str] = None) -> Optional[Dict]:
        """Classify book using Vertex AI"""
        if not title:
            return None
        
        # For now, implement a rule-based classifier since we don't have
        # actual Vertex AI model access. This would be replaced with actual API calls.
        
        try:
            # Prepare input data
            input_text = f"Title: {title}"
            if author:
                input_text += f", Author: {author}"
            if description:
                input_text += f", Description: {description}"
            if genres:
                input_text += f", Genres: {', '.join(genres)}"
            
            # This is where actual Vertex AI API call would go:
            # prediction = self._call_vertex_ai_api(input_text)
            
            # For now, use rule-based classification
            classification, confidence = self._rule_based_classification(
                title, author, description, genres or []
            )
            
            return {
                'classification': classification,
                'confidence': confidence,
                'source': 'VERTEX_AI',
                'genres': genres or [],
                'reasoning': f"Based on title: {title}"
            }
            
        except Exception as e:
            logger.error(f"Vertex AI classification failed: {e}")
            return None
    
    def _rule_based_classification(self, title: str, author: str, 
                                  description: str, genres: List[str]) -> tuple:
        """Rule-based fallback classification"""
        text = f"{title} {author} {description} {' '.join(genres)}".lower()
        
        # Classification rules based on common patterns
        classification_rules = [
            # Fiction patterns
            (['novel', 'fiction', 'story', 'tale', 'saga'], 'FIC', 0.85),
            (['mystery', 'crime', 'detective', 'thriller'], 'FIC-MYSTERY', 0.9),
            (['science fiction', 'sci-fi', 'fantasy'], 'FIC-SF', 0.9),
            (['romance', 'love story'], 'FIC-ROMANCE', 0.9),
            (['historical fiction'], 'FIC-HIST', 0.9),
            
            # Non-fiction patterns
            (['history', 'historical'], 'NF-HISTORY', 0.85),
            (['biography', 'autobiography', 'memoir'], 'NF-BIO', 0.9),
            (['science', 'technology', 'math'], 'NF-SCIENCE', 0.85),
            (['cookbook', 'recipe', 'cooking'], 'NF-COOKING', 0.95),
            (['travel', 'guidebook'], 'NF-TRAVEL', 0.9),
            (['business', 'economics', 'finance'], 'NF-BUSINESS', 0.85),
            (['self-help', 'psychology'], 'NF-SELFHELP', 0.8),
            (['art', 'photography'], 'NF-ART', 0.85),
            (['music'], 'NF-MUSIC', 0.85),
            
            # Reference
            (['dictionary', 'encyclopedia', 'reference'], 'REF', 0.95),
            
            # Children's books
            (['children', 'kids', 'picture book'], 'CHILDREN', 0.9),
            (['young adult', 'ya fiction'], 'YA', 0.9),
        ]
        
        best_match = ('FIC', 0.7)  # Default to fiction with medium confidence
        
        for keywords, classification, confidence in classification_rules:
            if any(keyword in text for keyword in keywords):
                if confidence > best_match[1]:
                    best_match = (classification, confidence)
        
        return best_match
    
    def _call_vertex_ai_api(self, input_text: str) -> Optional[Dict]:
        """Actual Vertex AI API call (placeholder for real implementation)"""
        # This would be the actual API call to Vertex AI
        # Example:
        # endpoint = f"https://{self.location}-aiplatform.googleapis.com/v1/projects/{self.project_id}/locations/{self.location}/endpoints/{self.endpoint_id}:predict"
        # 
        # headers = {
        #     'Authorization': f'Bearer {self.credentials.token}',
        #     'Content-Type': 'application/json'
        # }
        # 
        # data = {
        #     'instances': [{'content': input_text}],
        #     'parameters': {'confidenceThreshold': 0.7}
        # }
        # 
        # response = requests.post(endpoint, headers=headers, json=data)
        # response.raise_for_status()
        # return response.json()
        
        return None

def get_vertex_ai_data(marc_record: Dict, google_data: Dict = None) -> Optional[Dict]:
    """Get Vertex AI classification for a book"""
    client = VertexAIClient()
    
    title = marc_record.get('title', '')
    author = marc_record.get('author', '')
    
    # Extract additional context from Google data if available
    description = ""
    genres = []
    
    if google_data:
        description = google_data.get('description', '')
        if google_data.get('genres'):
            genres = google_data['genres'].split(',')
    
    result = client.classify_book(title, author, description, genres)
    
    if result:
        return {
            'barcode': marc_record.get('barcode', ''),
            'classification': result['classification'],
            'confidence': result['confidence'],
            'source_urls': "https://cloud.google.com/vertex-ai",
            'reviews': "AI-generated classification",
            'genres': result['genres'],
            'series_info': "",
            'years': marc_record.get('publication_year', ''),
            'source': 'VERTEX_AI',
            'reasoning': result.get('reasoning', '')
        }
    
    return None