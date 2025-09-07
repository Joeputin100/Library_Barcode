#!/usr/bin/env python3
"""
Data Quality Validator for Book Enrichment Pipeline
Validates data consistency across multiple sources and flags conflicts
"""

import json
import logging
from typing import Dict, List, Set, Optional, Any
from dataclasses import dataclass
from enum import Enum

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ValidationLevel(Enum):
    INFO = "INFO"
    WARNING = "WARNING" 
    ERROR = "ERROR"
    CONFLICT = "CONFLICT"

@dataclass
class ValidationResult:
    barcode: str
    field: str
    level: ValidationLevel
    message: str
    source_values: Dict[str, Any]
    recommended_value: Optional[Any] = None

class DataQualityValidator:
    def __init__(self):
        self.validation_rules = self._load_validation_rules()
        
    def _load_validation_rules(self) -> Dict[str, Dict]:
        """Load validation rules for different field types"""
        return {
            'title': {
                'required': True,
                'max_length': 500,
                'sources': ['google_books', 'loc', 'original']
            },
            'author': {
                'required': True, 
                'max_length': 200,
                'sources': ['google_books', 'loc', 'original']
            },
            'isbn': {
                'required': False,
                'pattern': r'^[0-9X-]+$',
                'sources': ['google_books', 'loc', 'original']
            },
            'publication_year': {
                'required': False,
                'min_value': 1400,
                'max_value': 2025,
                'sources': ['google_books', 'loc', 'original']
            },
            'publisher': {
                'required': False,
                'max_length': 150,
                'sources': ['google_books', 'loc']
            },
            'page_count': {
                'required': False,
                'min_value': 1,
                'max_value': 5000,
                'sources': ['google_books']
            }
        }
    
    def validate_record(self, barcode: str, sources_data: Dict[str, Dict]) -> List[ValidationResult]:
        """Validate a single record across multiple data sources"""
        results = []
        
        # Check for missing required fields
        results.extend(self._validate_required_fields(barcode, sources_data))
        
        # Cross-reference field values across sources
        results.extend(self._cross_reference_fields(barcode, sources_data))
        
        # Validate field formats and constraints
        results.extend(self._validate_field_constraints(barcode, sources_data))
        
        return results
    
    def _validate_required_fields(self, barcode: str, sources_data: Dict[str, Dict]) -> List[ValidationResult]:
        """Check for missing required fields"""
        results = []
        
        for field, rules in self.validation_rules.items():
            if rules['required']:
                missing_in_all = True
                for source in rules['sources']:
                    if source in sources_data and sources_data[source].get(field):
                        missing_in_all = False
                        break
                
                if missing_in_all:
                    results.append(ValidationResult(
                        barcode=barcode,
                        field=field,
                        level=ValidationLevel.ERROR,
                        message=f"Required field '{field}' missing from all sources",
                        source_values={}
                    ))
        
        return results
    
    def _cross_reference_fields(self, barcode: str, sources_data: Dict[str, Dict]) -> List[ValidationResult]:
        """Cross-reference field values across different sources and flag conflicts"""
        results = []
        
        for field, rules in self.validation_rules.items():
            source_values = {}
            
            # Collect values from all sources
            for source in rules['sources']:
                if source in sources_data and sources_data[source].get(field):
                    source_values[source] = sources_data[source][field]
            
            # Check for conflicts if we have multiple values
            if len(source_values) > 1:
                unique_values = set(str(v).lower().strip() for v in source_values.values())
                
                if len(unique_values) > 1:
                    # Conflict detected
                    recommended = self._resolve_conflict(field, source_values)
                    
                    results.append(ValidationResult(
                        barcode=barcode,
                        field=field,
                        level=ValidationLevel.CONFLICT,
                        message=f"Conflicting values for '{field}' across sources",
                        source_values=source_values,
                        recommended_value=recommended
                    ))
                else:
                    # Values agree across sources
                    results.append(ValidationResult(
                        barcode=barcode,
                        field=field,
                        level=ValidationLevel.INFO,
                        message=f"Field '{field}' consistent across all sources",
                        source_values=source_values
                    ))
        
        return results
    
    def _resolve_conflict(self, field: str, source_values: Dict[str, Any]) -> Any:
        """Resolve conflicts between sources using priority rules"""
        # Priority order: google_books > loc > original
        priority_order = ['google_books', 'loc', 'original']
        
        for source in priority_order:
            if source in source_values:
                return source_values[source]
        
        # Fallback: return the first available value
        return next(iter(source_values.values()))
    
    def _validate_field_constraints(self, barcode: str, sources_data: Dict[str, Dict]) -> List[ValidationResult]:
        """Validate field-specific constraints"""
        results = []
        
        for field, rules in self.validation_rules.items():
            for source in rules['sources']:
                if source in sources_data and sources_data[source].get(field):
                    value = sources_data[source][field]
                    
                    # Length validation
                    if 'max_length' in rules and isinstance(value, str):
                        if len(value) > rules['max_length']:
                            results.append(ValidationResult(
                                barcode=barcode,
                                field=field,
                                level=ValidationLevel.WARNING,
                                message=f"Field '{field}' exceeds maximum length ({len(value)} > {rules['max_length']})",
                                source_values={source: value}
                            ))
                    
                    # Numeric range validation
                    if 'min_value' in rules and isinstance(value, (int, float)):
                        if value < rules['min_value']:
                            results.append(ValidationResult(
                                barcode=barcode,
                                field=field,
                                level=ValidationLevel.WARNING,
                                message=f"Field '{field}' below minimum value ({value} < {rules['min_value']})",
                                source_values={source: value}
                            ))
                    
                    if 'max_value' in rules and isinstance(value, (int, float)):
                        if value > rules['max_value']:
                            results.append(ValidationResult(
                                barcode=barcode,
                                field=field,
                                level=ValidationLevel.WARNING,
                                message=f"Field '{field}' above maximum value ({value} > {rules['max_value']})",
                                source_values={source: value}
                            ))
        
        return results
    
    def generate_validation_report(self, results: List[ValidationResult]) -> Dict:
        """Generate a comprehensive validation report"""
        report = {
            'summary': {
                'total_records': len(set(r.barcode for r in results)),
                'total_issues': len(results),
                'errors': sum(1 for r in results if r.level == ValidationLevel.ERROR),
                'warnings': sum(1 for r in results if r.level == ValidationLevel.WARNING),
                'conflicts': sum(1 for r in results if r.level == ValidationLevel.CONFLICT),
                'info': sum(1 for r in results if r.level == ValidationLevel.INFO)
            },
            'records': {}
        }
        
        for result in results:
            if result.barcode not in report['records']:
                report['records'][result.barcode] = []
            
            report['records'][result.barcode].append({
                'field': result.field,
                'level': result.level.value,
                'message': result.message,
                'source_values': result.source_values,
                'recommended_value': result.recommended_value
            })
        
        return report

# Example usage
def main():
    validator = DataQualityValidator()
    
    # Example data structure (would come from your actual sources)
    example_data = {
        "B000001": {
            "google_books": {
                "title": "Example Book Title",
                "author": "John Doe",
                "isbn": "978-0123456789",
                "publication_year": 2020
            },
            "loc": {
                "title": "Example Book Title",
                "author": "J. Doe",  # Slight variation
                "publisher": "Example Publisher"
            },
            "original": {
                "title": "Example Book",  # Different title
                "author": "John Doe"
            }
        }
    }
    
    # Validate each record
    all_results = []
    for barcode, sources in example_data.items():
        results = validator.validate_record(barcode, sources)
        all_results.extend(results)
    
    # Generate report
    report = validator.generate_validation_report(all_results)
    
    # Save report
    with open('data_quality_report.json', 'w') as f:
        json.dump(report, f, indent=2)
    
    logger.info(f"Validation complete. Issues found: {report['summary']['total_issues']}")

if __name__ == "__main__":
    main()