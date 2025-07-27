# pii_classifier.py
"""
PII Classification and Inventory Management for Atlan S3 Connector
Provides advanced PII detection, CIA rating application, and inventory reporting
"""

import logging
import json
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
from enum import Enum

from pyatlan.model.assets import Asset, Table, Column, S3Object
from pyatlan.model.enums import AtlanTagColor
from pyatlan.client.atlan import AtlanClient

logger = logging.getLogger(__name__)

class ConfidentialityLevel(Enum):
    """Confidentiality levels for CIA ratings"""
    LOW = "Low"
    MEDIUM = "Medium"
    HIGH = "High"
    RESTRICTED = "Restricted"

class IntegrityLevel(Enum):
    """Integrity levels for CIA ratings"""
    LOW = "Low"
    MEDIUM = "Medium"
    HIGH = "High"

class AvailabilityLevel(Enum):
    """Availability levels for CIA ratings"""
    LOW = "Low"
    MEDIUM = "Medium"
    HIGH = "High"
    CRITICAL = "Critical"

@dataclass
class CIARating:
    """CIA (Confidentiality, Integrity, Availability) rating for an asset"""
    confidentiality: ConfidentialityLevel
    integrity: IntegrityLevel
    availability: AvailabilityLevel

@dataclass
class PIIClassification:
    """PII classification details for an asset"""
    has_pii: bool
    pii_types: List[str]
    sensitivity_level: str
    confidence: float
    cia_rating: CIARating
    sensitive_columns: List[str] = None

class PIIClassifier:
    """
    Handles PII classification and inventory management across the data pipeline
    """
    
    def __init__(self, atlan_client: AtlanClient):
        self.atlan_client = atlan_client
        
        # PII type patterns for rule-based detection
        self.pii_patterns = {
            "email": ["email", "mail", "e-mail", "e_mail"],
            "phone": ["phone", "tel", "mobile", "cell", "contact"],
            "name": ["name", "first", "last", "full", "customer_name", "client_name"],
            "address": ["address", "street", "city", "zip", "postal", "location"],
            "id": ["id", "ssn", "social", "passport", "license", "identification"],
            "financial": ["account", "card", "credit", "bank", "routing", "payment"],
            "health": ["health", "medical", "insurance", "diagnosis", "treatment"],
            "biometric": ["biometric", "fingerprint", "facial", "retina", "dna"]
        }
        
        # CIA rating defaults based on PII types
        self.default_cia_ratings = {
            "email": CIARating(
                confidentiality=ConfidentialityLevel.MEDIUM,
                integrity=IntegrityLevel.MEDIUM,
                availability=AvailabilityLevel.LOW
            ),
            "phone": CIARating(
                confidentiality=ConfidentialityLevel.MEDIUM,
                integrity=IntegrityLevel.MEDIUM,
                availability=AvailabilityLevel.LOW
            ),
            "name": CIARating(
                confidentiality=ConfidentialityLevel.MEDIUM,
                integrity=IntegrityLevel.MEDIUM,
                availability=AvailabilityLevel.LOW
            ),
            "address": CIARating(
                confidentiality=ConfidentialityLevel.MEDIUM,
                integrity=IntegrityLevel.MEDIUM,
                availability=AvailabilityLevel.LOW
            ),
            "id": CIARating(
                confidentiality=ConfidentialityLevel.HIGH,
                integrity=IntegrityLevel.HIGH,
                availability=AvailabilityLevel.MEDIUM
            ),
            "financial": CIARating(
                confidentiality=ConfidentialityLevel.HIGH,
                integrity=IntegrityLevel.HIGH,
                availability=AvailabilityLevel.MEDIUM
            ),
            "health": CIARating(
                confidentiality=ConfidentialityLevel.RESTRICTED,
                integrity=IntegrityLevel.HIGH,
                availability=AvailabilityLevel.HIGH
            ),
            "biometric": CIARating(
                confidentiality=ConfidentialityLevel.RESTRICTED,
                integrity=IntegrityLevel.HIGH,
                availability=AvailabilityLevel.MEDIUM
            )
        }
        
        # Compliance tags mapping
        self.compliance_tags = {
            "singapore_pdpa": "Singapore Personal Data Protection Act",
            "indonesia_pp71": "Indonesia PP No. 71/2019",
            "gdpr_equivalent": "GDPR Equivalent Protection",
            "financial_sensitive": "Financial Sensitive Data",
            "customer_data": "Customer Data Protection Required",
            "hr_restricted": "HR Data - Restricted Access",
            "transaction_audit": "Transaction Data - Audit Required"
        }
    
    def detect_pii_rule_based(self, asset_info: Dict[str, Any]) -> PIIClassification:
        """
        Detect PII using rule-based approach based on column names and sample values
        
        Args:
            asset_info: Dictionary containing asset metadata including schema information
            
        Returns:
            PIIClassification object with detection results
        """
        schema_info = asset_info.get('metadata', {}).get('schema_info', {})
        columns = schema_info.get('columns', [])
        
        detected_pii_types = set()
        sensitive_columns = []
        
        # Check each column for PII indicators
        for column in columns:
            column_name = column.get('name', '').lower()
            column_type = column.get('type', '')
            sample_values = column.get('sample_values', [])
            
            # Convert sample values to strings for pattern matching
            sample_values_str = ' '.join([str(val).lower() for val in sample_values])
            
            is_sensitive = False
            column_pii_types = []
            
            # Check against each PII pattern
            for pii_type, patterns in self.pii_patterns.items():
                for pattern in patterns:
                    if pattern in column_name:
                        detected_pii_types.add(pii_type)
                        column_pii_types.append(pii_type)
                        is_sensitive = True
                        break
            
            if is_sensitive:
                sensitive_columns.append({
                    'name': column['name'],
                    'pii_types': column_pii_types
                })
        
        # Determine overall sensitivity level
        sensitivity_level = "Low"
        if "health" in detected_pii_types or "biometric" in detected_pii_types:
            sensitivity_level = "Restricted"
        elif "financial" in detected_pii_types or "id" in detected_pii_types:
            sensitivity_level = "High"
        elif detected_pii_types:
            sensitivity_level = "Medium"
        
        # Determine CIA rating based on highest sensitivity PII type
        cia_rating = CIARating(
            confidentiality=ConfidentialityLevel.LOW,
            integrity=IntegrityLevel.LOW,
            availability=AvailabilityLevel.LOW
        )
        
        # Update CIA rating based on detected PII types
        for pii_type in detected_pii_types:
            if pii_type in self.default_cia_ratings:
                default_rating = self.default_cia_ratings[pii_type]
                
                # Take the highest level for each component
                if self._get_level_value(default_rating.confidentiality) > self._get_level_value(cia_rating.confidentiality):
                    cia_rating.confidentiality = default_rating.confidentiality
                
                if self._get_level_value(default_rating.integrity) > self._get_level_value(cia_rating.integrity):
                    cia_rating.integrity = default_rating.integrity
                
                if self._get_level_value(default_rating.availability) > self._get_level_value(cia_rating.availability):
                    cia_rating.availability = default_rating.availability
        
        # Create and return the classification
        return PIIClassification(
            has_pii=bool(detected_pii_types),
            pii_types=list(detected_pii_types),
            sensitivity_level=sensitivity_level,
            confidence=0.85 if detected_pii_types else 0.7,  # Rule-based confidence
            cia_rating=cia_rating,
            sensitive_columns=[col['name'] for col in sensitive_columns]
        )
    
    def _get_level_value(self, level: Enum) -> int:
        """Helper method to get numeric value for enum levels for comparison"""
        if isinstance(level, ConfidentialityLevel):
            levels = {
                ConfidentialityLevel.LOW: 1,
                ConfidentialityLevel.MEDIUM: 2,
                ConfidentialityLevel.HIGH: 3,
                ConfidentialityLevel.RESTRICTED: 4
            }
            return levels.get(level, 0)
        elif isinstance(level, IntegrityLevel):
            levels = {
                IntegrityLevel.LOW: 1,
                IntegrityLevel.MEDIUM: 2,
                IntegrityLevel.HIGH: 3
            }
            return levels.get(level, 0)
        elif isinstance(level, AvailabilityLevel):
            levels = {
                AvailabilityLevel.LOW: 1,
                AvailabilityLevel.MEDIUM: 2,
                AvailabilityLevel.HIGH: 3,
                AvailabilityLevel.CRITICAL: 4
            }
            return levels.get(level, 0)
        return 0
    
    def apply_classification_to_asset(self, asset: Asset, classification: PIIClassification) -> Asset:
        """
        Apply PII classification and CIA ratings to an asset
        
        Args:
            asset: The Atlan asset to update
            classification: PIIClassification object with detection results
            
        Returns:
            Updated asset with classifications applied
        """
        try:
            # Create an updater for the asset
            updater = type(asset).updater(
                qualified_name=asset.qualified_name,
                name=asset.name
            )
            
            # Apply CIA ratings as custom attributes
            updater.custom_metadata_set(
                "CIAClassification",
                {
                    "confidentiality": classification.cia_rating.confidentiality.value,
                    "integrity": classification.cia_rating.integrity.value,
                    "availability": classification.cia_rating.availability.value,
                    "sensitivityLevel": classification.sensitivity_level,
                    "hasPII": "Yes" if classification.has_pii else "No",
                    "piiTypes": ", ".join(classification.pii_types) if classification.pii_types else "None",
                    "classificationDate": "{{now}}",
                    "classificationConfidence": str(classification.confidence)
                }
            )
            
            # Apply appropriate compliance tags based on PII types
            tags_to_apply = self._determine_compliance_tags(classification)
            
            # Save the asset with updated metadata
            updated_asset = self.atlan_client.asset.save(updater)
            
            # Apply tags separately (as they may require a different API call)
            if tags_to_apply:
                self._apply_tags_to_asset(asset, tags_to_apply)
            
            logger.info(f"Applied PII classification to asset {asset.qualified_name}")
            return updated_asset
            
        except Exception as e:
            logger.error(f"Failed to apply classification to asset {asset.qualified_name}: {str(e)}")
            return asset
    
    def _determine_compliance_tags(self, classification: PIIClassification) -> List[str]:
        """Determine which compliance tags to apply based on PII classification"""
        tags = []
        
        if not classification.has_pii:
            return tags
        
        # Apply Singapore PDPA for personal data
        if any(pii_type in ["email", "phone", "name", "address", "id"] for pii_type in classification.pii_types):
            tags.append("singapore_pdpa")
        
        # Apply Indonesia PP No. 71/2019 for electronic data
        if any(pii_type in ["email", "id", "financial"] for pii_type in classification.pii_types):
            tags.append("indonesia_pp71")
        
        # Apply GDPR equivalent for comprehensive personal data
        if len(classification.pii_types) >= 2 and "name" in classification.pii_types:
            tags.append("gdpr_equivalent")
        
        # Apply financial tag for financial data
        if "financial" in classification.pii_types:
            tags.append("financial_sensitive")
        
        # Apply customer data tag for customer-related information
        if "name" in classification.pii_types or "customer" in ' '.join(classification.sensitive_columns).lower():
            tags.append("customer_data")
        
        return tags
    
    def _apply_tags_to_asset(self, asset: Asset, tag_ids: List[str]) -> None:
        """Apply compliance tags to an asset"""
        try:
            for tag_id in tag_ids:
                if tag_id in self.compliance_tags:
                    tag_name = self.compliance_tags[tag_id]
                    # Note: This is a placeholder. The actual implementation depends on Atlan's API
                    # for applying tags, which might differ from the standard asset update
                    logger.info(f"Would apply tag '{tag_name}' to asset {asset.qualified_name}")
                    
                    # Placeholder for actual tag application
                    # self.atlan_client.asset.add_tag(asset.guid, tag_name)
            
        except Exception as e:
            logger.error(f"Failed to apply tags to asset {asset.qualified_name}: {str(e)}")
    
    async def propagate_classification_through_lineage(self, asset_guid: str, classification: PIIClassification) -> Dict[str, Any]:
        """
        Propagate PII classification to related assets through lineage
        
        Args:
            asset_guid: GUID of the asset to start propagation from
            classification: PIIClassification to propagate
            
        Returns:
            Dictionary with propagation results
        """
        results = {
            "source_asset": asset_guid,
            "propagated_to": [],
            "failed": []
        }
        
        try:
            # Get lineage relationships for this asset
            lineage = self.atlan_client.lineage.get_lineage(
                guid=asset_guid,
                direction="BOTH",
                depth=1
            )
            
            # Process upstream assets
            for upstream_asset in lineage.get_upstream_assets():
                try:
                    # Get the full asset to update
                    full_asset = self.atlan_client.asset.get_by_guid(upstream_asset.guid)
                    
                    # Apply the same classification
                    self.apply_classification_to_asset(full_asset, classification)
                    
                    results["propagated_to"].append({
                        "guid": upstream_asset.guid,
                        "name": upstream_asset.name,
                        "type": upstream_asset.type_name,
                        "direction": "upstream"
                    })
                except Exception as e:
                    results["failed"].append({
                        "guid": upstream_asset.guid,
                        "name": upstream_asset.name,
                        "error": str(e),
                        "direction": "upstream"
                    })
            
            # Process downstream assets
            for downstream_asset in lineage.get_downstream_assets():
                try:
                    # Get the full asset to update
                    full_asset = self.atlan_client.asset.get_by_guid(downstream_asset.guid)
                    
                    # Apply the same classification
                    self.apply_classification_to_asset(full_asset, classification)
                    
                    results["propagated_to"].append({
                        "guid": downstream_asset.guid,
                        "name": downstream_asset.name,
                        "type": downstream_asset.type_name,
                        "direction": "downstream"
                    })
                except Exception as e:
                    results["failed"].append({
                        "guid": downstream_asset.guid,
                        "name": downstream_asset.name,
                        "error": str(e),
                        "direction": "downstream"
                    })
            
            return results
            
        except Exception as e:
            logger.error(f"Failed to propagate classification through lineage: {str(e)}")
            results["error"] = str(e)
            return results
    
    async def generate_pii_inventory_report(self) -> Dict[str, Any]:
        """
        Generate a comprehensive inventory report of PII data across the pipeline
        
        Returns:
            Dictionary containing the PII inventory report
        """
        # This is a placeholder for the actual implementation
        # In a real implementation, you would query Atlan for all assets with PII classifications
        
        report = {
            "generated_at": "{{now}}",
            "summary": {
                "total_assets_scanned": 0,
                "assets_with_pii": 0,
                "high_sensitivity_assets": 0,
                "medium_sensitivity_assets": 0,
                "low_sensitivity_assets": 0
            },
            "by_source": {
                "postgres": {
                    "total": 0,
                    "with_pii": 0
                },
                "s3": {
                    "total": 0,
                    "with_pii": 0
                },
                "snowflake": {
                    "total": 0,
                    "with_pii": 0
                }
            },
            "by_pii_type": {
                "email": 0,
                "phone": 0,
                "name": 0,
                "address": 0,
                "id": 0,
                "financial": 0,
                "health": 0,
                "biometric": 0
            },
            "sensitive_assets": []
        }
        
        # In a real implementation, you would populate this report with actual data
        # from Atlan by querying assets with PII classifications
        
        return report