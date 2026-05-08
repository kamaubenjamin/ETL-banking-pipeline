"""
Canonical Customer Schema - Standardized customer representation across all sources.

This schema defines the normalized structure for customer data from various sources
including ERP systems, email domains, historical data, and external databases.
"""

from dataclasses import dataclass, field
from typing import Dict, Any, Optional, List
from datetime import datetime


@dataclass
class CanonicalCustomer:
    """
    Canonical customer representation for unified customer intelligence.

    Supports customer matching across multiple data sources with standardized
    identity resolution and metadata enrichment.
    """

    # Core Identity
    canonical_id: str
    customer_name: str
    normalized_name: str

    # Source Information
    source: str
    source_type: str  # 'erp', 'email', 'manual', 'external'

    # Contact Information
    email_domain: Optional[str] = None
    phone: Optional[str] = None
    address: Optional[str] = None

    # Classification
    customer_type: Optional[str] = None  # 'business', 'individual', 'government', etc.
    industry: Optional[str] = None
    region: Optional[str] = None

    # Business Information
    company_name: Optional[str] = None
    tax_id: Optional[str] = None
    registration_number: Optional[str] = None
    source_id: Optional[str] = None

    # Status
    is_active: bool = True
    is_blocked: bool = False
    credit_limit: Optional[float] = None

    # Metadata
    timestamp: datetime = field(default_factory=datetime.now)
    confidence_score: float = 0.0
    match_type: str = "unknown"

    # Additional Features
    features: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)

    # Quality Indicators
    data_quality_score: float = 0.0
    last_verified: Optional[datetime] = None

    def __post_init__(self):
        """Validate and normalize data after initialization."""
        if not self.canonical_id:
            raise ValueError("canonical_id is required")

        if not self.customer_name:
            raise ValueError("customer_name is required")

        # Ensure normalized_name exists
        if not self.normalized_name:
            self.normalized_name = self.customer_name.lower().strip()

        # Validate source_type
        valid_source_types = ['erp', 'email', 'manual', 'external', 'historical']
        if self.source_type not in valid_source_types:
            raise ValueError(f"source_type must be one of {valid_source_types}")

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'CanonicalCustomer':
        """Create CanonicalCustomer from dictionary."""
        # Handle timestamp conversion
        for timestamp_field in ['timestamp', 'last_verified']:
            if timestamp_field in data and isinstance(data[timestamp_field], str):
                data[timestamp_field] = datetime.fromisoformat(data[timestamp_field])

        return cls(**data)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        result = {
            'canonical_id': self.canonical_id,
            'customer_name': self.customer_name,
            'normalized_name': self.normalized_name,
            'email_domain': self.email_domain,
            'phone': self.phone,
            'address': self.address,
            'customer_type': self.customer_type,
            'industry': self.industry,
            'region': self.region,
            'company_name': self.company_name,
            'tax_id': self.tax_id,
            'registration_number': self.registration_number,
            'source': self.source,
            'source_type': self.source_type,
            'source_id': self.source_id,
            'is_active': self.is_active,
            'is_blocked': self.is_blocked,
            'credit_limit': self.credit_limit,
            'timestamp': self.timestamp.isoformat(),
            'confidence_score': self.confidence_score,
            'match_type': self.match_type,
            'features': self.features,
            'metadata': self.metadata,
            'data_quality_score': self.data_quality_score,
            'last_verified': self.last_verified.isoformat() if self.last_verified else None,
        }
        return result

    def update_contact_info(self, email: str = None, phone: str = None, address: str = None):
        """Update contact information."""
        if email:
            self.email_domain = email.split('@')[-1] if '@' in email else None
        if phone:
            self.phone = phone
        if address:
            self.address = address

        self.timestamp = datetime.now()

    def calculate_match_confidence(self, source_name: str, match_method: str) -> float:
        """Calculate confidence score based on match method and source."""
        base_confidence = {
            'exact': 1.0,
            'email_domain': 0.8,
            'historical': 0.7,
            'fuzzy': 0.6,
            'semantic': 0.5,
        }.get(match_method, 0.3)

        # Adjust based on source reliability
        source_multiplier = {
            'erp': 1.0,
            'official': 0.9,
            'verified': 0.8,
            'external': 0.6,
        }.get(self.source_type, 0.5)

        return min(base_confidence * source_multiplier, 1.0)