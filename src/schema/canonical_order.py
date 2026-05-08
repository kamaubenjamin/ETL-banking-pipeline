"""
Canonical Order Schema - Standardized order representation across all sources.

This schema defines the normalized structure for order data from various sources
including ERP systems, email processing, manual entry, and external systems.
"""

from dataclasses import dataclass, field
from typing import Dict, Any, Optional, List
from datetime import datetime
from decimal import Decimal


@dataclass
class CanonicalOrder:
    """
    Canonical order representation for unified order processing.

    Supports order processing across multiple channels with standardized
    header information, line items, and processing metadata.
    """

    # Core Identity
    canonical_id: str
    order_number: str
    order_date: datetime
    source: str
    source_type: str  # 'erp', 'email', 'manual', 'api', 'external'

    # Customer Information
    customer_id: Optional[str] = None
    customer_name: str = ""
    delivery_location: Optional[str] = None

    # Order Details
    order_type: str = "standard"  # 'standard', 'quote', 'return', etc.
    status: str = "draft"  # 'draft', 'confirmed', 'processing', 'shipped', 'delivered'
    priority: str = "normal"  # 'low', 'normal', 'high', 'urgent'

    # Financial Information
    currency: str = "KES"
    total_amount: Optional[Decimal] = None
    tax_amount: Optional[Decimal] = None
    discount_amount: Optional[Decimal] = None

    # Additional fields with defaults
    source_id: Optional[str] = None
    source_file_path: Optional[str] = None

    # Processing Metadata
    processed_at: datetime = field(default_factory=datetime.now)
    processed_by: Optional[str] = None
    confidence_score: float = 0.0

    # Line Items (references to CanonicalItem objects)
    items: List[str] = field(default_factory=list)  # List of canonical item IDs

    # Additional Features
    features: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)

    # Quality Indicators
    data_quality_score: float = 0.0
    validation_errors: List[str] = field(default_factory=list)

    def __post_init__(self):
        """Validate and normalize data after initialization."""
        if not self.canonical_id:
            raise ValueError("canonical_id is required")

        if not self.order_number:
            raise ValueError("order_number is required")

        if not isinstance(self.order_date, datetime):
            raise ValueError("order_date must be a datetime object")

        # Validate enums
        valid_order_types = ['standard', 'quote', 'return', 'sample', 'replacement']
        if self.order_type not in valid_order_types:
            raise ValueError(f"order_type must be one of {valid_order_types}")

        valid_statuses = ['draft', 'confirmed', 'processing', 'shipped', 'delivered', 'cancelled']
        if self.status not in valid_statuses:
            raise ValueError(f"status must be one of {valid_statuses}")

        valid_priorities = ['low', 'normal', 'high', 'urgent']
        if self.priority not in valid_priorities:
            raise ValueError(f"priority must be one of {valid_priorities}")

        valid_source_types = ['erp', 'email', 'manual', 'api', 'external']
        if self.source_type not in valid_source_types:
            raise ValueError(f"source_type must be one of {valid_source_types}")

        # Normalize currency
        if self.currency:
            self.currency = self.currency.upper()

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'CanonicalOrder':
        """Create CanonicalOrder from dictionary."""
        # Handle datetime conversion
        for datetime_field in ['order_date', 'processed_at']:
            if datetime_field in data and isinstance(data[datetime_field], str):
                data[datetime_field] = datetime.fromisoformat(data[datetime_field])

        # Handle Decimal conversion
        for decimal_field in ['total_amount', 'tax_amount', 'discount_amount']:
            if decimal_field in data and data[decimal_field] is not None:
                data[decimal_field] = Decimal(str(data[decimal_field]))

        return cls(**data)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        result = {
            'canonical_id': self.canonical_id,
            'order_number': self.order_number,
            'order_date': self.order_date.isoformat(),
            'customer_id': self.customer_id,
            'customer_name': self.customer_name,
            'delivery_location': self.delivery_location,
            'order_type': self.order_type,
            'status': self.status,
            'priority': self.priority,
            'currency': self.currency,
            'total_amount': float(self.total_amount) if self.total_amount else None,
            'tax_amount': float(self.tax_amount) if self.tax_amount else None,
            'discount_amount': float(self.discount_amount) if self.discount_amount else None,
            'source': self.source,
            'source_type': self.source_type,
            'source_id': self.source_id,
            'source_file_path': self.source_file_path,
            'processed_at': self.processed_at.isoformat(),
            'processed_by': self.processed_by,
            'confidence_score': self.confidence_score,
            'items': self.items,
            'features': self.features,
            'metadata': self.metadata,
            'data_quality_score': self.data_quality_score,
            'validation_errors': self.validation_errors,
        }
        return result

    def add_item(self, item_id: str):
        """Add an item to the order."""
        if item_id not in self.items:
            self.items.append(item_id)

    def remove_item(self, item_id: str):
        """Remove an item from the order."""
        if item_id in self.items:
            self.items.remove(item_id)

    def update_status(self, new_status: str, processed_by: str = None):
        """Update order status."""
        valid_statuses = ['draft', 'confirmed', 'processing', 'shipped', 'delivered', 'cancelled']
        if new_status not in valid_statuses:
            raise ValueError(f"Invalid status: {new_status}")

        self.status = new_status
        self.processed_at = datetime.now()
        if processed_by:
            self.processed_by = processed_by

    def calculate_order_total(self) -> Decimal:
        """Calculate total order amount (placeholder - would need item details)."""
        # This would typically sum up item totals
        # For now, return stored total or zero
        return self.total_amount or Decimal('0')

    def validate_order(self) -> List[str]:
        """Validate order data and return list of errors."""
        errors = []

        if not self.customer_name:
            errors.append("Customer name is required")

        if not self.items:
            errors.append("Order must have at least one item")

        if self.total_amount and self.total_amount < 0:
            errors.append("Total amount cannot be negative")

        return errors