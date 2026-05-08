"""
Canonical Item Schema - Standardized order line item representation.

This schema defines the normalized structure for individual order line items,
linking products to orders with quantity, pricing, and delivery information.
"""

from dataclasses import dataclass, field
from typing import Dict, Any, Optional, List
from datetime import datetime
from decimal import Decimal


@dataclass
class CanonicalItem:
    """
    Canonical order line item representation.

    Represents individual products within orders with quantity, pricing,
    and delivery requirements. Links to CanonicalProduct and CanonicalOrder.
    """

    # Core Identity
    canonical_id: str
    order_id: str  # Reference to CanonicalOrder.canonical_id
    product_id: str  # Reference to CanonicalProduct.canonical_id
    product_name: str
    quantity: int
    unit_price: Decimal
    source: str
    source_type: str  # 'erp', 'email', 'manual', 'api'

    # Product Information (denormalized for performance)
    product_code: Optional[str] = None
    brand: Optional[str] = None
    category: Optional[str] = None

    # Quantity and Units
    unit_of_measure: str = "PCS"
    pcs_per_carton: Optional[int] = None

    # Pricing
    currency: str = "KES"
    discount_amount: Optional[Decimal] = None
    tax_amount: Optional[Decimal] = None

    # Delivery Information
    delivery_date: Optional[datetime] = None
    delivery_location: Optional[str] = None
    special_instructions: Optional[str] = None

    # Additional fields with defaults
    source_line_number: Optional[int] = None

    # Processing Metadata
    processed_at: datetime = field(default_factory=datetime.now)
    processed_by: Optional[str] = None
    confidence_score: float = 0.0

    # Status
    status: str = "pending"  # 'pending', 'confirmed', 'shipped', 'delivered', 'cancelled'

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

        if not self.order_id:
            raise ValueError("order_id is required")

        if not self.product_id:
            raise ValueError("product_id is required")

        if not self.product_name:
            raise ValueError("product_name is required")

        if self.quantity <= 0:
            raise ValueError("quantity must be positive")

        if self.unit_price <= 0:
            raise ValueError("unit_price must be positive")

        # Calculate line total
        self.line_total = self.unit_price * Decimal(str(self.quantity))

        # Validate enums
        valid_statuses = ['pending', 'confirmed', 'shipped', 'delivered', 'cancelled', 'backordered']
        if self.status not in valid_statuses:
            raise ValueError(f"status must be one of {valid_statuses}")

        valid_source_types = ['erp', 'email', 'manual', 'api', 'external']
        if self.source_type not in valid_source_types:
            raise ValueError(f"source_type must be one of {valid_source_types}")

        # Normalize currency
        if self.currency:
            self.currency = self.currency.upper()

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'CanonicalItem':
        """Create CanonicalItem from dictionary."""
        # Handle datetime conversion
        for datetime_field in ['delivery_date', 'processed_at']:
            if datetime_field in data and isinstance(data[datetime_field], str):
                data[datetime_field] = datetime.fromisoformat(data[datetime_field])

        # Handle Decimal conversion
        for decimal_field in ['unit_price', 'discount_amount', 'tax_amount']:
            if decimal_field in data and data[decimal_field] is not None:
                data[decimal_field] = Decimal(str(data[decimal_field]))

        return cls(**data)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        result = {
            'canonical_id': self.canonical_id,
            'order_id': self.order_id,
            'product_id': self.product_id,
            'product_name': self.product_name,
            'product_code': self.product_code,
            'brand': self.brand,
            'category': self.category,
            'quantity': self.quantity,
            'unit_of_measure': self.unit_of_measure,
            'pcs_per_carton': self.pcs_per_carton,
            'unit_price': float(self.unit_price),
            'currency': self.currency,
            'discount_amount': float(self.discount_amount) if self.discount_amount else None,
            'tax_amount': float(self.tax_amount) if self.tax_amount else None,
            'delivery_date': self.delivery_date.isoformat() if self.delivery_date else None,
            'delivery_location': self.delivery_location,
            'special_instructions': self.special_instructions,
            'source': self.source,
            'source_type': self.source_type,
            'source_line_number': self.source_line_number,
            'processed_at': self.processed_at.isoformat(),
            'processed_by': self.processed_by,
            'confidence_score': self.confidence_score,
            'status': self.status,
            'features': self.features,
            'metadata': self.metadata,
            'data_quality_score': self.data_quality_score,
            'validation_errors': self.validation_errors,
        }
        return result

    def update_quantity(self, new_quantity: int):
        """Update item quantity and recalculate totals."""
        if new_quantity <= 0:
            raise ValueError("quantity must be positive")

        self.quantity = new_quantity
        self.line_total = self.unit_price * Decimal(str(self.quantity))
        self.processed_at = datetime.now()

    def update_price(self, new_price: Decimal):
        """Update unit price and recalculate line total."""
        if new_price <= 0:
            raise ValueError("unit_price must be positive")

        self.unit_price = new_price
        self.line_total = self.unit_price * Decimal(str(self.quantity))
        self.processed_at = datetime.now()

    def apply_discount(self, discount_amount: Decimal):
        """Apply discount to the line item."""
        if discount_amount < 0:
            raise ValueError("discount_amount cannot be negative")

        if discount_amount > self.line_total:
            raise ValueError("discount_amount cannot exceed line total")

        self.discount_amount = discount_amount
        self.processed_at = datetime.now()

    def update_status(self, new_status: str, processed_by: str = None):
        """Update item status."""
        valid_statuses = ['pending', 'confirmed', 'shipped', 'delivered', 'cancelled', 'backordered']
        if new_status not in valid_statuses:
            raise ValueError(f"Invalid status: {new_status}")

        self.status = new_status
        self.processed_at = datetime.now()
        if processed_by:
            self.processed_by = processed_by

    def calculate_net_total(self) -> Decimal:
        """Calculate net total after discounts and taxes."""
        net_total = self.line_total

        if self.discount_amount:
            net_total -= self.discount_amount

        if self.tax_amount:
            net_total += self.tax_amount

        return net_total

    def validate_item(self) -> List[str]:
        """Validate item data and return list of errors."""
        errors = []

        if self.quantity <= 0:
            errors.append("Quantity must be positive")

        if self.unit_price <= 0:
            errors.append("Unit price must be positive")

        if self.discount_amount and self.discount_amount > self.line_total:
            errors.append("Discount amount cannot exceed line total")

        if self.delivery_date and self.delivery_date < datetime.now():
            errors.append("Delivery date cannot be in the past")

        return errors