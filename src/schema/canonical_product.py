"""
Canonical Product Schema - Standardized product representation across all sources.

This schema defines the normalized structure that ALL product data must be transformed into,
regardless of the original source format. This ensures consistent matching, comparison, and
integration across internal ERP systems, external market data, and supplier catalogs.
"""

from dataclasses import dataclass, field
from typing import Dict, Any, Optional, List
from datetime import datetime


@dataclass
class CanonicalProduct:
    """
    Canonical product representation for cross-system intelligence.

    Every source (internal CSV, ERP export, web scraping, API) gets transformed
    into this standardized structure before matching, comparison, or storage.
    """

    # Core Identity
    canonical_id: str
    product_name: str
    normalized_name: str

    # Source Information
    source: str
    source_type: str  # 'internal' or 'external'

    # Classification
    brand: Optional[str] = None
    category: Optional[str] = None
    subcategory: Optional[str] = None

    # Specifications
    model: Optional[str] = None
    size: Optional[str] = None
    unit_of_measure: Optional[str] = None
    pcs_per_carton: Optional[int] = None
    source_file_path: Optional[str] = None

    # Pricing
    price: Optional[float] = None
    currency: str = "KES"
    supplier_price: Optional[float] = None
    market_price_min: Optional[float] = None
    market_price_max: Optional[float] = None
    market_price_avg: Optional[float] = None

    # Metadata
    timestamp: datetime = field(default_factory=datetime.now)
    confidence_score: float = 0.0
    match_type: str = "unknown"

    # Additional Features (flexible for extensions)
    features: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)

    # Quality Indicators
    is_blocked: bool = False
    is_discontinued: bool = False
    quality_score: float = 0.0

    def __post_init__(self):
        """Validate and normalize data after initialization."""
        if not self.canonical_id:
            raise ValueError("canonical_id is required")

        if not self.product_name:
            raise ValueError("product_name is required")

        # Ensure normalized_name exists
        if not self.normalized_name:
            self.normalized_name = self.product_name.lower().strip()

        # Validate source_type
        if self.source_type not in ['internal', 'external']:
            raise ValueError("source_type must be 'internal' or 'external'")

        # Normalize currency
        if self.currency:
            self.currency = self.currency.upper()

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'CanonicalProduct':
        """Create CanonicalProduct from dictionary."""
        # Handle timestamp conversion
        if 'timestamp' in data and isinstance(data['timestamp'], str):
            data['timestamp'] = datetime.fromisoformat(data['timestamp'])

        return cls(**data)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        result = {
            'canonical_id': self.canonical_id,
            'product_name': self.product_name,
            'normalized_name': self.normalized_name,
            'brand': self.brand,
            'category': self.category,
            'subcategory': self.subcategory,
            'model': self.model,
            'size': self.size,
            'unit_of_measure': self.unit_of_measure,
            'pcs_per_carton': self.pcs_per_carton,
            'source': self.source,
            'source_type': self.source_type,
            'source_file_path': self.source_file_path,
            'price': self.price,
            'currency': self.currency,
            'supplier_price': self.supplier_price,
            'market_price_min': self.market_price_min,
            'market_price_max': self.market_price_max,
            'market_price_avg': self.market_price_avg,
            'timestamp': self.timestamp.isoformat(),
            'confidence_score': self.confidence_score,
            'match_type': self.match_type,
            'features': self.features,
            'metadata': self.metadata,
            'is_blocked': self.is_blocked,
            'is_discontinued': self.is_discontinued,
            'quality_score': self.quality_score,
        }
        return result

    def update_pricing(self, new_price: float, price_type: str = 'price'):
        """Update pricing information."""
        if price_type == 'supplier_price':
            self.supplier_price = new_price
        elif price_type == 'market_min':
            self.market_price_min = new_price
        elif price_type == 'market_max':
            self.market_price_max = new_price
        elif price_type == 'market_avg':
            self.market_price_avg = new_price
        else:
            self.price = new_price

        self.timestamp = datetime.now()

    def calculate_price_metrics(self) -> Dict[str, float]:
        """Calculate price competitiveness metrics."""
        metrics = {}

        if self.supplier_price and self.market_price_avg:
            metrics['price_difference'] = self.supplier_price - self.market_price_avg
            metrics['price_difference_pct'] = (metrics['price_difference'] / self.market_price_avg) * 100
            metrics['is_competitive'] = self.supplier_price <= self.market_price_avg * 1.05

        if self.market_price_max and self.supplier_price:
            metrics['undercut_potential'] = self.market_price_max - self.supplier_price
            metrics['profit_margin_pct'] = (metrics['undercut_potential'] / self.supplier_price) * 100 if self.supplier_price > 0 else 0

        return metrics