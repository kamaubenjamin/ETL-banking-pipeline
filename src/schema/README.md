# Canonical Data Schema Layer

This package provides the **Canonical Data Schema Layer** - the standardized data structures that form the backbone of Phase 3A Internal Dataset Integration. All data transformations across the ETL pipeline must produce these canonical formats to ensure consistent matching, comparison, and integration.

## Overview

The canonical schemas prevent the "matching/comparison/integration chaos" that occurs when different data sources use incompatible formats. Every source (internal ERP, external market data, web scraping, API feeds) gets transformed into these standardized structures before any cross-system intelligence operations.

## Schemas

### CanonicalProduct
Standardized product representation for unified product intelligence.

**Key Fields:**
- `canonical_id`: Unique identifier across all systems
- `product_name`/`normalized_name`: Product identification
- `source`/`source_type`: Data provenance tracking
- `price`/`supplier_price`/`market_price_*`: Multi-source pricing
- `brand`/`category`: Product classification
- Quality indicators and metadata

**Use Cases:**
- Product matching across internal/external catalogs
- Price comparison and competitiveness analysis
- Supplier catalog integration

### CanonicalCustomer
Standardized customer representation for unified customer intelligence.

**Key Fields:**
- `canonical_id`: Unique identifier across all systems
- `customer_name`/`normalized_name`: Customer identification
- `email_domain`: Email-based matching
- `source`/`source_type`: Data provenance tracking
- Business information and contact details

**Use Cases:**
- Customer matching across ERP/email/historical data
- Cross-system customer intelligence
- Unified customer view for order processing

### CanonicalOrder
Standardized order representation for unified order processing.

**Key Fields:**
- `canonical_id`/`order_number`: Order identification
- `order_date`: Temporal ordering
- `customer_id`/`customer_name`: Customer linkage
- `total_amount`/`currency`: Financial information
- `status`/`priority`: Order lifecycle tracking
- `items`: List of line item references

**Use Cases:**
- Multi-channel order processing
- Order status tracking across systems
- Financial reporting and analytics

### CanonicalItem
Standardized order line item representation.

**Key Fields:**
- `canonical_id`: Unique identifier
- `order_id`/`product_id`: Relational linkages
- `quantity`/`unit_price`/`line_total`: Financial details
- `delivery_date`/`delivery_location`: Logistics
- `status`: Item-level fulfillment tracking

**Use Cases:**
- Order line item processing
- Inventory management integration
- Delivery and fulfillment coordination

## Architecture Principles

### 1. Canonical Identity
Every entity has a `canonical_id` that uniquely identifies it across all systems and sources.

### 2. Data Provenance
All records track their `source`, `source_type`, and processing metadata for auditability.

### 3. Normalized Representations
Text fields include both original and normalized versions for flexible matching.

### 4. Type Safety
Uses Python type hints and dataclasses for compile-time validation.

### 5. Serialization Support
All schemas support `to_dict()`/`from_dict()` for JSON serialization.

### 6. Validation
Built-in validation in `__post_init__()` methods ensure data integrity.

## Usage

```python
from src.schema import CanonicalProduct, CanonicalCustomer, CanonicalOrder, CanonicalItem
from datetime import datetime
from decimal import Decimal

# Create a canonical product
product = CanonicalProduct(
    canonical_id='prod-001',
    product_name='Premium Widget',
    normalized_name='premium widget',
    source='erp_system',
    source_type='internal',
    price=99.99,
    brand='Acme Corp'
)

# Serialize for storage/transmission
product_dict = product.to_dict()

# Deserialize from storage
product_copy = CanonicalProduct.from_dict(product_dict)

# Update pricing with timestamp
product.update_pricing(89.99, 'supplier_price')
```

## Integration Points

### Data Sources
- **Internal**: ERP exports, CSV files, database dumps
- **External**: Market data feeds, supplier catalogs, web scraping
- **Processed**: Email parsing, manual data entry, API responses

### Downstream Consumers
- **Matching Engine**: Product/customer deduplication and linking
- **Comparison Engine**: Price/market analysis and intelligence
- **Integration Layer**: Cross-system data synchronization
- **Reporting**: Unified analytics and business intelligence

## Quality Assurance

### Validation Rules
- Required fields cannot be empty
- Data types are enforced at instantiation
- Business rules (positive quantities/prices) are validated
- Enum values are constrained to valid options

### Testing
- Unit tests for each schema class
- Serialization/deserialization round-trip testing
- Validation error testing
- Integration tests with sample data

## Future Extensions

### Additional Schemas
- `CanonicalSupplier`: Supplier/vendor information
- `CanonicalInventory`: Stock level and availability data
- `CanonicalTransaction`: Financial transaction records

### Enhanced Features
- Schema versioning for backward compatibility
- Custom validation rules per use case
- Automatic data quality scoring
- Integration with data validation frameworks

## Maintenance

### Schema Evolution
When adding new fields:
1. Add to dataclass with appropriate defaults
2. Update `to_dict()`/`from_dict()` methods
3. Add validation in `__post_init__()` if needed
4. Update documentation and tests
5. Consider backward compatibility

### Performance Considerations
- Use `field(init=False)` for computed fields
- Consider dataclass `slots=True` for memory optimization
- Profile serialization performance for high-volume scenarios