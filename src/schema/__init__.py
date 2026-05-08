"""
Canonical Data Schema Layer

This package provides standardized data schemas for all entities in the ETL pipeline.
All data transformations must produce these canonical formats to ensure consistent
matching, comparison, and integration across internal and external data sources.

Schemas:
- CanonicalProduct: Standardized product representation
- CanonicalCustomer: Standardized customer representation
- CanonicalOrder: Standardized order representation
- CanonicalItem: Standardized order line item representation

Usage:
    from src.schema import CanonicalProduct, CanonicalCustomer, CanonicalOrder, CanonicalItem
"""

from .canonical_product import CanonicalProduct
from .canonical_customer import CanonicalCustomer
from .canonical_order import CanonicalOrder
from .canonical_item import CanonicalItem

__all__ = [
    'CanonicalProduct',
    'CanonicalCustomer',
    'CanonicalOrder',
    'CanonicalItem',
]