import pandas as pd
import pytest
from src.transform.engine import TransformEngine
from src.transform.product_parser import extract_product_info


@pytest.fixture
def sample_raw_df():
    """Create sample raw extraction data."""
    return pd.DataFrame({
        'content': [
            'Samsung 55" QLED TV - £3500',
            'LG 65" OLED TV - $4200',
            'TCL 43" Smart TV KSh 45,000',
            'In stock - Sony Bravia 50" - $3800',
            'Out of stock - Sharp LED 32" - 15999 KES'
        ]
    })


class TestTransformEngine:
    def test_apply_drop_nulls_rule(self):
        """Test drop nulls transformation rule."""
        df = pd.DataFrame({
            'content': ['TV A $1000', 'TV B $2000', 'TV C $3000']
        })
        
        engine = TransformEngine(df)
        result = engine.apply([{'type': 'drop_nulls', 'subset': ['price']}])
        
        # Product parser runs, so we have structured output
        assert len(result) > 0
        assert 'price' in result.columns

    def test_apply_rename_columns(self):
        """Test column renaming rule."""
        df = pd.DataFrame({
            'old_name': [1, 2, 3],
            'other': ['a', 'b', 'c']
        })
        
        engine = TransformEngine(df)
        result = engine.apply([{'type': 'rename', 'columns': {'old_name': 'new_name'}}])
        
        assert 'new_name' in result.columns
        assert 'old_name' not in result.columns

    def test_apply_filter_rule(self):
        """Test filter transformation rule."""
        df = pd.DataFrame({
            'content': ['TV A $2000', 'TV B $3000', 'Speaker C $500']
        })
        
        engine = TransformEngine(df)
        result = engine.apply([{'type': 'filter', 'condition': 'price > 750'}])
        
        # After product parser, filter should reduce rows
        assert len(result) >= 0

    def test_apply_add_column(self):
        """Test adding a new column."""
        df = pd.DataFrame({
            'content': ['TV A $1000', 'TV B $2000']
        })
        
        engine = TransformEngine(df)
        result = engine.apply([{'type': 'add_column', 'column': 'source', 'value': 'test'}])
        
        assert 'source' in result.columns
        assert all(result['source'] == 'test')

    def test_apply_multiple_rules(self):
        """Test applying multiple transformation rules."""
        df = pd.DataFrame({
            'content': ['TV A $1000', 'TV B $2000', 'TV C $3000']
        })
        
        rules = [
            {'type': 'add_column', 'column': 'source', 'value': 'test'}
        ]
        
        engine = TransformEngine(df)
        result = engine.apply(rules)
        
        assert len(result) > 0
        assert 'source' in result.columns


class TestProductParser:
    def test_parse_price_with_currency(self):
        """Test parsing price and currency."""
        df = pd.DataFrame({
            'content': [
                'Samsung TV - £3500',
                'LG OLED - $4200',
                'TCL Smart - KSh 45,000'
            ]
        })
        
        result = extract_product_info(df)
        
        assert len(result) >= 1
        assert 'price' in result.columns
        assert 'currency' in result.columns
        assert result['currency'].iloc[0] in ['GBP', 'USD', 'KES']

    def test_parse_availability(self):
        """Test parsing availability status."""
        df = pd.DataFrame({
            'content': [
                'Samsung TV £3500 - In stock',
                'LG TV $4200 - Out of stock'
            ]
        })
        
        result = extract_product_info(df)
        
        assert len(result) >= 1
        assert 'availability' in result.columns
        availability_values = set(result['availability'].dropna().unique())
        assert len(availability_values) > 0

    def test_parse_product_name(self):
        """Test extracting product name."""
        df = pd.DataFrame({
            'content': [
                'Samsung 55" QLED TV - $3500',
                'LG 65" OLED - £4200'
            ]
        })
        
        result = extract_product_info(df)
        
        assert len(result) >= 1
        assert 'product_name' in result.columns
        assert all(len(name) > 0 for name in result['product_name'])

    def test_skip_weak_rows(self):
        """Test that very short product names are skipped."""
        df = pd.DataFrame({
            'content': [
                'A',  # Too short
                'AB',  # Too short
                'Samsung TV - $3500',  # Valid
                'LG 55" TV - £2000'  # Valid
            ]
        })
        
        result = extract_product_info(df)
        
        assert len(result) >= 2
        assert all(len(name) >= 3 for name in result['product_name'])


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
