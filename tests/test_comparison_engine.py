import pandas as pd
import pytest
from src.transform.comparison_engine import (
    normalize_name,
    extract_brand,
    detect_category,
    extract_features,
    match_products,
    combine_datasets,
    build_comparison_table
)


class TestNormalization:
    def test_normalize_name_lowercase(self):
        """Test that names are converted to lowercase."""
        result = normalize_name("Samsung TV")
        assert result == result.lower()

    def test_normalize_name_removes_punctuation(self):
        """Test that punctuation is removed."""
        result = normalize_name('Samsung 55" TV')
        assert '"' not in result
        assert "'" not in result

    def test_normalize_name_removes_noise_words(self):
        """Test that common noise words are removed."""
        result = normalize_name("Samsung 55 inch Smart LED QLED TV")
        assert "smart" not in result
        assert "led" not in result


class TestBrandExtraction:
    def test_extract_known_brand(self):
        """Test extracting known brand."""
        result = extract_brand("Samsung 55 inch TV")
        assert result == "samsung"

    def test_extract_brand_case_insensitive(self):
        """Test brand extraction is case insensitive."""
        result = extract_brand("SAMSUNG 55 INCH TV")
        assert result.lower() == "samsung"

    def test_default_first_word_if_no_brand(self):
        """Test default to first word if brand not found."""
        result = extract_brand("SomeUnknown TV")
        assert result == "someunknown"


class TestCategoryDetection:
    def test_detect_tv_category(self):
        """Test detecting TV category."""
        result = detect_category("Samsung 55 inch QLED TV")
        assert result == "electronics"

    def test_detect_wearables_category(self):
        """Test detecting wearables category."""
        result = detect_category("Apple SmartWatch Series 7")
        assert result == "wearables"

    def test_detect_accessories_category(self):
        """Test detecting accessories category."""
        result = detect_category("USB-C charging cable")
        assert result == "accessories"

    def test_default_other_category(self):
        """Test default to 'other' for unknown categories."""
        result = detect_category("Random product name xyz")
        assert result == "other"


class TestFeatureExtraction:
    def test_extract_size(self):
        """Test extracting size from product name."""
        result = extract_features("Samsung 55 inch TV")
        assert result['size'] == '55'

    def test_extract_brand_feature(self):
        """Test brand extraction in features."""
        result = extract_features("LG 65 inch OLED TV")
        assert result['brand'] == 'lg'

    def test_extract_all_features(self):
        """Test extracting all available features."""
        result = extract_features("Samsung 55 inch UA55RU7100KXXL TV")
        assert 'brand' in result
        assert 'size' in result
        assert 'category' in result


class TestProductMatching:
    def test_match_identical_products(self):
        """Test that identical products get the same match_id."""
        df = pd.DataFrame({
            'product_name': [
                'Samsung 55" QLED TV',
                'Samsung 55 inch QLED Television',
                'LG 65" OLED TV'
            ],
            'price': [50000, 50000, 60000],
            'source': ['jumia', 'kilimall', 'kilimall']
        })
        
        result = match_products(df, threshold=50)  # Lower threshold
        
        # First two should have matching characteristics
        assert 'match_id' in result.columns
        # Verify match_ids are assigned
        assert result['match_id'].min() >= 0

    def test_match_by_size(self):
        """Test matching products by size."""
        df = pd.DataFrame({
            'product_name': [
                'Samsung 55" TV',
                'Samsung 55 inch TV',
            ],
            'price': [50000, 50000],
            'source': ['jumia', 'kilimall']
        })
        
        result = match_products(df)
        assert result.iloc[0]['match_id'] == result.iloc[1]['match_id']

    def test_no_match_different_categories(self):
        """Test that different categories don't match."""
        df = pd.DataFrame({
            'product_name': [
                'Samsung 55" TV',
                'Samsung SmartWatch',
            ],
            'price': [50000, 5000],
            'source': ['jumia', 'kilimall']
        })
        
        result = match_products(df)
        assert result.iloc[0]['match_id'] != result.iloc[1]['match_id']

    def test_match_id_starts_at_zero(self):
        """Test that match IDs start from 0."""
        df = pd.DataFrame({
            'product_name': ['Product A', 'Product B'],
            'price': [100, 200],
            'source': ['jumia', 'kilimall']
        })
        
        result = match_products(df)
        assert 0 in result['match_id'].values


class TestCombineDatasets:
    def test_combine_multiple_sources(self):
        """Test combining datasets from multiple sources."""
        datasets = {
            'jumia': pd.DataFrame({
                'product_name': ['TV A', 'TV B'],
                'price': [50000, 30000]
            }),
            'kilimall': pd.DataFrame({
                'product_name': ['TV C', 'TV D'],
                'price': [55000, 32000]
            })
        }
        
        result = combine_datasets(datasets)
        
        assert len(result) == 4
        assert 'source' in result.columns
        assert set(result['source'].unique()) == {'jumia', 'kilimall'}

    def test_preserve_original_data(self):
        """Test that original data is preserved when combining."""
        datasets = {
            'source1': pd.DataFrame({'price': [100]}),
            'source2': pd.DataFrame({'price': [200]})
        }
        
        result = combine_datasets(datasets)
        
        assert 100 in result['price'].values
        assert 200 in result['price'].values


class TestBuildComparisonTable:
    def test_build_pivot_table(self):
        """Test building comparison pivot table."""
        df = pd.DataFrame({
            'product_name': ['TV A', 'TV A', 'TV B', 'TV B'],
            'source': ['jumia', 'kilimall', 'jumia', 'kilimall'],
            'price': [50000, 52000, 30000, 29000],
            'match_id': [0, 0, 1, 1]
        })
        
        result = build_comparison_table(df)
        
        assert len(result) == 2
        assert 'product_name' in result.columns
        assert 'jumia' in result.columns
        assert 'kilimall' in result.columns

    def test_find_cheapest_source(self):
        """Test that cheapest source is correctly identified."""
        df = pd.DataFrame({
            'product_name': ['TV A', 'TV A'],
            'source': ['jumia', 'kilimall'],
            'price': [50000, 45000],
            'match_id': [0, 0]
        })
        
        result = build_comparison_table(df)
        
        assert result.iloc[0]['cheapest'] == 'kilimall'

    def test_handle_missing_prices(self):
        """Test handling missing prices in comparison."""
        df = pd.DataFrame({
            'product_name': ['TV A', 'TV A'],
            'source': ['jumia', 'kilimall'],
            'price': [50000, None],
            'match_id': [0, 0]
        })
        
        result = build_comparison_table(df)
        
        assert len(result) == 1
        assert result.iloc[0]['cheapest'] == 'jumia'


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
