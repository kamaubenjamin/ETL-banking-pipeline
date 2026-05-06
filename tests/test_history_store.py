import pandas as pd
import pytest
import os
from datetime import datetime, timedelta
from src.storage.history_store import save_snapshot, detect_price_changes
from src.alerts.alert_engine import generate_alerts


@pytest.fixture
def sample_comparison_df():
    """Create a sample comparison dataframe."""
    return pd.DataFrame({
        'product_name': [
            'Vision Plus 50" QLED TV',
            'Amtec 43" Smart TV',
            'Samsung 55" UHD TV'
        ],
        'jumia': [35666.0, 16743.0, 45000.0],
        'kilimall': [36000.0, 17999.0, None],
        'cheapest': ['jumia', 'jumia', 'jumia']
    })


@pytest.fixture
def history_file(tmp_path):
    """Create a temporary history file."""
    file_path = tmp_path / "test_history.csv"
    return str(file_path)


class TestSaveSnapshot:
    def test_save_new_snapshot(self, sample_comparison_df, history_file):
        """Test saving a new snapshot when file doesn't exist."""
        save_snapshot(sample_comparison_df, history_file)
        
        assert os.path.exists(history_file)
        df = pd.read_csv(history_file)
        assert len(df) == 3
        assert 'timestamp' in df.columns
        assert df['timestamp'].notna().all()

    def test_append_to_existing_snapshot(self, sample_comparison_df, history_file):
        """Test appending new data to existing snapshot."""
        save_snapshot(sample_comparison_df, history_file)
        
        new_data = sample_comparison_df.copy()
        new_data['jumia'] = [37000.0, 17000.0, 46000.0]
        save_snapshot(new_data, history_file)
        
        df = pd.read_csv(history_file)
        assert len(df) == 6  # 3 original + 3 new
        assert 'timestamp' in df.columns

    def test_timestamp_format_is_iso(self, sample_comparison_df, history_file):
        """Test that timestamps are saved in ISO format."""
        save_snapshot(sample_comparison_df, history_file)
        
        df = pd.read_csv(history_file)
        first_timestamp = df['timestamp'].iloc[0]
        
        # ISO format should be parseable
        parsed = pd.to_datetime(first_timestamp)
        assert parsed is not pd.NaT


class TestDetectPriceChanges:
    def test_detect_price_increase(self):
        """Test detecting price increase."""
        df = pd.DataFrame({
            'product_name': ['Product A', 'Product A'],
            'jumia': [100.0, 120.0],
            'kilimall': [110.0, 110.0],
            'timestamp': ['2026-05-05 10:00:00', '2026-05-05 11:00:00']
        })
        
        changes = detect_price_changes(df)
        assert len(changes) == 1
        assert changes.iloc[0]['product'] == 'Product A'
        assert changes.iloc[0]['source'] == 'jumia'
        assert changes.iloc[0]['old_price'] == 100.0
        assert changes.iloc[0]['new_price'] == 120.0

    def test_detect_price_decrease(self):
        """Test detecting price decrease (undercut)."""
        df = pd.DataFrame({
            'product_name': ['Product B', 'Product B'],
            'jumia': [5000.0, 4500.0],
            'kilimall': [5200.0, 5200.0],
            'timestamp': ['2026-05-05 10:00:00', '2026-05-05 11:00:00']
        })
        
        changes = detect_price_changes(df)
        assert len(changes) == 1
        assert changes.iloc[0]['new_price'] < changes.iloc[0]['old_price']

    def test_ignore_nan_prices(self):
        """Test that NaN prices are ignored."""
        df = pd.DataFrame({
            'product_name': ['Product C', 'Product C'],
            'jumia': [100.0, 120.0],
            'kilimall': [None, None],
            'timestamp': ['2026-05-05 10:00:00', '2026-05-05 11:00:00']
        })
        
        changes = detect_price_changes(df)
        # Should only detect jumia change, kilimall is all NaN
        assert all(changes['source'] == 'jumia')

    def test_no_change_same_price(self):
        """Test that same prices are not flagged as changes."""
        df = pd.DataFrame({
            'product_name': ['Product D', 'Product D'],
            'jumia': [1000.0, 1000.0],
            'kilimall': [1050.0, 1050.0],
            'timestamp': ['2026-05-05 10:00:00', '2026-05-05 11:00:00']
        })
        
        changes = detect_price_changes(df)
        assert len(changes) == 0

    def test_handle_missing_timestamp(self):
        """Test that missing timestamp column doesn't break detection."""
        df = pd.DataFrame({
            'product_name': ['Product E', 'Product E'],
            'jumia': [100.0, 120.0],
            'kilimall': [110.0, 110.0]
        })
        
        changes = detect_price_changes(df)
        assert len(changes) == 1


class TestGenerateAlerts:
    def test_generate_undercut_alert(self):
        """Test generating undercut alert."""
        changes = pd.DataFrame({
            'product': ['Samsung TV'],
            'source': ['jumia'],
            'old_price': [50000.0],
            'new_price': [45000.0]
        })
        
        alerts = generate_alerts(changes)
        assert len(alerts) == 1
        assert '🚨 UNDERCUT ALERT' in alerts[0]
        assert 'jumia' in alerts[0]
        assert '50000' in alerts[0]
        assert '45000' in alerts[0]

    def test_generate_price_increase_alert(self):
        """Test generating price increase alert."""
        changes = pd.DataFrame({
            'product': ['Samsung TV'],
            'source': ['kilimall'],
            'old_price': [45000.0],
            'new_price': [50000.0]
        })
        
        alerts = generate_alerts(changes)
        assert len(alerts) == 1
        assert '⬆️ PRICE INCREASE' in alerts[0]

    def test_empty_changes_no_alert(self):
        """Test that empty changes return default message."""
        changes = pd.DataFrame()
        alerts = generate_alerts(changes)
        assert len(alerts) == 1
        assert 'No price changes detected' in alerts[0]

    def test_multiple_alerts(self):
        """Test generating multiple alerts."""
        changes = pd.DataFrame({
            'product': ['Samsung TV', 'LG TV', 'LG TV'],
            'source': ['jumia', 'jumia', 'kilimall'],
            'old_price': [50000.0, 30000.0, 31000.0],
            'new_price': [45000.0, 30000.0, 32000.0]
        })
        
        alerts = generate_alerts(changes)
        assert len(alerts) == 3


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
