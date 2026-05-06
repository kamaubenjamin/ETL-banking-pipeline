from src.extract.extract import run_extraction
import src.config as config


def test_extract_real_website():
    """Test extraction from real website."""
    df = run_extraction(
        source_type="default (web)",
        config=config,
        mode=None,
        selector=None
    )

    # Basic checks using real data
    assert df is not None
    assert len(df) > 0
    assert df.shape[1] > 0  # Has columns