# 💰 Competitor Price Monitor ETL

Real-time multi-source price tracking with automated ETL pipeline. Monitor competitor pricing across platforms with intelligent alerts and historical price tracking.

## 🎯 Features

- **Multi-Source Extraction**: Web scraping, API integration, CSV import, and dynamic page loading (Selenium/Playwright)
- **Smart Transformation**: Product normalization, price parsing, availability detection, feature extraction
- **Intelligent Matching**: Fuzzy matching with brand, size, and category awareness for accurate product comparison
- **Price Change Alerts**: Real-time undercut detection and price increase notifications
- **Audit Trail**: Complete operation history with error tracking and performance metrics
- **Data Export**: CSV downloads and database persistence
- **Interactive Dashboard**: Streamlit-based UI for monitoring and configuration

## 📋 Table of Contents

- [Installation](#installation)
- [Quick Start](#quick-start)
- [Architecture](#architecture)
- [Configuration](#configuration)
- [API Reference](#api-reference)
- [Testing](#testing)
- [Troubleshooting](#troubleshooting)

## 🚀 Installation

### Prerequisites

- Python 3.9+
- pip or conda
- Virtual environment (recommended)

### Setup

```bash
# Clone repository
git clone <repo-url>
cd ETL\ Banking

# Create virtual environment
python -m venv venv

# Activate virtual environment
# On Windows:
.\venv\Scripts\Activate.ps1
# On macOS/Linux:
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
pip install streamlit rapidfuzz playwright selenium webdriver-manager pytest

# Download browser drivers (for Playwright)
python -m playwright install
```

### Verify Installation

```bash
python -m pytest tests/ -v
```

## 🏃 Quick Start

### Run the Dashboard

```bash
streamlit run dashboard.py
```

Then open `http://localhost:8501` in your browser.

### Run ETL Pipeline Programmatically

```python
from src.orchestrator import ETLPipeline
import src.config as config

pipeline = ETLPipeline(config)

result = pipeline.run(
    source_type="playwright",
    mode="Auto Detect",
    selector="article.product",
    rules=[{"type": "drop_nulls"}],
    load_option="CSV"
)

print(f"Extracted {result['shape'][0]} rows")
```

### Run Multi-Source Monitoring

```python
from src.pipeline.multi_source_pipeline import run_multi_source_pipeline
import src.config as config

sources = {
    "jumia": {
        "type": "playwright",
        "url": "https://www.jumia.co.ke/electronics/",
        "selector": "article.prd"
    },
    "kilimall": {
        "type": "playwright",
        "url": "https://www.kilimall.co.ke/search",
        "selector": ".product-item"
    }
}

comparison = run_multi_source_pipeline(sources, config)
print(comparison)
```

## 🏗️ Architecture

### Pipeline Stages

```
Extract → Transform → Load
   ↓         ↓         ↓
 Connectors  Rules   Storage
```

### Module Structure

```
src/
├── extract/           # Data extraction connectors
│   ├── base_connector.py
│   ├── web_scraper.py
│   ├── playwright_connector.py
│   ├── selenium_connector.py
│   ├── api_connector.py
│   └── file_loader.py
├── transform/         # Data transformation
│   ├── engine.py
│   ├── product_parser.py
│   ├── comparison_engine.py
│   ├── cleaners.py
│   └── analyzers.py
├── storage/           # History and state
│   └── history_store.py
├── alerts/            # Alert generation
│   └── alert_engine.py
├── pipeline/          # Multi-source orchestration
│   └── multi_source_pipeline.py
├── audit.py           # Audit logging and error recovery
├── orchestrator.py    # Main pipeline orchestrator
├── config.py          # Configuration
├── load.py            # Data loading
└── utils.py           # Utilities
```

### Data Flow

1. **Extract**: Fetch raw data from sources
2. **Parse**: Extract structured fields (name, price, currency, availability)
3. **Normalize**: Clean and standardize data
4. **Match**: Find equivalent products across sources
5. **Compare**: Build price comparison table
6. **Alert**: Detect and generate price change alerts
7. **Store**: Save to history and database

## ⚙️ Configuration

### Environment Setup

Edit `src/config.py`:

```python
url = 'https://your-target-url.com'
db_name = 'Banks.db'
table_name = 'products'
csv_path = './output_data/extracted_data.csv'
```

### Dashboard Configuration

Available in sidebar:

- **Data Source**: Web, CSV, API, or uploaded file
- **Scraping Mode**: Auto-detect, table extraction, full page, or custom selector
- **CSS Selector**: For dynamic content targeting
- **Transformation Rules**: Drop nulls, filter, rename columns
- **Load Destination**: CSV, Database, or both

## 📚 API Reference

### ETLPipeline

```python
from src.orchestrator import ETLPipeline

pipeline = ETLPipeline(config)

result = pipeline.run(
    source_type: str,           # "default (web)", "playwright", "selenium", "csv", "upload dataset"
    uploaded_df: pd.DataFrame,  # For "upload dataset" source
    mode: str,                  # "Auto Detect", "Table Extraction", "Full Page Text", "Custom Selector"
    selector: str,              # CSS selector for dynamic content
    rules: List[Dict],          # Transformation rules
    load_option: str            # "CSV", "Database", "Both"
) -> Dict
```

Returns:
```python
{
    "extract": {"rows": int, "cols": int, "status": str},
    "transform": {"rows": int, "cols": int, "status": str},
    "load": {"status": str, "rows": int},
    "data": pd.DataFrame,
    "shape": Tuple[int, int],
    "execution_time": float
}
```

### TransformEngine

```python
from src.transform.engine import TransformEngine

engine = TransformEngine(df)

# Available rules:
rules = [
    {"type": "drop_nulls", "subset": ["col1", "col2"]},
    {"type": "filter", "condition": "price > 1000"},
    {"type": "rename", "columns": {"old": "new"}},
    {"type": "add_column", "column": "category", "value": "electronics"}
]

result_df = engine.apply(rules)
```

### Audit Logging

```python
from src.audit import AuditLogger

audit = AuditLogger()

# Log operations
audit.log_extraction("playwright", 150, "success", duration=12.5)
audit.log_transformation(150, 145, ["drop_nulls"], "success")
audit.log_load("CSV", 145, "success")

# View audit history
df = audit.get_audit_history(operation_type="extract", hours=24)
stats = audit.get_stats(hours=24)
```

### Price Change Detection

```python
from src.storage.history_store import detect_price_changes
from src.alerts.alert_engine import generate_alerts

# Detect changes
changes = detect_price_changes(history_df)

# Generate alerts
alerts = generate_alerts(changes)
for alert in alerts:
    print(alert)
```

## 🧪 Testing

### Run All Tests

```bash
pytest tests/ -v
```

### Run Specific Test Suite

```bash
pytest tests/test_history_store.py -v
pytest tests/test_comparison_engine.py -v
pytest tests/test_transform.py -v
```

### Test Coverage

- **History Store**: Snapshot saving, price change detection, alert generation
- **Comparison Engine**: Product matching, normalization, feature extraction, pivot tables
- **Transform Engine**: Rule application, product parsing, column manipulation
- **Extraction**: Connector factory, source type normalization

## 📊 Audit Trail

Access audit logs in `code_log/audit/audit.jsonl`:

```json
{"timestamp": "2026-05-06T10:30:45.123456", "operation_type": "extract", "status": "success", "details": {"source_type": "playwright", "rows_extracted": 150}, "duration": 12.5}
```

View audit statistics:

```python
from src.audit import AuditLogger

audit = AuditLogger()
print(audit.get_stats(hours=24))
# Output:
# {
#     "total_operations": 45,
#     "successful": 42,
#     "failed": 3,
#     "by_operation": {
#         "extract": 15,
#         "transform": 15,
#         "load": 15
#     }
# }
```

## 🔍 Troubleshooting

### Common Issues

**Issue**: Playwright timeout error

```
Error: Timeout waiting for selector
```

**Solution**:
- Increase wait time in connector
- Verify CSS selector is correct
- Check target website loads properly

**Issue**: Memory usage too high

```
MemoryError: Unable to allocate X GiB
```

**Solution**:
- Use `drop_nulls` rule to reduce data
- Process data in batches
- Filter data before transform

**Issue**: No data extracted

```
Exception: No data extracted
```

**Solution**:
- Check URL is accessible
- Verify CSS selector matches actual page structure
- Use browser dev tools to inspect elements
- Try different scraping mode

### Debug Mode

Enable verbose logging:

```python
import logging

logging.basicConfig(level=logging.DEBUG)
pipeline.run(...)
```

Check pipeline logs:

```bash
tail -f code_log/pipeline.log
```

View audit trail:

```bash
tail -f code_log/audit/audit.jsonl
```

## 📈 Performance Tips

1. **Use CSV for large datasets** - Faster than database insert
2. **Filter data early** - Apply filters before transform
3. **Reuse connectors** - Create once, use multiple times
4. **Batch processing** - Process data in chunks for large sources
5. **Monitor memory** - Check DataFrame memory usage

## 🤝 Contributing

1. Create a feature branch
2. Write tests for new functionality
3. Ensure all tests pass
4. Submit pull request

## 📝 License

MIT License - See LICENSE file for details

## 📞 Support

For issues and questions:
1. Check [Troubleshooting](#troubleshooting) section
2. Review audit logs in `code_log/audit/`
3. Run test suite to identify problems
4. Check recent pipeline logs

---

**Last Updated**: May 6, 2026
**Version**: 2.0
