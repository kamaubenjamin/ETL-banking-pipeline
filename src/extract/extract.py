from src.extract.web_scraper import WebScraperConnector
from src.extract.file_loader import CSVConnector
from src.extract.api_connector import APIConnector
from src.extract.dataframe_connector import DataFrameConnector
from src.extract.selenium_connector import SeleniumConnector
from src.extract.playwright_connector import PlaywrightConnector
# Factory function to get the appropriate connector based on the source type specified by the user.
def get_connector(source_type, config=None, uploaded_df=None, mode=None, selector=None):

    source_type = source_type.strip().lower()   # 🔥 normalize everything

    if source_type == "default (web)":
        return WebScraperConnector(config.url, mode, selector)

    elif source_type == "csv":
        return CSVConnector(config.csv_path)

    elif source_type == "upload dataset":
        if uploaded_df is None:
            raise ValueError("No uploaded dataframe found")
        return DataFrameConnector(uploaded_df)

    elif source_type == "api":
        return APIConnector(config.api_url)

    elif source_type == "selenium":
        return SeleniumConnector(
            url=config.url,
            selector=selector,
            keyword=getattr(config, "keyword", None)
        )

    elif source_type == "playwright":
        return PlaywrightConnector(
            url=config.url,
            selector=selector,
            keyword=getattr(config, "keyword", None)
        )

    else:
        raise Exception(f"Unsupported source type: {repr(source_type)}")


def run_extraction(source_type, config, uploaded_df=None, mode=None, selector=None):
    connector = get_connector(
        source_type=source_type,
        config=config,
        uploaded_df=uploaded_df,
        mode=mode,
        selector=selector
    )
    print("RUNNING WITH SOURCE:", repr(source_type))
    return connector.extract()