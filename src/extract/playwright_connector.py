from playwright.sync_api import sync_playwright
import pandas as pd
from src.extract.base_connector import BaseConnector


class PlaywrightConnector(BaseConnector):

    def __init__(self, url, selector=None, keyword=None):
        super().__init__()
        self.url = url
        self.selector = selector
        self.keyword = keyword

    def extract(self):

        if not self.url:
            raise Exception("URL is required")

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()

            page.goto(self.url)
            page.wait_for_timeout(8000)  # allow JS load

            # fallback to body if no selector
            target_selector = self.selector if self.selector else "body"

            elements = page.query_selector_all(target_selector)

            data = []

            for el in elements:
                try:
                    text = el.inner_text().strip()

                    if not text:
                        continue

                    if self.keyword:
                        if self.keyword.lower() not in text.lower():
                            continue

                    data.append(text)

                except:
                    continue

            browser.close()

        if not data:
            raise Exception("No data extracted")

        df = pd.DataFrame({"content": data})

        return self.validate_output(df)