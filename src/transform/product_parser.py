import re
import pandas as pd


def extract_product_info(df: pd.DataFrame) -> pd.DataFrame:
    """
    Parser V3: Handles multiple currencies and formats
    """

    results = []

    for row in df.iloc[:, 0]:
        text = str(row)

        # -----------------------------
        # 💰 PRICE + CURRENCY (IMPROVED)
        # -----------------------------
        price_match = re.search(
            r"(£|\$|KSh|KES)?\s?([\d,]+(?:\.\d+)?)",
            text,
            re.IGNORECASE
        )

        price = None
        currency = None

        if price_match:
            currency = price_match.group(1)

            raw_price = price_match.group(2).replace(",", "")

            try:
                price = float(raw_price)
            except:
                price = None

        # -----------------------------
        # 📦 AVAILABILITY
        # -----------------------------
        lower_text = text.lower()

        if "in stock" in lower_text:
            availability = "In stock"
        elif "out of stock" in lower_text:
            availability = "Out of stock"
        else:
            availability = ""

        # -----------------------------
        # 🏷️ PRODUCT NAME
        # -----------------------------
        name = text

        if price_match:
            name = text[:price_match.start()].strip()

        # Clean junk words
        junk_words = [
            "add to basket",
            "buy now",
            "shop now",
            "view product"
        ]

        for word in junk_words:
            name = name.replace(word, "").strip()

        # Skip bad rows
        if len(name) < 3:
            continue

        results.append({
            "product_name": name,
            "price": price,
            "currency": currency,
            "availability": availability
        })

    return pd.DataFrame(results)