import re
import pandas as pd


def extract_product_info(df: pd.DataFrame) -> pd.DataFrame:
    """
    Parser V3: Handles multiple currencies and avoids quantity noise (e.g. 500ml, 2kg)
    """

    results = []

    for row in df.iloc[:, 0]:
        text = str(row)

        # -----------------------------
        # 💰 PRICE + CURRENCY (FIXED LOGIC)
        # -----------------------------
        price = None
        currency = None

        # PRIORITY: Match currency-based price
        price_match = re.search(
            r"(£|\$|KSh|KES)\s?([\d,]+(?:\.\d+)?)",
            text,
            re.IGNORECASE
        )

        if price_match:
            currency = price_match.group(1).upper()

            raw_price = price_match.group(2).replace(",", "")

            try:
                price = float(raw_price)
            except:
                price = None

        else:
            # Fallback: only if no currency found
            fallback_match = re.search(r"([\d,]+(?:\.\d+)?)", text)

            if fallback_match:
                try:
                    price = float(fallback_match.group(1).replace(",", ""))
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

        # Remove common junk
        junk_words = [
            "add to basket",
            "buy now",
            "shop now",
            "view product"
        ]

        for word in junk_words:
            name = name.replace(word, "").strip()

        # Avoid weak rows
        if len(name) < 3:
            continue

        results.append({
            "product_name": name,
            "price": price,
            "currency": currency,
            "availability": availability
        })

    return pd.DataFrame(results)