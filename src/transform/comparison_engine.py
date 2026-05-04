import pandas as pd
import re
from rapidfuzz import fuzz

def combine_datasets(datasets: dict) -> pd.DataFrame:
    """
    Combine multiple datasets into one with source labels

    datasets = {
        "jumia": df1,
        "kilimall": df2
    }
    """

    combined = []

    for source, df in datasets.items():
        temp = df.copy()
        temp["source"] = source
        combined.append(temp)

    return pd.concat(combined, ignore_index=True)

# Group similar products based on name similarity

def match_products(df: pd.DataFrame, threshold: int = 75) -> pd.DataFrame:
    """
    Improved product matching using token_set_ratio
    """

    df = df.copy().reset_index(drop=True)
    df["match_id"] = -1

    match_id = 0

    for i in range(len(df)):
        if df.loc[i, "match_id"] != -1:
            continue

        df.loc[i, "match_id"] = match_id
        base_name = normalize_name(df.loc[i, "product_name"])


        for j in range(i + 1, len(df)):
            if df.loc[j, "match_id"] != -1:
                continue

            compare_name = normalize_name(df.loc[j, "product_name"])

            score = fuzz.token_set_ratio(base_name, compare_name)

            if score >= threshold:
                df.loc[j, "match_id"] = match_id

        match_id += 1

    return df

def build_comparison_table(df: pd.DataFrame) -> pd.DataFrame:
    """
    Build comparison table across sources using match_id
    """

    # Pivot table: rows = match_id, columns = source
    pivot = df.pivot_table(
        index="match_id",
        columns="source",
        values="price",
        aggfunc="first"
    )

    pivot = pivot.reset_index()

    # Add representative product name
    names = df.groupby("match_id")["product_name"].first().reset_index()

    result = pivot.merge(names, on="match_id")

    # Reorder columns
    cols = ["product_name"] + [col for col in pivot.columns if col != "match_id"]
    result = result[["product_name"] + [c for c in result.columns if c not in ["product_name", "match_id"]]]

    # -----------------------------
    # 💰 FIND CHEAPEST SOURCE
    # -----------------------------
    source_cols = [col for col in result.columns if col not in ["product_name"]]

    def find_cheapest(row):
        prices = {col: row[col] for col in source_cols if pd.notna(row[col])}
        if not prices:
            return None
        return min(prices, key=prices.get)

    result["cheapest"] = result.apply(find_cheapest, axis=1)

    return result



def normalize_name(text: str) -> str:
    text = text.lower()

    # remove quotes and symbols
    text = re.sub(r'["\'(),]', '', text)

    # normalize inch formats
    text = text.replace('inch', '').replace('"', '')

    # remove common noise words
    stop_words = ['smart', 'android', 'tv', 'inch', 'led', 'qled']
    words = text.split()

    words = [w for w in words if w not in stop_words]

    return " ".join(words)
def extract_features(name: str) -> dict:
    import re

    text = name.lower()

    # brand (first word usually)
    brand = text.split()[0]

    # size (e.g. 43", 50 inch)
    size_match = re.search(r'(\d{2})\s?(?:\"|inch)', text)
    size = size_match.group(1) if size_match else None

    # model (alphanumeric codes)
    model_match = re.search(r'\b[a-z0-9]{4,}\b', text)
    model = model_match.group(0) if model_match else None

    return {
        "brand": brand,
        "size": size,
        "model": model
    }