import pandas as pd
import re
from rapidfuzz import fuzz


# -----------------------------
# 🔗 COMBINE DATASETS
# -----------------------------
def combine_datasets(datasets: dict) -> pd.DataFrame:
    combined = []

    for source, df in datasets.items():
        temp = df.copy()
        temp["source"] = source
        combined.append(temp)

    return pd.concat(combined, ignore_index=True)


# -----------------------------
# 🧠 NORMALIZE NAME
# -----------------------------
def normalize_name(text: str) -> str:
    text = text.lower()

    # remove punctuation
    text = re.sub(r"[^\w\s]", "", text)

    # normalize units
    text = text.replace("inch", "").replace('"', "")

    # remove noise words
    stop_words = ["smart", "android", "tv", "led", "qled"]
    words = text.split()
    words = [w for w in words if w not in stop_words]

    return " ".join(words)


# -----------------------------
# 🏷️ BRAND EXTRACTION
# -----------------------------
def extract_brand(text: str) -> str:
    brands = ["vitron", "amtec", "vision", "samsung", "lg", "sony", "hisense"]

    text = text.lower()

    for b in brands:
        if b in text:
            return b

    return text.split()[0] if text.split() else None


# -----------------------------
# 📦 CATEGORY DETECTION (GENERIC)
# -----------------------------
def detect_category(name: str) -> str:
    text = name.lower()

    keywords = {
        "electronics": ["tv", "speaker", "woofer", "headphone", "earphone"],
        "wearables": ["watch", "smartwatch"],
        "accessories": ["cable", "charger", "adapter"],
        "tools": ["glue", "repair", "tool"],
    }

    for category, words in keywords.items():
        for w in words:
            if w in text:
                return category

    return "other"


# -----------------------------
# 🧩 FEATURE EXTRACTION
# -----------------------------
def extract_features(name: str) -> dict:
    text = name.lower()

    brand = extract_brand(text)

    # size (e.g. 43", 50 inch)
    size_match = re.search(r"(\d{2})\s?(?:\"|inch)", text)
    size = size_match.group(1) if size_match else None

    # model (flexible alphanumeric)
    model_match = re.search(r"\b[a-z0-9]{4,}\b", text)
    model = model_match.group(0) if model_match else None

    category = detect_category(text)

    return {
        "brand": brand,
        "size": size,
        "model": model,
        "category": category
    }


# -----------------------------
# 🔍 MATCH PRODUCTS
# -----------------------------
def match_products(df: pd.DataFrame, threshold: int = 70) -> pd.DataFrame:
    df = df.copy().reset_index(drop=True)
    df["match_id"] = -1

    match_id = 0

    for i in range(len(df)):
        if df.loc[i, "match_id"] != -1:
            continue

        df.loc[i, "match_id"] = match_id

        base_raw = df.loc[i, "product_name"]
        base_norm = normalize_name(base_raw)
        features_i = extract_features(base_raw)

        for j in range(i + 1, len(df)):
            if df.loc[j, "match_id"] != -1:
                continue

            compare_raw = df.loc[j, "product_name"]
            compare_norm = normalize_name(compare_raw)
            features_j = extract_features(compare_raw)

            # -----------------------------
            # 🚫 CATEGORY FILTER (light, generic)
            # -----------------------------
            if features_i["category"] != features_j["category"]:
                continue

            score = 0

            # -----------------------------
            # 🔥 FEATURE MATCHING
            # -----------------------------
            if features_i["brand"] and features_i["brand"] == features_j["brand"]:
                score += 30

            if features_i["size"] and features_i["size"] == features_j["size"]:
                score += 30

            # flexible model match
            if features_i["model"] and features_j["model"]:
                if (
                    features_i["model"] in features_j["model"]
                    or features_j["model"] in features_i["model"]
                ):
                    score += 30

            # -----------------------------
            # 🔁 FUZZY MATCH
            # -----------------------------
            fuzzy_score = fuzz.token_set_ratio(base_norm, compare_norm)

            final_score = score + (fuzzy_score * 0.7)

            if final_score >= threshold:
                df.loc[j, "match_id"] = match_id

        match_id += 1

    return df


# -----------------------------
# 💰 BUILD COMPARISON TABLE
# -----------------------------
def build_comparison_table(df: pd.DataFrame) -> pd.DataFrame:
    pivot = df.pivot_table(
        index="match_id",
        columns="source",
        values="price",
        aggfunc="first"
    )

    pivot = pivot.reset_index()

    names = df.groupby("match_id")["product_name"].first().reset_index()

    result = pivot.merge(names, on="match_id")

    # reorder columns
    result = result[
        ["product_name"]
        + [c for c in result.columns if c not in ["product_name", "match_id"]]
    ]

    # -----------------------------
    # 🏆 FIND CHEAPEST
    # -----------------------------
    source_cols = [col for col in result.columns if col != "product_name"]

    def find_cheapest(row):
        prices = {col: row[col] for col in source_cols if pd.notna(row[col])}
        if not prices:
            return None
        return min(prices, key=prices.get)

    result["cheapest"] = result.apply(find_cheapest, axis=1)

    return result