import pandas as pd
from datetime import datetime
import os


# -----------------------------
# 💾 SAVE SNAPSHOT
# -----------------------------
def save_snapshot(df: pd.DataFrame, filename="price_history.csv"):
    df = df.copy()
    df["timestamp"] = datetime.now().isoformat()

    if os.path.exists(filename):
        existing = pd.read_csv(filename)
        if "timestamp" in existing.columns:
            existing["timestamp"] = pd.to_datetime(existing["timestamp"], errors="coerce")
        df = pd.concat([existing, df], ignore_index=True)

    df.to_csv(filename, index=False)


# -----------------------------
# 🔍 DETECT PRICE CHANGES
# -----------------------------
def detect_price_changes(df: pd.DataFrame):
    df = df.copy()

    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
        if df["timestamp"].notna().any():
            df = df.sort_values("timestamp")
        else:
            df = df.reset_index(drop=True)
    else:
        df = df.reset_index(drop=True)

    changes = []

    for product in df["product_name"].unique():
        product_df = df[df["product_name"] == product]

        if len(product_df) < 2:
            continue

        latest = product_df.iloc[-1]
        previous = product_df.iloc[-2]

        for source in df.columns:
            if source in ["product_name", "timestamp"]:
                continue

            old_price = previous.get(source)
            new_price = latest.get(source)

            # 🔥 FIX: ignore NaN comparisons
            if pd.isna(old_price) or pd.isna(new_price):
                continue

            if (not pd.isna(old_price)and not pd.isna(new_price)and old_price != new_price):
                changes.append({
                    "product": product,
                    "source": source,
                    "old_price": old_price,
                    "new_price": new_price
                })

    return pd.DataFrame(changes)