import pandas as pd
from datetime import datetime
import os


# -----------------------------
# 💾 SAVE SNAPSHOT
# -----------------------------
def save_snapshot(df: pd.DataFrame, filename="price_history.csv"):
    df = df.copy()
    df["timestamp"] = datetime.now()

    if os.path.exists(filename):
        existing = pd.read_csv(filename)
        df = pd.concat([existing, df], ignore_index=True)

    df.to_csv(filename, index=False)


# -----------------------------
# 🔍 DETECT PRICE CHANGES
# -----------------------------
def detect_price_changes(df: pd.DataFrame):
    df = df.sort_values("timestamp")

    changes = []

    for product in df["product_name"].unique():
        product_df = df[df["product_name"] == product]

        if len(product_df) < 2:
            continue

        latest = product_df.iloc[-1]
        previous = product_df.iloc[-2]

        for source in ["jumia", "kilimall"]:
            if source in df.columns:
                if latest[source] != previous[source]:
                    changes.append({
                        "product": product,
                        "source": source,
                        "old_price": previous[source],
                        "new_price": latest[source]
                    })

    return pd.DataFrame(changes)