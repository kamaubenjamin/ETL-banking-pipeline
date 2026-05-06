import pandas as pd


def generate_alerts(changes_df: pd.DataFrame):
    """
    Convert price changes into actionable SME alerts
    """

    alerts = []

    if changes_df.empty:
        return ["No price changes detected"]

    for _, row in changes_df.iterrows():

        # 🔥 UNDERCUT DETECTION (most important signal)
        if row["new_price"] < row["old_price"]:
            message = (
                f"🚨 UNDERCUT ALERT | {row['source']} lowered {row['product']} "
                f"from {row['old_price']} → {row['new_price']}"
            )

        # 📈 PRICE INCREASE
        else:
            message = (
                f"⬆️ PRICE INCREASE | {row['product']} | {row['source']} | "
                f"{row['old_price']} → {row['new_price']}"
            )

        alerts.append(message)

    return alerts