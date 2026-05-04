import pandas as pd
from src.storage.history_store import detect_price_changes
from src.alerts.alert_engine import generate_alerts

df = pd.read_csv("price_history.csv")

changes = detect_price_changes(df)

alerts = generate_alerts(changes)

print("\n=== ALERTS ===\n")
for a in alerts:
    print(a)