import pandas as pd
from src.transform.product_parser import extract_product_info

# 🔥 Simulated Kenyan-style data
df = pd.DataFrame({
    "content": [
        "Milk 500ml KSh 65 In stock",
        "Bread KES 120 Out of stock",
        "Sugar 2kg KSh 1,299 In stock"
    ]
})

# Run parser
parsed = extract_product_info(df)

print(parsed)