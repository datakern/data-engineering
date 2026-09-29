"""
Solution 5 — Pandas: Filter, Group & Summarise
"""

import pandas as pd
import os

# Load the 25,000 row dataset
DATA_FILE = os.path.join(os.path.dirname(__file__), "..", "data", "orders_75k.csv")
df = pd.read_csv(DATA_FILE)
df["revenue"] = df["quantity"] * df["unit_price"]

# Task A — unit_price > 100
print("=== Task A: unit_price > 100 ===")
print(df[df["unit_price"] > 100].head(10))

# Task B — Premium customers only
print("\n=== Task B: Premium only ===")
print(df[df["customer_type"] == "Premium"].head(10))

# Task C — unit_price > 50 AND not Regular
print("\n=== Task C: price > 50 AND not Regular ===")
mask = (df["unit_price"] > 50) & (df["customer_type"] != "Regular")
print(df[mask].head(10))

# Task D — total revenue per customer type
print("\n=== Task D: total revenue by customer_type ===")
print(df.groupby("customer_type")["revenue"].sum())

# Task E — average unit_price per product
print("\n=== Task E: average unit_price by product ===")
print(df.groupby("product")["unit_price"].mean())
