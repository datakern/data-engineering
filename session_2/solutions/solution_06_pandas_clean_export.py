"""
Solution 6 — Pandas: Messy Data → Clean Data → Export
"""

import pandas as pd
import os

# Load the 25,000 row dataset
DATA_FILE = os.path.join(os.path.dirname(__file__), "..", "data", "orders_75k.csv")
df = pd.read_csv(DATA_FILE)

print("=== RAW DATA (first 15 rows) ===")
print(df.head(15))
print()

# Task A — count missing values per column
print("=== Task A: NaN counts ===")
print(df.isnull().sum())
print()

# Task B — fill missing customer_type with "Unknown"
df["customer_type"] = df["customer_type"].fillna("Unknown")

# Task C — fill missing unit_price with the column mean
mean_price = df["unit_price"].mean()
df["unit_price"] = df["unit_price"].fillna(mean_price)

# Task D — drop any remaining rows with NaN  (the 'product' column still has one)
df = df.dropna()

# Task E — print the final clean DataFrame
print("=== Task E: CLEAN DATA (first 15 rows) ===")
print(df.head(15))
print()

# Task F — export without the index
df.to_csv("clean_orders.csv", index=False)
print("Saved → clean_orders.csv")
