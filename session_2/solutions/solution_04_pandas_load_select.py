"""
Solution 4 — Pandas: Load, Explore & Select
"""

import pandas as pd
import os

# Load the 25,000 row dataset
DATA_FILE = os.path.join(os.path.dirname(__file__), "..", "data", "orders_75k.csv")
df = pd.read_csv(DATA_FILE)

# Task A — first 3 rows
print("=== Task A: first 3 rows ===")
print(df.head(3))

# Task B — column names and dtypes
print("\n=== Task B: column info ===")
print(df.info())

# Task C — single column → Series
print("\n=== Task C: 'product' Series ===")
products = df["product"]
print(products.head(10))
print(type(products))           # <class 'pandas.core.series.Series'>

# Task D — loc by label
print("\n=== Task D: loc[2] ===")
print(df.loc[2])

# Task E — iloc by position
print("\n=== Task E: iloc[4] (5th row) ===")
print(df.iloc[4])

# Task F — two columns together
print("\n=== Task F: product & unit_price ===")
print(df[["product", "unit_price"]].head(10))
