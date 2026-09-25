

import pandas as pd
import os

DATA_FILE = os.path.join(os.path.dirname(__file__), "..", "data", "orders_75k.csv")
df = pd.read_csv(DATA_FILE)

print("=== RAW (broken) data (first 15 rows) ===")
print(df.head(15))
print()

Nan_count = df.isnull().sum()
print("\nNan count", Nan_count)

df["customer_type"] =df["customer_type"].fillna("Unknown")

df = df.dropna()

print("\nCleaned data first 15 rows:\n", df.head(15))
print("\nRows survived:\n", {len(df)})
print("\nRemaining Nans:\n", df.isnull().sum())

OUTPUT_FILE = os.path.join(os.path.dirname(__file__), "clean_orders.csv")
df.to_csv(OUTPUT_FILE, index=False)

print("\nDone! Check for clean_orders.csv in your folder.")