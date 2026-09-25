

import pandas as pd
import os

DATA_FILE = os.path.join(os.path.dirname(__file__), "..", "data", "orders_75k.csv")
df = pd.read_csv(DATA_FILE)

unit_price_over_100 = df[df["unit_price"] > 100]
print("orders over unit price 100:\n", unit_price_over_100)
rows_left= len(df)-len(unit_price_over_100)
print("\nnumber of rows left\n", rows_left)

premium_customers = df[df["customer_type"] == "Premium"]
print("\n Premium customer list\n", premium_customers)

unit_price_over_50_and_customer_type_not_regular = df[(df["unit_price"] > 50) & (df["customer_type"] != "Regular")]
print("\nunit_price_over_50_and_customer_type_not_regular\n", unit_price_over_50_and_customer_type_not_regular)


total_revenue_by_customer_type = df.groupby("customer_type")["unit_price"].sum()
print("\ntotal_revenue_by_customer_type\n", total_revenue_by_customer_type)

average_price_per_product = df.groupby("product")["unit_price"].mean()
print("\naverage_price_per_product", average_price_per_product)