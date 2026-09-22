
import pandas as pd

import os

DATA_FILE = os.path.join(os.path.dirname(__file__), "..", "data", "orders_75k.csv")
df = pd.read_csv(DATA_FILE)

print("First 3 rows:")
print(df.head(3))

print("\nColumn names, their types, and non-null counts:")
df.info()

print("\nProduct Column:")
Products= df.iloc[:,3]
print(type(Products))


Index_Label_2_row= df.loc[2, 'product']
print("\nProduct ordered for index label 2:", Index_Label_2_row)

Customer_type= df.iloc[4,3]
print("\nCustomer type who placed this order:", Customer_type)

product_and_unit_price = df.iloc[:, [3,5]]
print("\nProduct and unit Price", product_and_unit_price)

