
import pandas as pd

import os

DATA_FILE = os.path.join(os.path.dirname(__file__), "..", "data", "orders_75k.csv")
df = pd.read_csv(DATA_FILE)

print("Task A:")
print(df.head(3))