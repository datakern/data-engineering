"""
╔══════════════════════════════════════════════════════════════╗
║  Exercise 05 — Ask the Data Questions                       ║
║  Topic: Boolean Masking · groupby · sum / mean              ║
╚══════════════════════════════════════════════════════════════╝

Real-world story
────────────────
Your manager walks in with three questions:
  1. "Show me all orders over €100."
  2. "Which orders came from Premium customers?"
  3. "What's the total revenue per customer type?"

In Excel, you'd click and filter. In Pandas, you write one line.
That one line works on 10 rows OR 10 million rows — equally fast.

────────────────────────────────────────────────────────────────
Thinking hints  💡
────────────────────────────────────────────────────────────────
Boolean filtering (masking):
  → df[ df["column"] > value ]
  → df[ df["column"] == "some text" ]
  → Combine two conditions:  (condition1) & (condition2)
     ⚠️  Each condition MUST be wrapped in its own parentheses

groupby — the Split-Apply-Combine tool:
  → df.groupby("column_to_group_by")["column_to_measure"].sum()
  → Swap .sum() for .mean() to get the average instead

────────────────────────────────────────────────────────────────
Complete each task. Print the result and read the numbers.
────────────────────────────────────────────────────────────────
"""

import pandas as pd
import os

# ── Data — do NOT change ──────────────────────────────────────────────────────

# Load the 25,000 row dataset
DATA_FILE = os.path.join(os.path.dirname(__file__), "..", "data", "orders_75k.csv")
df = pd.read_csv(DATA_FILE)
df["revenue"] = df["quantity"] * df["unit_price"]   # already done for you ✅


# ─────────────────────────────────────────────────────────────────────────────
# Task A — Filter by price
#   Show only orders where unit_price is greater than 100.
#   How many rows are left?
# ─────────────────────────────────────────────────────────────────────────────
print("Task A — Orders over €100:")
# YOUR CODE HERE


# ─────────────────────────────────────────────────────────────────────────────
# Task B — Filter by customer type
#   Show only rows where customer_type is exactly "Premium".
#   Hint: use  ==  for equality in Pandas, not  is
# ─────────────────────────────────────────────────────────────────────────────
print("\nTask B — Premium customers only:")
# YOUR CODE HERE


# ─────────────────────────────────────────────────────────────────────────────
# Task C — Combine two filters
#   Show orders where  unit_price > 50  AND  customer_type != "Regular"
#   Hint:  !=  means "not equal to"
#   Hint:  wrap each condition in ( ) before joining with  &
# ─────────────────────────────────────────────────────────────────────────────
print("\nTask C — Price > 50 AND not Regular:")
# YOUR CODE HERE


# ─────────────────────────────────────────────────────────────────────────────
# Task D — Total revenue per customer type  (groupby + sum)
#   Who generates the most revenue — Regular, Premium, or Corporate?
#   Expected (approximate):
#     Corporate   649.85
#     Premium    1389.95
#     Regular    1699.95
# ─────────────────────────────────────────────────────────────────────────────
print("\nTask D — Total revenue by customer type:")
# YOUR CODE HERE


# ─────────────────────────────────────────────────────────────────────────────
# Task E — Average price per product  (groupby + mean)
#   Which product has the highest average unit price?
# ─────────────────────────────────────────────────────────────────────────────
print("\nTask E — Average unit price by product:")
# YOUR CODE HERE
