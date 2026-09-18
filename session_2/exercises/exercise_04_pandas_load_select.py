"""
╔══════════════════════════════════════════════════════════════╗
║  Exercise 04 — Your First DataFrame                         ║
║  Topic: pd.read_csv · .head() · .info() · loc / iloc        ║
╚══════════════════════════════════════════════════════════════╝

Real-world story
────────────────
Three months have passed. Your OOP system has been running.
Thousands of orders have come in, and someone exported them to a CSV.

You've just been handed that file. Your mission:
load it into Pandas and start exploring what's inside.

The data is already embedded below — no file needed,
just run the script and start working on each task.

────────────────────────────────────────────────────────────────
Thinking hints  💡
────────────────────────────────────────────────────────────────
  • .head(n)        → see the first n rows
  • .info()         → see column names, types, and how many values
  • df["column"]    → grab one column (you get back a Series)
  • df[["col1","col2"]] → grab multiple columns (stays a DataFrame)
  • .loc[label]     → find a row by its index LABEL (the name)
  • .iloc[number]   → find a row by its position NUMBER (0, 1, 2…)

  Remember from class:
    loc  → Label  (like finding a folder by its name)
    iloc → Integer (like counting folders from the top)

────────────────────────────────────────────────────────────────
Complete each task. Print the result to see what you get.
────────────────────────────────────────────────────────────────
"""

import pandas as pd
import io

# ── Data — do NOT change ──────────────────────────────────────────────────────

RAW_CSV = """order_id,customer_type,product,quantity,unit_price
ORD001,Regular,Laptop,1,999.99
ORD002,Premium,Mouse,3,29.99
ORD003,Corporate,Keyboard,10,49.99
ORD004,Regular,Monitor,2,299.99
ORD005,Premium,Laptop,1,999.99
ORD006,Corporate,Mouse,5,29.99
ORD007,Regular,Keyboard,2,49.99
ORD008,Premium,Monitor,1,299.99
"""

df = pd.read_csv(io.StringIO(RAW_CSV))

# ─────────────────────────────────────────────────────────────────────────────
# Task A — Preview the data
#   Show just the first 3 rows.
#   Hint: df.head(...)
# ─────────────────────────────────────────────────────────────────────────────
print("Task A:")
# YOUR CODE HERE


# ─────────────────────────────────────────────────────────────────────────────
# Task B — Understand the structure
#   Print the column names, their types, and non-null counts.
#   Hint: df.info()
# ─────────────────────────────────────────────────────────────────────────────
print("\nTask B:")
# YOUR CODE HERE


# ─────────────────────────────────────────────────────────────────────────────
# Task C — Extract one column
#   Pull out just the 'product' column.
#   Then print its type — is it a Series or a DataFrame?
#   Hint: type(...)
# ─────────────────────────────────────────────────────────────────────────────
print("\nTask C:")
# YOUR CODE HERE


# ─────────────────────────────────────────────────────────────────────────────
# Task D — Row by label (loc)
#   Get the row with index label  2  using .loc
#   What product was ordered in that row?
# ─────────────────────────────────────────────────────────────────────────────
print("\nTask D:")
# YOUR CODE HERE


# ─────────────────────────────────────────────────────────────────────────────
# Task E — Row by position (iloc)
#   Get the 5th row (position 4) using .iloc
#   What customer type placed that order?
# ─────────────────────────────────────────────────────────────────────────────
print("\nTask E:")
# YOUR CODE HERE


# ─────────────────────────────────────────────────────────────────────────────
# Task F — Two columns at once
#   Select both 'product' AND 'unit_price' together.
#   Notice the double brackets — why do you think that's needed?
# ─────────────────────────────────────────────────────────────────────────────
print("\nTask F:")
# YOUR CODE HERE
