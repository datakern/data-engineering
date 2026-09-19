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
import os

# ── Data — do NOT change ──────────────────────────────────────────────────────

# Load the 25,000 row dataset
DATA_FILE = os.path.join(os.path.dirname(__file__), "..", "data", "orders_75k.csv")
df = pd.read_csv(DATA_FILE)

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
