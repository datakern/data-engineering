"""
╔══════════════════════════════════════════════════════════════╗
║  Exercise 06 — Clean the Mess, Save the Result              ║
║  Topic: NaN · fillna · dropna · to_csv                      ║
╚══════════════════════════════════════════════════════════════╝

Real-world story
────────────────
The orders data you received is straight from production.
Sensors failed. Users skipped fields. Systems crashed.

Some cells are completely empty — Pandas marks these as NaN
(Not a Number). You CANNOT do math on NaN. Left alone, it
will silently break your entire analysis.

Your job: diagnose the damage, fix what you can, drop what you
can't, and save the clean result to a file.

────────────────────────────────────────────────────────────────
Thinking hints  💡
────────────────────────────────────────────────────────────────
  Step 1 — Find the holes:
    → df.isnull().sum()   tells you how many NaNs per column

  Step 2 — Fill what makes sense:
    → df["col"].fillna("some default")   fills with a fixed value
    → df["col"].fillna(df["col"].mean()) fills with the column average
    ⚠️  Remember to assign back:  df["col"] = df["col"].fillna(...)

  Step 3 — Drop what can't be fixed:
    → df = df.dropna()   removes any row that STILL has a NaN

  Step 4 — Export:
    → df.to_csv("filename.csv", index=False)
    → index=False stops Pandas from writing the row numbers into the file

────────────────────────────────────────────────────────────────
Work through each task in order — they build on each other.
────────────────────────────────────────────────────────────────
"""

import pandas as pd
import os

# ── Messy data — do NOT change ────────────────────────────────────────────────

# Load the 25,000 row dataset
DATA_FILE = os.path.join(os.path.dirname(__file__), "..", "data", "orders_25k.csv")
df = pd.read_csv(DATA_FILE)

print("=== RAW (broken) data (first 15 rows) ===")
print(df.head(15))
print()

# ─────────────────────────────────────────────────────────────────────────────
# Task A — Diagnose
#   Count how many NaN values exist in each column.
#   Which columns have missing data?
# ─────────────────────────────────────────────────────────────────────────────
print("Task A — NaN count per column:")
# YOUR CODE HERE


# ─────────────────────────────────────────────────────────────────────────────
# Task B — Fill missing customer_type
#   If we don't know who the customer is, label them "Unknown".
#   Hint: fillna("Unknown")
# ─────────────────────────────────────────────────────────────────────────────
# YOUR CODE HERE


# ─────────────────────────────────────────────────────────────────────────────
# Task C — Fill missing unit_price
#   Use the average price of the unit_price column as a best guess.
#   Hint: df["unit_price"].mean() gives you the average
# ─────────────────────────────────────────────────────────────────────────────
# YOUR CODE HERE


# ─────────────────────────────────────────────────────────────────────────────
# Task D — Drop what's still broken
#   After B and C, one row still has a missing product name.
#   Drop any row that still contains a NaN.
# ─────────────────────────────────────────────────────────────────────────────
# YOUR CODE HERE


# ─────────────────────────────────────────────────────────────────────────────
# Task E — Inspect the clean result
#   Print the final DataFrame.
#   How many rows survived? Does the data look healthy?
# ─────────────────────────────────────────────────────────────────────────────
print("\nTask E — Clean data (first 15 rows):")
# YOUR CODE HERE


# ─────────────────────────────────────────────────────────────────────────────
# Task F — Save to file
#   Export your clean DataFrame to  clean_orders.csv
#   Open the file and check it looks right. No row numbers in the file!
# ─────────────────────────────────────────────────────────────────────────────
# YOUR CODE HERE
print("\nDone! Check for clean_orders.csv in your folder.")
