"""
DataKern — Session 5 Demo
Script 04: Window Functions Introduction — Calculations Without Collapsing Grain

What you will learn:
  1. The core mental model: Why groupBy collapses grain, while Window preserves every row
  2. How to construct a Window specification (partitionBy and orderBy)
  3. Practical Pattern 1: Running cumulative revenue over time
  4. Practical Pattern 2: Ranking top items within categories (row_number vs rank)
  5. How this builds directly into your upcoming Assignment 5

Prerequisites:
  - Spark running locally
  - Olist datasets in demo/data/ (or symlinked)

Run from demo/ folder:
  python scripts/04_window_functions_intro.py
"""

import os
import sys
import time

# Prevent macOS binding errors
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ─────────────────────────────────────────────────────────────────
# SECTION 0 — Configuration & Helper Functions
# ─────────────────────────────────────────────────────────────────

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
ITEMS_CSV = os.path.join(DATA_DIR, "olist_order_items_dataset.csv")
ORDERS_CSV = os.path.join(DATA_DIR, "olist_orders_dataset.csv")
PRODUCTS_CSV = os.path.join(DATA_DIR, "olist_products_dataset.csv")

DIVIDER = "═" * 72
SUB_DIVIDER = "─" * 72


def pause():
    """Pauses execution to allow interactive exploration in the terminal and Spark UI."""
    input("\n🔍 Press ENTER to continue ➔ ")
    print("\n" + SUB_DIVIDER)


# ─────────────────────────────────────────────────────────────────
# SECTION 1 — Start the SparkSession
# ─────────────────────────────────────────────────────────────────

print("\n" + DIVIDER)
print("  DATAKERN SESSION 5: SCRIPT 04 — WINDOW FUNCTIONS INTRO")
print(DIVIDER)
print("""
THE FUNDAMENTAL MENTAL MODEL:
- groupBy:
    Collapses multiple rows into ONE summary row per group.
    (Analogy: Writing the class average on the whiteboard. Individual students disappear!)
- Window:
    Calculates an aggregation or rank ACROSS a group, but ATTACHES it to each row.
    (Analogy: Handing each student their report card showing their own score AND class rank.
     Every student row is preserved!)
""")

spark = SparkSession.builder \
    .appName("DataKern_Session5_04_Window_Functions") \
    .master("local[*]") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.ui.port", "4040") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

cores_available = spark.sparkContext.defaultParallelism

print(f"✅ SparkSession ready.")
print(f"   Master: {spark.sparkContext.master} ({cores_available} CPU cores detected on your laptop)")
print("   Task counts and partition metrics in the Spark UI will reflect this hardware configuration.")
print("   Spark UI active at: http://localhost:4040")

pause()

# ─────────────────────────────────────────────────────────────────
# SECTION 2 — Prepare Joined Dataset
# ─────────────────────────────────────────────────────────────────

print("SECTION 2: Preparing Order Items with Categories and Timestamps\n")

df_items = spark.read.csv(ITEMS_CSV, header=True, inferSchema=True) \
    .select("order_id", "product_id", "price")

df_orders = spark.read.csv(ORDERS_CSV, header=True, inferSchema=True) \
    .filter(F.col("order_status") == "delivered") \
    .select("order_id", "order_purchase_timestamp")

df_products = spark.read.csv(PRODUCTS_CSV, header=True, inferSchema=True) \
    .select("product_id", "product_category_name")

# Join with Broadcast for speed
df_base = df_items \
    .join(df_orders, on="order_id", how="inner") \
    .join(F.broadcast(df_products), on="product_id", how="inner") \
    .filter(F.col("product_category_name").isNotNull())

base_count = df_base.count()
print(f"✅ Base dataset prepared: {base_count:,} item transactions.")
print("   Sample rows:")
df_base.select("order_id", "product_category_name", "price", "order_purchase_timestamp").show(5, truncate=False)

pause()

# ─────────────────────────────────────────────────────────────────
# SECTION 3 — groupBy vs Window Side by Side
# ─────────────────────────────────────────────────────────────────

print("SECTION 3: Seeing groupBy vs Window Side by Side\n")

print("1. THE GROUPBY APPROACH:")
df_groupby = df_base.groupBy("product_category_name").agg(
    F.count("order_id").alias("item_count"),
    F.sum("price").alias("total_revenue")
)
print("   Notice the row count after groupBy:")
print(f"   Original rows: {base_count:,} ➔ GroupBy output rows: {df_groupby.count():,}")
df_groupby.show(3)

print("\n2. THE WINDOW APPROACH:")
print("   Now suppose you want each item transaction to show its OWN price,")
print("   PLUS the total revenue of that entire product category right beside it!")

# Define a Window partition
w_category = Window.partitionBy("product_category_name")

df_with_window = df_base.withColumn(
    "category_total_revenue",
    F.sum("price").over(w_category)
).withColumn(
    "pct_of_category_revenue",
    F.round((F.col("price") / F.col("category_total_revenue")) * 100, 2)
)

print(f"   Original rows: {base_count:,} ➔ Window output rows: {df_with_window.count():,}")
print("   EVERY ORIGINAL ROW IS PRESERVED!")
df_with_window.select(
    "order_id", "product_category_name", "price", "category_total_revenue", "pct_of_category_revenue"
).show(5, truncate=False)

print("\n   👀 OBSERVE IN SPARK UI (http://localhost:4040):")
print("   Check the 'SQL / DataFrame' and 'Stages' tabs:")
print("   Notice that Window.partitionBy('product_category_name') creates an Exchange boundary!")
print("   Spark redistributes rows so all items of the same category reside on the same worker partition,")
print("   where the WindowExec operator evaluates the window function locally in memory.")

pause()

# ─────────────────────────────────────────────────────────────────
# SECTION 4 — Window Pattern 1: Running Cumulative Revenue
# ─────────────────────────────────────────────────────────────────

print("SECTION 4: Practical Pattern 1 — Running Cumulative Total Over Time\n")
print("""
BUSINESS QUESTION:
As orders arrive throughout the year, what is the cumulative revenue accumulated
within each category up to that exact transaction?

HOW TO BUILD THE WINDOW:
1. partitionBy("product_category_name")  ➔ Calculate separately for each category.
2. orderBy("order_purchase_timestamp")   ➔ Order chronologically.
3. rowsBetween(unboundedPreceding, currentRow) ➔ Sum from the beginning of time up to this row.
""")

w_running = Window \
    .partitionBy("product_category_name") \
    .orderBy("order_purchase_timestamp") \
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

df_running = df_base.withColumn(
    "cumulative_category_revenue",
    F.round(F.sum("price").over(w_running), 2)
)

print("Sample Output: Running Revenue for 'telefonia' Category:")
df_running \
    .filter(F.col("product_category_name") == "telefonia") \
    .select("product_category_name", "order_purchase_timestamp", "price", "cumulative_category_revenue") \
    .show(10, truncate=False)

print("""
🔎 NOTICE:
Look at the 'cumulative_category_revenue' column:
Each row adds its 'price' to the running balance of the previous row!
In plain SQL, doing this without window functions requires a self-join that kills performance.
In Spark, Window executes in a single streaming scan per partition!
""")

pause()

# ─────────────────────────────────────────────────────────────────
# SECTION 5 — Window Pattern 2: Ranking Items within Categories
# ─────────────────────────────────────────────────────────────────

print("SECTION 5: Practical Pattern 2 — Top Items per Category (Ranking)\n")
print("""
BUSINESS QUESTION:
What are the top 3 most expensive items ever sold in each product category?

HOW TO BUILD THE WINDOW:
1. partitionBy("product_category_name")
2. orderBy(F.desc("price"))
3. F.row_number().over(w)  ➔ Assigns ranks 1, 2, 3...
""")

w_rank = Window \
    .partitionBy("product_category_name") \
    .orderBy(F.desc("price"))

df_ranked = df_base.withColumn("item_price_rank", F.row_number().over(w_rank))

print("Top 3 most expensive items in 'audio' and 'relogios_presentes':")
df_ranked \
    .filter(F.col("product_category_name").isin("audio", "relogios_presentes")) \
    .filter(F.col("item_price_rank") <= 3) \
    .select("product_category_name", "item_price_rank", "price", "order_id") \
    .show(6, truncate=False)

print("""
🔎 DIFFERENCE BETWEEN RANK FUNCTIONS:
- F.row_number(): Always generates unique sequential numbers (1, 2, 3, 4).
- F.rank()      : Leaves gaps on ties (1, 2, 2, 4).
- F.dense_rank(): No gaps on ties (1, 2, 2, 3).
""")

pause()

# ─────────────────────────────────────────────────────────────────
# SECTION 6 — Clean Production Reference & Assignment 5 Preview
# ─────────────────────────────────────────────────────────────────

print("SECTION 6: Clean Production Reference Code & Assignment 5 Preview\n")
print("""
# ─────────────────────────────────────────────────────────────────
# WINDOW FUNCTIONS SYNTAX CHEATSHEET:
# ─────────────────────────────────────────────────────────────────
from pyspark.sql.window import Window
from pyspark.sql import functions as F

# 1. Spec definition:
w = Window.partitionBy("category").orderBy("timestamp")

# 2. Running Total:
df = df.withColumn("running_total", F.sum("amount").over(
    w.rowsBetween(Window.unboundedPreceding, Window.currentRow)
))

# 3. Ranking:
w_rank = Window.partitionBy("category").orderBy(F.desc("amount"))
df_top = df.withColumn("rnk", F.row_number().over(w_rank)).filter(F.col("rnk") <= 3)

# 4. Lead / Lag (Preview for Assignment 5):
# F.lag("price", 1).over(w) ➔ Gets the price of the PREVIOUS row (used for Month-over-Month growth!)

# ─────────────────────────────────────────────────────────────────
# 🎯 ASSIGNMENT 5 PREVIEW:
# In Assignment 5, you will use Window functions to:
#   1. Calculate Month-over-Month growth percentages using F.lag()
#   2. Compute customer repeat purchase gaps
#   3. Rank customer lifetime value
# ─────────────────────────────────────────────────────────────────
""")

pause()

# ─────────────────────────────────────────────────────────────────
# SECTION 7 — Summary & Handoff
# ─────────────────────────────────────────────────────────────────

print("SECTION 7: Summary & What's Next\n")
print("""
🎉 SCRIPT 04 COMPLETE!
What you have seen with your own eyes:
  ✅ groupBy collapses rows; Window preserves every row while computing group metrics.
  ✅ Window.partitionBy().orderBy() enables running totals in a single pass.
  ✅ Ranking functions (row_number, rank) isolate top-N items per group without self-joins.

👉 NEXT UP:
Run Script 05 to see why modern big data abandoned CSV: Parquet Anatomy, Column Pruning, 
and directory-based Partitioning!

Command to run:
  python scripts/05_parquet_anatomy_and_partitioning.py
""")

spark.stop()
print("SparkSession stopped cleanly.")
