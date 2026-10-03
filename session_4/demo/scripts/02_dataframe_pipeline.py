"""
DataKern — Session 4 Demo
Script 02: The Finance Pipeline — DataFrame API

What you will learn:
  1. How to chain transformations: filter → join → groupBy → agg
  2. When Spark actually reads the data (hint: not at each transformation)
  3. How grain applies to distributed data (same concept from Session 3)
  4. How to verify what one output row represents

This script answers the same Finance question from Session 3:
  "How is item sales value changing across product categories over time?"
The logic is identical — the engine is now distributed.

Run from the demo/ folder:
  python scripts/02_dataframe_pipeline.py
"""

import os
import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ─────────────────────────────────────────────────────────────────
# SECTION 0 — Configuration
# ─────────────────────────────────────────────────────────────────

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")


def pause():
    input("\n🔍 Press ENTER to continue ➔ ")
    print("\n" + "─" * 70)


# ─────────────────────────────────────────────────────────────────
# SECTION 1 — SparkSession
# ─────────────────────────────────────────────────────────────────

print("\n" + "=" * 70)
print("SECTION 1: Starting SparkSession")
print("=" * 70)

spark = SparkSession.builder \
    .appName("DataKern_Session4_DataFramePipeline") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
print("✅ SparkSession ready.")
print("   OBSERVE: Open your Spark UI at http://localhost:4040")
print("   Keep it open. The Jobs tab should be empty.")

pause()

# ─────────────────────────────────────────────────────────────────
# SECTION 2 — Read source DataFrames
# ─────────────────────────────────────────────────────────────────

print("SECTION 2: Read the source CSVs\n")

print("PREDICT: When spark.read.csv() is called three times below,")
print("will Spark read all three files immediately?")

input("\n🔍 Press ENTER to run the read commands and find out ➔ ")

df_orders = spark.read.csv(
    os.path.join(DATA_DIR, "olist_orders_dataset.csv"),
    header=True,
    inferSchema=True
)

df_items = spark.read.csv(
    os.path.join(DATA_DIR, "olist_order_items_dataset.csv"),
    header=True,
    inferSchema=True
)

df_products = spark.read.csv(
    os.path.join(DATA_DIR, "olist_products_dataset.csv"),
    header=True,
    inferSchema=True
)

print("\n✅ Three DataFrames created.")
print("   OBSERVE: Check the Spark UI. You should see multiple small Jobs.")
print("   Why? Because inferSchema=True forced Spark to peek at the files")
print("   just to guess the column types. But the actual full data has NOT")
print("   been loaded into memory. We are still just building a plan.")

pause()

# ─────────────────────────────────────────────────────────────────
# SECTION 3 — Transformation 1: Filter
# ─────────────────────────────────────────────────────────────────

print("SECTION 3: Filter — keep only delivered orders\n")

# SQL equivalent: WHERE order_status = 'delivered'
# This is a TRANSFORMATION. It adds a step to the plan.
df_delivered = df_orders.filter(F.col("order_status") == "delivered")

print(f"   SQL equivalent : WHERE order_status = 'delivered'")
print(f"   Type returned  : {type(df_delivered)}")
print(f"   Data moved?    : No — still just a plan.")
print()
print("   Notice: the result is still a DataFrame, not a list of rows.")
print("   A DataFrame is always a description of what to compute,")
print("   not the result of having computed it.")

pause()

# ─────────────────────────────────────────────────────────────────
# SECTION 4 — Transformation 2: Add a derived column
# ─────────────────────────────────────────────────────────────────

print("SECTION 4: withColumn — add an order_month column\n")

# SQL equivalent: DATE_TRUNC('month', order_purchase_timestamp)
df_with_month = df_delivered.withColumn(
    "order_month",
    F.date_trunc("month", F.col("order_purchase_timestamp"))
)

print("   SQL equivalent : DATE_TRUNC('month', order_purchase_timestamp)")
print("   What it does   : truncates timestamps to the first day of their month")
print("   Example        : 2017-03-15 08:32 → 2017-03-01 00:00")
print("   Data moved?    : No — this adds another step to the plan.")

pause()

# ─────────────────────────────────────────────────────────────────
# SECTION 5 — Transformation 3: Join three tables
# ─────────────────────────────────────────────────────────────────

print("SECTION 5: Join — order_items to orders and products\n")

print("PREDICT: Think about the grain before looking at the result.")
print("   The grain of df_items is: one row = one product item in one order.")
print("   After joining df_items to delivered orders:")
print("     Will the joined DataFrame have MORE rows than delivered orders?")
print("     Or FEWER? Or the same?")

input("\n🔍 Press ENTER to build the join plan ➔ ")

# SQL equivalent:
#   FROM order_items oi
#   JOIN orders o   ON oi.order_id   = o.order_id
#   JOIN products p ON oi.product_id = p.product_id
#   WHERE o.order_status = 'delivered'
df_joined = df_items \
    .join(df_with_month, on="order_id",   how="inner") \
    .join(df_products,   on="product_id", how="inner")

print("\n   ✅ Join built — still lazy. No data read yet.")

pause()

# ─────────────────────────────────────────────────────────────────
# SECTION 6 — Transformation 4: GroupBy + Aggregate
# ─────────────────────────────────────────────────────────────────

print("SECTION 6: groupBy + agg — the Finance metric\n")

print("PREDICT: What will ONE row in the output represent?")
print("   We are grouping by order_month AND product_category_name.")
print("   So one output row = one month + one product category.")
print("   That is the grain of the curated Finance product —")
print("   the same grain you designed in Assignment 3.")

input("\n🔍 Press ENTER to build the aggregation plan ➔ ")

df_finance = df_joined \
    .groupBy(
        F.col("order_month"),
        F.col("product_category_name")
    ) \
    .agg(
        F.sum("price").alias("item_sales_value"),
        F.countDistinct("order_id").alias("distinct_orders")
    ) \
    .orderBy("order_month", F.desc("item_sales_value"))

print("\n   ✅ Aggregation built. Four transformations total. Still no data read.")
print()
print("   Total transformations so far:")
print("     1. filter(order_status == 'delivered')")
print("     2. withColumn(order_month)")
print("     3. join (order_items + orders + products)")
print("     4. groupBy(month, category).agg(SUM(price))")

pause()

# ─────────────────────────────────────────────────────────────────
# SECTION 7 — ACTION: .show() executes the full pipeline
# ─────────────────────────────────────────────────────────────────

print("SECTION 7: ACTION — .show(10) runs the whole plan at once\n")

print("   Spark will now:")
print("     1. Read all three CSV files")
print("     2. Apply the filter")
print("     3. Add the order_month column")
print("     4. Join all three tables")
print("     5. Group by month and category, sum the prices")
print("     6. Sort and return the top 10 rows")
print()
print("   All of this happens in ONE pass through the data.")

input("\n🔍 Press ENTER to trigger the ACTION ➔ ")

t0 = time.time()
df_finance.show(10, truncate=False)
elapsed = time.time() - t0

print(f"\n⏱️  Full pipeline executed in {elapsed:.2f} seconds")
print()
print("   OBSERVE: Check the Spark UI at http://localhost:4040")
print("   You should see a large new Job appear — that is the .show() action.")
print("   Click into it. You will see it is broken into multiple Stages (due to joins/grouping).")

pause()

# ─────────────────────────────────────────────────────────────────
# SECTION 8 — Verify grain with row counts
# ─────────────────────────────────────────────────────────────────

print("SECTION 8: Verify the grain — check the row counts\n")

print("We are going to count rows at each stage to answer the grain question from Section 5.")
print("Because .count() is an ACTION, Spark will run the pipeline THREE times to compute these.")

input("\n🔍 Press ENTER to trigger the counts (this will take a moment) ➔ ")

delivered_count      = df_delivered.count()
items_delivered_count = df_joined.count()
output_rows          = df_finance.count()

print(f"\n   Delivered orders                  : {delivered_count:>10,}")
print(f"   Order-items for delivered orders  : {items_delivered_count:>10,}  ← more than orders (multiple items per order)")
print(f"   Finance output rows               : {output_rows:>10,}  ← one row per month × category")
print()
print("   OBSERVE: Check the Spark UI. You will see several new completed Jobs!")

pause()

# ─────────────────────────────────────────────────────────────────
# SECTION 9 — The same pipeline written cleanly (reference)
# ─────────────────────────────────────────────────────────────────

print("SECTION 9: The complete pipeline — clean reference version\n")

print("""
Here is the full pipeline without section breaks — use this as a reference:

─────────────────────────────────────────────────────────────────────
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder \\
    .appName("FinancePipeline") \\
    .master("local[*]") \\
    .getOrCreate()

df_orders   = spark.read.csv("data/olist_orders_dataset.csv",      header=True, inferSchema=True)
df_items    = spark.read.csv("data/olist_order_items_dataset.csv", header=True, inferSchema=True)
df_products = spark.read.csv("data/olist_products_dataset.csv",    header=True, inferSchema=True)

df_finance = df_items \\
    .join(
        df_orders.filter(F.col("order_status") == "delivered")
                 .withColumn("order_month", F.date_trunc("month", F.col("order_purchase_timestamp"))),
        on="order_id", how="inner"
    ) \\
    .join(df_products, on="product_id", how="inner") \\
    .groupBy(F.col("order_month"), F.col("product_category_name")) \\
    .agg(
        F.sum("price").alias("item_sales_value"),
        F.countDistinct("order_id").alias("distinct_orders")
    ) \\
    .orderBy("order_month", F.desc("item_sales_value"))

df_finance.show(10)
─────────────────────────────────────────────────────────────────────
""")

print("\n" + "=" * 70)
print("✅ Script 02 complete.")
print("=" * 70)
print()
print("   What you built:")
print("     filter → withColumn → join → groupBy → agg → show")
print("     Five transformations. One action. One pass through the data.")
print()
print("   Next → run python scripts/03_spark_sql_pipeline.py")
print("   The output should be identical — different API, same engine.")

print("\n" + "─" * 70)
print("🛑 Before you leave: The Spark UI is still running at http://localhost:4040.")
print("Take a final look around. When you press ENTER below, the script will call")
print("spark.stop(). The JVM will shut down, and the UI will disappear immediately.")
input("\n🔍 Press ENTER to stop the SparkSession and exit ➔ ")

spark.stop()
print("✅ SparkSession stopped. See you in the next script!")
