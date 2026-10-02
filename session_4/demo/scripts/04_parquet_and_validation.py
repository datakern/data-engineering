"""
DataKern — Session 4 Demo
Script 04: Saving to Parquet and Validating

What you will learn:
  1. How to use Native Spark Functions to validate data logic
  2. How to write a DataFrame to Parquet format
  3. Why Spark writes directories, not single files

Run from the demo/ folder:
  python scripts/04_parquet_and_validation.py
"""

import os
import shutil
import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
OUTPUT_DIR = os.path.join(DATA_DIR, "finance_curated.parquet")


def pause():
    input("\n🔍 Press ENTER to continue ➔ ")
    print("\n" + "─" * 70)


# ─────────────────────────────────────────────────────────────────
# SECTION 1 — Setup
# ─────────────────────────────────────────────────────────────────

print("\n" + "=" * 70)
print("SECTION 1: Setup and Pipeline Execution")
print("=" * 70)

spark = SparkSession.builder \
    .appName("DataKern_Session4_Parquet") \
    .master("local[*]") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

print("✅ SparkSession ready.")

# Run the full pipeline quietly
df_orders   = spark.read.csv(os.path.join(DATA_DIR, "olist_orders_dataset.csv"),      header=True, inferSchema=True)
df_items    = spark.read.csv(os.path.join(DATA_DIR, "olist_order_items_dataset.csv"), header=True, inferSchema=True)
df_products = spark.read.csv(os.path.join(DATA_DIR, "olist_products_dataset.csv"),    header=True, inferSchema=True)

# -------------------------------------------------------------------------
# THE FACTORY ASSEMBLY LINE (Pipeline Explanation)
# Think of this pipeline like a factory assembly line. Every dot (.) moves 
# the data to the next machine to filter, join, group, or sort.
# -------------------------------------------------------------------------

df_finance = df_items \
    .join(
        # 1. PREP ORDERS: Filter for 'delivered' orders only (SQL: WHERE)
        #    Then create 'order_month' by rounding the date down to the month
        df_orders.filter(F.col("order_status") == "delivered")
                 .withColumn("order_month", F.date_trunc("month", F.col("order_purchase_timestamp"))),
        on="order_id", how="inner"
    ) \
    # 2. JOIN PRODUCTS: Bring in the product category names
    .join(df_products, on="product_id", how="inner") \
    # 3. GROUPING: Bucket the data by Month and Product Category (SQL: GROUP BY)
    .groupBy(F.col("order_month"), F.col("product_category_name")) \
    # 4. AGGREGATING (The Math): Sum the price and count unique orders.
    #    .alias() renames the resulting column (SQL: AS)
    .agg(
        F.sum("price").alias("item_sales_value"),
        F.countDistinct("order_id").alias("distinct_orders")
    ) \
    # 5. SORTING: Sort chronologically, then by highest sales descending (SQL: ORDER BY)
    .orderBy("order_month", F.desc("item_sales_value"))

print("✅ Pipeline plan built (lazy).")

pause()

# ─────────────────────────────────────────────────────────────────
# SECTION 2 — Data Validation using a UDF
# ─────────────────────────────────────────────────────────────────

print("SECTION 2: Data Validation using Native Functions\n")

print("While you can use UDFs for custom logic, it is highly recommended")
print("to use native Spark functions (F.when, F.col).")
print("Native functions run entirely in the JVM and avoid Python serialization overhead (and bugs!).")
print()

# Custom business logic:
# If there are distinct orders, the sales value must be > 0.
# If sales value is <= 0, but there are orders, that's invalid data.
valid_expr = F.when(
    F.col("item_sales_value").isNull() | F.col("distinct_orders").isNull(), False
).when(
    (F.col("distinct_orders") > 0) & (F.col("item_sales_value") <= 0), False
).otherwise(True)

print("✅ Validation expression defined using native PySpark functions.")

input("\n🔍 Press ENTER to apply the validation logic ➔ ")

# Apply the expression to create a new validation column
df_validated = df_finance.withColumn("is_valid", valid_expr)

print("\nWe just added a new column 'is_valid' computed by native PySpark functions.")

print("\nPREDICT: Will we find any invalid rows?")
input("\n🔍 Press ENTER to filter for invalid rows and show them ➔ ")

invalid_rows = df_validated.filter(F.col("is_valid") == False)
invalid_count = invalid_rows.count()

if invalid_count == 0:
    print(f"\n✅ Data is clean! 0 invalid rows found.")
else:
    print(f"\n⚠️ WARNING: Found {invalid_count} invalid rows:")
    invalid_rows.show()

pause()

# ─────────────────────────────────────────────────────────────────
# SECTION 3 — Writing to Parquet (ACTION)
# ─────────────────────────────────────────────────────────────────

print("SECTION 3: Writing data to Parquet\n")

print("Instead of writing CSV, Data Engineers write Parquet.")
print("Parquet is columnar, compressed, and includes the schema.")
print()
print(f"Target directory: {OUTPUT_DIR}")

# Clean up old run if it exists
if os.path.exists(OUTPUT_DIR):
    shutil.rmtree(OUTPUT_DIR)

input("\n🔍 Press ENTER to run the write action ➔ ")

print("\nWriting... (this executes the entire pipeline and saves the result)")
t0 = time.time()

# .write is an ACTION.
# mode("overwrite") ensures we don't fail if the folder exists.
df_finance.write \
    .mode("overwrite") \
    .parquet(OUTPUT_DIR)

elapsed = time.time() - t0
print(f"\n✅ Data written in {elapsed:.2f} seconds.")

pause()

# ─────────────────────────────────────────────────────────────────
# SECTION 4 — Inspecting the Parquet Output
# ─────────────────────────────────────────────────────────────────

print("SECTION 4: Inspecting the Parquet Output\n")

print("Let's look at what Spark actually wrote to the disk:")

input("\n🔍 Press ENTER to list the output directory ➔ ")

files = os.listdir(OUTPUT_DIR)
print()
for f in sorted(files):
    print(f"   {f}")

print("\nNotice anything strange?")
print("   Spark didn't write ONE file called 'finance_curated.parquet'.")
print("   It created a FOLDER containing multiple 'part-0000X...' files.")
print()
print("   Why? Because Spark is distributed.")
print("   If you have 10 workers processing data, they don't wait in line")
print("   to write to one file. They each write their own 'part' file simultaneously.")
print("   This is how Spark achieves massive write speeds.")

print("\n" + "=" * 70)
print("✅ Script 04 complete.")
print("=" * 70)
print()
print("   What you learned:")
print("     • How to validate data using native PySpark functions")
print("     • How to write DataFrames to Parquet")
print("     • That Spark writes partitioned directories, not single files")
print()
print("   Next → You are ready for Assignment 4!")

print("\n" + "─" * 70)
print("🛑 Before you leave: The Spark UI is still running at http://localhost:4040.")
print("Take a final look around. When you press ENTER below, the script will call")
print("spark.stop(). The JVM will shut down, and the UI will disappear immediately.")
input("\n🔍 Press ENTER to stop the SparkSession and exit ➔ ")

spark.stop()
print("✅ SparkSession stopped.")
