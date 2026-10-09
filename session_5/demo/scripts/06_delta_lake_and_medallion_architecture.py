"""
DataKern — Session 5 Demo
Script 06: Delta Lake, ACID Transactions, Time Travel, and Medallion Architecture

What you will learn:
  1. Why plain Parquet is not enough: The need for ACID transactions
  2. How Delta Lake works under the hood: Parquet + The Transaction Log (_delta_log)
  3. Reading and inspecting the JSON transaction log directly
  4. Time Travel: How to query previous versions of your data with versionAsOf
  5. Audit history: Using DeltaTable.history() to inspect all changes
  6. Schema Enforcement vs Schema Evolution (mergeSchema)
  7. The Medallion Architecture (Bronze ➔ Silver ➔ Gold) in practice

Prerequisites:
  - Spark 3.5.1 + delta-spark 3.2.0 installed in .venv
  - Olist datasets in demo/data/ (or symlinked)

Run from demo/ folder:
  python scripts/06_delta_lake_and_medallion_architecture.py
"""

import json
import os
import shutil
import sys
import time

# Prevent macOS binding errors
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

try:
    from delta import configure_spark_with_delta_pip
    from delta.tables import DeltaTable
except ImportError:
    print("🛑 Error: delta-spark is not installed in this environment.")
    print("   Please activate your virtual environment and run:")
    print("   pip install delta-spark==3.2.0")
    sys.exit(1)

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ─────────────────────────────────────────────────────────────────
# SECTION 0 — Configuration & Helper Functions
# ─────────────────────────────────────────────────────────────────

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")
OUTPUT_DIR = os.path.join(BASE_DIR, "output")

ORDERS_CSV = os.path.join(DATA_DIR, "olist_orders_dataset.csv")
ITEMS_CSV = os.path.join(DATA_DIR, "olist_order_items_dataset.csv")
PRODUCTS_CSV = os.path.join(DATA_DIR, "olist_products_dataset.csv")

DELTA_SILVER = os.path.join(OUTPUT_DIR, "delta_silver_orders")
DELTA_GOLD = os.path.join(OUTPUT_DIR, "delta_gold_finance")

DIVIDER = "═" * 72
SUB_DIVIDER = "─" * 72


def pause():
    """Pauses execution to allow interactive exploration in the terminal and Spark UI."""
    input("\n🔍 Press ENTER to continue ➔ ")
    print("\n" + SUB_DIVIDER)


# ─────────────────────────────────────────────────────────────────
# SECTION 1 — Start SparkSession with Delta Lake Extensions
# ─────────────────────────────────────────────────────────────────

print("\n" + DIVIDER)
print("  DATAKERN SESSION 5: SCRIPT 06 — DELTA LAKE & MEDALLION ARCHITECTURE")
print(DIVIDER)
print("""
THE REVOLUTION OF DELTA LAKE:
Plain Parquet gave us columnar speed, but it left big data problems unsolved:
  ❌ No ACID Transactions: If a write job fails halfway, the folder is corrupted.
  ❌ No Rollback / Time Travel: Overwriting a folder destroys the past forever.
  ❌ No Concurrent Writers: Two jobs writing simultaneously corrupt each other's data.
  ❌ No Audit Trail: You can never prove what changed or when.

DELTA LAKE SOLVES THIS WITH ONE FORMULA:
    Delta Lake = Parquet Files + ACID Transaction Log (_delta_log)
""")

builder = SparkSession.builder \
    .appName("DataKern_Session5_06_DeltaLake") \
    .master("local[*]") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.ui.port", "4040") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

cores_available = spark.sparkContext.defaultParallelism

print(f"✅ SparkSession initialized with Delta Lake catalog extensions.")
print(f"   Master: {spark.sparkContext.master} ({cores_available} CPU cores detected on your laptop)")
print("   Task counts and partition metrics in the Spark UI will reflect this hardware configuration.")
print("   Spark UI active at: http://localhost:4040")

pause()

# ─────────────────────────────────────────────────────────────────
# SECTION 2 — Write First Delta Table & Inspect _delta_log
# ─────────────────────────────────────────────────────────────────

print("SECTION 2: Writing a Delta Table and Inspecting the Transaction Log\n")

if os.path.exists(DELTA_SILVER):
    shutil.rmtree(DELTA_SILVER)

df_orders = spark.read.csv(ORDERS_CSV, header=True, inferSchema=True) \
    .select("order_id", "customer_id", "order_status", "order_purchase_timestamp")

print("Writing df_orders to Delta format (Version 0)...")
t0 = time.time()
df_orders.write.format("delta").mode("overwrite").save(DELTA_SILVER)
time_v0 = time.time() - t0

print(f"✅ Delta write completed in {time_v0:.2f}s.")
print(f"\n📁 WHAT IS ON DISK IN {DELTA_SILVER}?")
for item in sorted(os.listdir(DELTA_SILVER)):
    print(f"   ├── {item}")

delta_log_dir = os.path.join(DELTA_SILVER, "_delta_log")
print(f"\n📁 WHAT IS INSIDE _delta_log/?")
for log_file in sorted(os.listdir(delta_log_dir)):
    print(f"   └── _delta_log/{log_file}")

print("\n🔍 LET'S INSPECT THE ACTUAL JSON COMMIT FILE (00000000000000000000.json):")
v0_commit_path = os.path.join(delta_log_dir, "00000000000000000000.json")

with open(v0_commit_path, "r") as f:
    for line_idx, line in enumerate(f):
        entry = json.loads(line)
        key = list(entry.keys())[0]
        if key in ["commitInfo", "metaData", "protocol"]:
            print(f"   Entry: {key:<12} ➔ {entry[key]}")
        elif key == "add" and line_idx < 4:
            # show one sample added parquet file with its statistics
            file_stats = entry["add"].get("stats", "{}")
            print(f"   Entry: add (file)  ➔ path: {entry['add']['path'][:25]}... stats: {file_stats}")

print("""
🔎 LOOK CLOSELY AT THE JSON COMMIT:
Notice that Delta records:
1. Exactly which Parquet files were added.
2. The schema and column data types.
3. MIN and MAX statistics for every column inside those Parquet files!
When you query Delta, Spark reads this tiny JSON log first to know EXACTLY 
which Parquet files are valid.
""")

pause()

# ─────────────────────────────────────────────────────────────────
# SECTION 3 — Modifying Data & Version 1 Creation
# ─────────────────────────────────────────────────────────────────

print("SECTION 3: Overwriting Data — Creating Version 1\n")
print("""
SCENARIO:
A business filter updates our Silver table to retain ONLY 'delivered' orders.
In plain Parquet, an overwrite would destroy the old data forever.
Let's see what Delta Lake does:
""")

df_delivered_only = df_orders.filter(F.col("order_status") == "delivered")
initial_count = df_orders.count()
delivered_count = df_delivered_only.count()

print(f"Initial order count (Version 0)   : {initial_count:,}")
print(f"Delivered order count (Version 1) : {delivered_count:,}")

print("\nOverwriting Delta table with delivered orders only...")
df_delivered_only.write.format("delta").mode("overwrite").save(DELTA_SILVER)

print("✅ Overwrite completed.")
print(f"\n📁 INSPECTING _delta_log/ AFTER SECOND WRITE:")
for log_file in sorted(os.listdir(delta_log_dir)):
    print(f"   └── _delta_log/{log_file}")

print("Notice: '00000000000000000001.json' was created!")

pause()

# ─────────────────────────────────────────────────────────────────
# SECTION 4 — Time Travel & Audit History
# ─────────────────────────────────────────────────────────────────

print("SECTION 4: Time Travel & Full Audit Trail\n")
print("""
TIME TRAVEL IN ACTION:
What if an auditor asks: 'What was the exact data state BEFORE the overwrite?'
With Delta Lake, you simply pass:
    .option("versionAsOf", 0)
""")

print("Querying current data (Version 1):")
df_current = spark.read.format("delta").load(DELTA_SILVER)
print(f"   Current table count: {df_current.count():,} rows")

print("\nQuerying historical data via Time Travel (Version 0):")
df_historical = spark.read.format("delta").option("versionAsOf", 0).load(DELTA_SILVER)
print(f"   Version 0 table count: {df_historical.count():,} rows")

print("""
🚀 CONFIRMED:
The previous state was never destroyed!
Spark reads the commit log for Version 0, ignores the files added in Version 1, 
and perfectly recreates the historical state!
""")

print("NOW LET'S INSPECT THE FULL AUDIT TRAIL USING DeltaTable.history():")
delta_table = DeltaTable.forPath(spark, DELTA_SILVER)
delta_table.history().select(
    "version", "timestamp", "userId", "operation", "operationMetrics.numOutputRows"
).show(truncate=False)

pause()

# ─────────────────────────────────────────────────────────────────
# SECTION 5 — Schema Enforcement vs Schema Evolution
# ─────────────────────────────────────────────────────────────────

print("SECTION 5: Schema Enforcement vs Schema Evolution\n")
print("""
SCHEMA ENFORCEMENT:
What happens if an upstream pipeline accidentally adds an unexpected column?
Delta Lake strictly ENFORCES schemas to protect downstream data consumers.
""")

# Create data with an extra unwanted column
df_rogue = df_current.limit(5).withColumn("rogue_column", F.lit("unexpected_payload"))

print("Attempting to append data with new column WITHOUT permission...")
try:
    df_rogue.write.format("delta").mode("append").save(DELTA_SILVER)
    print("   Unexpectedly succeeded!")
except Exception as e:
    print("🛑 SCHEMA ENFORCEMENT BLOCKED THE WRITE!")
    print(f"   Error: {type(e).__name__} — Delta prevented schema mismatch from corrupting table.")

print("""
SCHEMA EVOLUTION:
If the business INTENTIONALLY wants to add this column, you pass:
    .option("mergeSchema", "true")
""")

print("Appending with .option('mergeSchema', 'true')...")
df_rogue.write.format("delta").mode("append").option("mergeSchema", "true").save(DELTA_SILVER)

print("✅ Schema evolved cleanly. New schema:")
spark.read.format("delta").load(DELTA_SILVER).printSchema()

pause()

# ─────────────────────────────────────────────────────────────────
# SECTION 6 — The Medallion Architecture: End-to-End Pipeline
# ─────────────────────────────────────────────────────────────────

print("SECTION 6: The Medallion Architecture in Practice\n")
print("""
THE MEDALLION ARCHITECTURE (Lakehouse Best Practice):
  🥉 BRONZE : Raw, unaltered ingest from CSV/APIs. Append-only.
  🥈 SILVER : Cleaned, schema-enforced, deduplicated, enriched joins.
  🥇 GOLD   : Curated business aggregations, metrics, and KPIs ready for BI.

Let's build a complete Gold Finance Table from our Olist data:
""")

if os.path.exists(DELTA_GOLD):
    shutil.rmtree(DELTA_GOLD)

# Step A: Load Silver Orders and Order Items
df_silver_orders = spark.read.format("delta").load(DELTA_SILVER) \
    .filter(F.col("order_status") == "delivered")

df_items = spark.read.csv(ITEMS_CSV, header=True, inferSchema=True)
df_products = spark.read.csv(PRODUCTS_CSV, header=True, inferSchema=True)

# Step B: Curate into Gold Finance dataset
from pyspark.sql.window import Window

df_gold_finance = df_items \
    .join(df_silver_orders, on="order_id", how="inner") \
    .join(F.broadcast(df_products), on="product_id", how="inner") \
    .filter(F.col("product_category_name").isNotNull()) \
    .groupBy("product_category_name") \
    .agg(
        F.count("order_id").alias("total_items_sold"),
        F.round(F.sum("price"), 2).alias("total_revenue"),
        F.round(F.avg("price"), 2).alias("avg_item_price")
    )

# Attach Window Ranking to Gold Table
w_gold = Window.orderBy(F.desc("total_revenue"))
df_gold_finance = df_gold_finance.withColumn("revenue_rank", F.row_number().over(w_gold))

print("Writing Gold table as Delta Lake...")
df_gold_finance.write.format("delta").mode("overwrite").save(DELTA_GOLD)

print("✅ Gold Finance Delta Table successfully created!")
print("\nTop 5 Product Categories in Gold Table:")
spark.read.format("delta").load(DELTA_GOLD).show(5, truncate=False)

pause()

# ─────────────────────────────────────────────────────────────────
# SECTION 7 — Clean Production Reference
# ─────────────────────────────────────────────────────────────────

print("SECTION 7: Clean Production Reference Code\n")
print("""
# ─────────────────────────────────────────────────────────────────
# DELTA LAKE PRODUCTION CHEATSHEET:
# ─────────────────────────────────────────────────────────────────

# 1. Delta Session Configuration:
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

builder = SparkSession.builder \\
    .appName("ProductionLakehouse") \\
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \\
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# 2. Writing Delta:
df.write.format("delta").mode("overwrite").save("path/to/delta_table")

# 3. Reading Delta:
df = spark.read.format("delta").load("path/to/delta_table")

# 4. Time Travel:
df_v0 = spark.read.format("delta").option("versionAsOf", 0).load("path/to/delta_table")
# or by timestamp:
# df_yesterday = spark.read.format("delta").option("timestampAsOf", "2026-10-05").load(...)

# 5. Audit History:
from delta.tables import DeltaTable
DeltaTable.forPath(spark, "path/to/delta_table").history().show()

# 6. Schema Evolution:
df.write.format("delta").mode("append").option("mergeSchema", "true").save("path/to/delta_table")
""")

pause()

# ─────────────────────────────────────────────────────────────────
# SECTION 8 — Summary & Assignment 5 Connection
# ─────────────────────────────────────────────────────────────────

print("SECTION 8: Summary & Connection to Assignment 5\n")
print("""
🎉 SCRIPT 06 COMPLETE — YOU HAVE MASTERED MODERN DATA LAKEHOUSES!

What you have proven:
  ✅ Delta Lake gives Parquet ACID reliability via the _delta_log JSON transaction log.
  ✅ Time Travel allows instant recovery of historical data without backups.
  ✅ Schema Enforcement protects against corrupt writes; mergeSchema evolves cleanly.
  ✅ Medallion Architecture (Bronze ➔ Silver ➔ Gold) builds robust enterprise data pipelines.

🎯 READY FOR ASSIGNMENT 5:
You now have all the tools needed to complete Assignment 5:
  1. Window Functions (running sums, rank, lead/lag MoM growth)
  2. Delta Lake outputs with audit history validation
  3. Reconciling numbers back to your Assignment 4 totals!

*Don't focus on getting the answer. Focus on learning how to engineer the solution.*
  — Uma Kiran | DataKern | www.data-kern.com
""")

spark.stop()
print("SparkSession stopped cleanly.")
