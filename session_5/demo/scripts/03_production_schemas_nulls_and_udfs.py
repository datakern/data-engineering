"""
DataKern — Session 5 Demo
Script 03: Production Schemas, Null Handling Design, and UDF Performance

What you will learn:
  1. Why inferSchema=True is dangerous in production and how to define explicit StructTypes
  2. Why Null Handling is a business design decision, not simple cleanup
  3. How to track row audit counts (ingested vs dropped vs preserved)
  4. The architectural penalty of Python UDFs (Py4J serialization & Catalyst blindness)
  5. How to replace Python UDFs with Native Spark SQL functions for 5x–10x speedup

Prerequisites:
  - Spark running locally
  - Olist datasets in demo/data/ (or symlinked)

Run from demo/ folder:
  python scripts/03_production_schemas_nulls_and_udfs.py
"""

import os
import sys
import time

# Prevent macOS binding errors
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    TimestampType
)

# ─────────────────────────────────────────────────────────────────
# SECTION 0 — Configuration & Helper Functions
# ─────────────────────────────────────────────────────────────────

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
ORDERS_CSV = os.path.join(DATA_DIR, "olist_orders_dataset.csv")
ITEMS_CSV = os.path.join(DATA_DIR, "olist_order_items_dataset.csv")

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
print("  DATAKERN SESSION 5: SCRIPT 03 — SCHEMAS, NULLS & UDF COSTS")
print(DIVIDER)
print("""
THE THREE PILLARS OF PRODUCTION DATA HYGIENE:
1. Schemas: Never let Spark guess data types in production.
2. Nulls: Missing data often carries business meaning. Never blindly .dropna().
3. Native Functions: Avoid Python UDFs to prevent the JVM-Python serialization penalty.
""")

spark = SparkSession.builder \
    .appName("DataKern_Session5_03_Schemas_Nulls_UDFs") \
    .master("local[*]") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.ui.port", "4040") \
    .getOrCreate()

cores_available = spark.sparkContext.defaultParallelism

print(f"✅ SparkSession ready.")
print(f"   Master: {spark.sparkContext.master} ({cores_available} CPU cores detected on your laptop)")
print("   Task counts and partition metrics in the Spark UI will reflect this hardware configuration.")
print("   Spark UI active at: http://localhost:4040")

pause()

# ─────────────────────────────────────────────────────────────────
# SECTION 2 — Explicit Schema Enforcement vs inferSchema
# ─────────────────────────────────────────────────────────────────

print("SECTION 2: Schema Enforcement — Don't Trust inferSchema in Production\n")
print("""
WHY inferSchema=True FAILS IN PRODUCTION:
1. Two-pass file scan: Spark reads the entire file once to guess types, then reads it again.
2. Silent data corruption: A zip code prefix '01440' is cast to integer 1440, losing the leading zero!
3. Schema Drift: If next week's file contains an unexpected string in a numeric column, 
   inferSchema silently changes the entire column type to String, breaking downstream queries!

THE PRODUCTION STANDARD:
Define explicit schemas with StructType and StructField.
""")

orders_schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("order_status", StringType(), False),
    StructField("order_purchase_timestamp", StringType(), True),
    StructField("order_approved_at", StringType(), True),
    StructField("order_delivered_carrier_date", StringType(), True),
    StructField("order_delivered_customer_date", StringType(), True),
    StructField("order_estimated_delivery_date", StringType(), True)
])

print("Explicit Schema Defined:")
for field in orders_schema:
    print(f"   {field.name:<32} : {field.dataType.simpleString()} (nullable={field.nullable})")

print("\nReading orders with explicit schema...")
t0 = time.time()
df_orders = spark.read.csv(
    ORDERS_CSV,
    header=True,
    schema=orders_schema,
    mode="DROPMALFORMED"  # Production safety mode
)
order_count = df_orders.count()
elapsed_read = time.time() - t0

print(f"✅ Fast single-pass read completed in {elapsed_read:.3f}s. Rows: {order_count:,}")
print("   Schema is guaranteed: zero type surprises, zero schema drift.")

pause()

# ─────────────────────────────────────────────────────────────────
# SECTION 3 — Null Handling: A Business Design Decision
# ─────────────────────────────────────────────────────────────────

print("SECTION 3: Null Handling — Design Decisions vs Accidental Data Loss\n")
print("""
THE TRAP:
A junior engineer sees NULL values in 'order_delivered_customer_date' and runs:
    df_clean = df_orders.dropna()

WHY IS THIS DANGEROUS?
Let's inspect what rows actually contain NULL delivery dates:
""")

# Count nulls across delivery milestones
null_stats = df_orders.select(
    F.count(F.when(F.col("order_approved_at").isNull(), 1)).alias("null_approved"),
    F.count(F.when(F.col("order_delivered_carrier_date").isNull(), 1)).alias("null_carrier"),
    F.count(F.when(F.col("order_delivered_customer_date").isNull(), 1)).alias("null_delivered")
)
null_stats.show()

print("Now let's check the ORDER STATUS for rows where delivery date is NULL:")
df_orders.filter(F.col("order_delivered_customer_date").isNull()) \
    .groupBy("order_status") \
    .count() \
    .show()

print("""
🔎 CRITICAL INSIGHT:
Orders with status 'shipped', 'processing', 'invoiced', or 'created' have NULL delivery dates
NOT because the data is broken, but because the parcel is CURRENTLY ON A DELIVERY TRUCK!
If you blindly dropped nulls, you would eliminate 100% of all in-transit business orders!

PRODUCTION NULL STRATEGY:
1. Audit primary keys: drop rows ONLY where primary keys (order_id, customer_id) are null.
2. Preserve in-transit states: leave legitimate business nulls or flag them clearly.
3. Detect anomalies: identify orders marked 'delivered' that mysteriously lack a delivery date.
""")

total_rows = df_orders.count()

# Step A: Drop rows missing primary identifiers
df_valid_keys = df_orders.na.drop(subset=["order_id", "customer_id"])
valid_count = df_valid_keys.count()

# Step B: Add an explicit Data Quality Flag column
df_flagged = df_valid_keys.withColumn(
    "delivery_quality_status",
    F.when(
        (F.col("order_status") == "delivered") & F.col("order_delivered_customer_date").isNull(),
        "DELIVERED_MISSING_DATE_ANOMALY"
    ).when(
        F.col("order_status").isin("shipped", "processing", "invoiced"),
        "IN_TRANSIT"
    ).otherwise("NORMAL")
)

anomaly_count = df_flagged.filter(F.col("delivery_quality_status") == "DELIVERED_MISSING_DATE_ANOMALY").count()

print("📊 PIPELINE AUDIT REPORT:")
print(f"   Total rows ingested             : {total_rows:,}")
print(f"   Corrupt rows (dropped null keys): {total_rows - valid_count:,}")
print(f"   Valid active orders preserved   : {valid_count:,}")
print(f"   Identified delivery anomalies   : {anomaly_count:,} (flagged for ops review, not lost)")

pause()

# ─────────────────────────────────────────────────────────────────
# SECTION 4 — The Cost of Python UDFs vs Built-in Functions
# ─────────────────────────────────────────────────────────────────

print("SECTION 4: The Hidden Architecture Penalty of Python UDFs\n")
print("""
WHY ARE PYTHON UDFs SLOW?
Apache Spark is written in Scala/Java and runs inside the Java Virtual Machine (JVM).
When you call a Python UDF:
  1. Spark must serialize the row from JVM memory into bytes.
  2. Send bytes over a local IPC socket to a Python worker process.
  3. Python deserializes the bytes into Python objects.
  4. Python executes your Python function row-by-row.
  5. Python serializes the return value back into bytes.
  6. Spark deserializes bytes back into JVM memory!
  7. CATALYST CANNOT OPTIMIZE PYTHON CODE (it is a complete black box).

NATIVE SPARK FUNCTIONS (F.when, F.regexp_replace, etc.):
  - Execute 100% inside the JVM with Tungsten whole-stage code generation.
  - Zero IPC serialization overhead. Fully optimized by Catalyst.

LET'S BENCHMARK BOTH APPROACHES ON 112,000 ORDER ITEMS:
Task: Categorize items into price tiers:
      - Price < 50.0  ➔ 'Budget'
      - Price < 150.0 ➔ 'Standard'
      - Price >= 150.0 ➔ 'Premium'
""")

df_items = spark.read.csv(ITEMS_CSV, header=True, inferSchema=True)

# ── Approach 1: Python UDF ───────────────────────────────────────
print("Approach 1: Running Python UDF (@F.udf)...")


def python_tier_func(price):
    if price is None:
        return "Unknown"
    elif price < 50.0:
        return "Budget"
    elif price < 150.0:
        return "Standard"
    else:
        return "Premium"


# Register UDF
tier_udf = F.udf(python_tier_func, StringType())

t0 = time.time()
df_with_udf = df_items.withColumn("price_tier", tier_udf(F.col("price")))
# Force computation across all 112k rows
udf_summary = df_with_udf.groupBy("price_tier").count().collect()
time_udf = time.time() - t0

print(f"✅ Python UDF finished in: {time_udf:.3f}s")

# ── Approach 2: Native Spark Function ────────────────────────────
print("\nApproach 2: Running Native Spark SQL Expression (F.when)...")

t0 = time.time()
df_with_native = df_items.withColumn(
    "price_tier",
    F.when(F.col("price").isNull(), "Unknown")
     .when(F.col("price") < 50.0, "Budget")
     .when(F.col("price") < 150.0, "Standard")
     .otherwise("Premium")
)
native_summary = df_with_native.groupBy("price_tier").count().collect()
time_native = time.time() - t0

print(f"✅ Native Spark finished in: {time_native:.3f}s")

speedup = (time_udf / time_native) if time_native > 0 else 1.0
print(f"\n🚀 RESULT: Native Spark was {speedup:.1f}x FASTER than the Python UDF!")

print("\n🔎 PHYSICAL PLAN INSPECTION:")
print("Let's look at df_with_udf.explain():")
print("Notice the 'BatchEvalPython' node — that represents the Python socket bottleneck.")
df_with_udf.select("order_id", "price_tier").explain()

pause()

# ─────────────────────────────────────────────────────────────────
# SECTION 5 — Clean Production Reference
# ─────────────────────────────────────────────────────────────────

print("SECTION 5: Clean Production Reference Code\n")
print("""
# ─────────────────────────────────────────────────────────────────
# PRODUCTION BEST PRACTICES:
# ─────────────────────────────────────────────────────────────────

# 1. EXPLICIT SCHEMA ENFORCEMENT:
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("price", DoubleType(), False)
])
df = spark.read.csv("data/items.csv", header=True, schema=schema, mode="DROPMALFORMED")

# 2. AUDITED NULL HANDLING:
# Drop only primary keys:
df_valid = df.na.drop(subset=["order_id"])

# Fill dimensions with fallback:
df_filled = df_valid.na.fill({"category": "uncategorized"})

# 3. ALWAYS USE NATIVE FUNCTIONS OVER UDFs:
# AVOID: @F.udf def my_func(col): ...
# USE:
df_result = df.withColumn(
    "tier",
    F.when(F.col("price") < 50, "Budget")
     .when(F.col("price") < 150, "Standard")
     .otherwise("Premium")
)
""")

pause()

# ─────────────────────────────────────────────────────────────────
# SECTION 6 — Summary & Handoff
# ─────────────────────────────────────────────────────────────────

print("SECTION 6: Summary & What's Next\n")
print("""
🎉 SCRIPT 03 COMPLETE!
What you have seen with your own eyes:
  ✅ inferSchema=True is slow and fragile; explicit StructTypes ensure safety and speed.
  ✅ Never blindly .dropna() — in-flight parcels and business statuses carry critical nulls.
  ✅ Python UDFs suffer severe IPC serialization overhead; native F.when is significantly faster.

👉 NEXT UP:
Run Script 04 to understand Window Functions: how to calculate running cumulative sums 
and rankings without collapsing row granularity!

Command to run:
  python scripts/04_window_functions_intro.py
""")

spark.stop()
print("SparkSession stopped cleanly.")
