"""
DataKern — Session 5 Demo
Script 02: Join Strategies, the Catalyst Optimizer, and Caching

What you will learn:
  1. Why SortMergeJoin shuffles both tables, and how BroadcastHashJoin eliminates shuffles
  2. How to use F.broadcast() to speed up fact-dimension joins
  3. How to read Spark's execution plan using .explain(mode="formatted")
  4. How Catalyst performs Predicate Pushdown and Column Pruning automatically
  5. When cache() speeds up your pipeline, when it hurts performance, and how to unpersist()

Prerequisites:
  - Spark running locally
  - Olist datasets in demo/data/ (or symlinked)

Run from demo/ folder:
  python scripts/02_join_strategies_and_catalyst_optimizer.py
"""

import os
import sys
import time

# Prevent macOS binding errors
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel

# ─────────────────────────────────────────────────────────────────
# SECTION 0 — Configuration & Helper Functions
# ─────────────────────────────────────────────────────────────────

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
ITEMS_CSV = os.path.join(DATA_DIR, "olist_order_items_dataset.csv")
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
print("  DATAKERN SESSION 5: SCRIPT 02 — JOINS, CATALYST & CACHING")
print(DIVIDER)
print("""
THE BIG QUESTIONS:
1. When joining 112,000 order items with 32,000 products, does Spark have to 
   shuffle BOTH datasets across the network?
2. What does Spark's brain (Catalyst) do behind the scenes when you write code?
3. Is cache() a magic button that makes everything faster, or can it backfire?
""")

spark = SparkSession.builder \
    .appName("DataKern_Session5_02_Joins_Catalyst_Cache") \
    .master("local[*]") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.ui.port", "4040") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

cores_available = spark.sparkContext.defaultParallelism

print(f"✅ SparkSession ready.")
print(f"   Master: {spark.sparkContext.master} ({cores_available} CPU cores detected on your laptop)")
print("   Task counts and partition metrics in the Spark UI will reflect this hardware configuration.")
print("\n" + "!" * 72)
print("💡 ACTION REQUIRED: OPEN YOUR SPARK UI")
print("   URL: http://localhost:4040")
print("   Tabs to watch today: 'SQL / DataFrame', 'Stages', and 'Storage'")
print("!" * 72)

pause()

# ─────────────────────────────────────────────────────────────────
# SECTION 2 — Load Datasets
# ─────────────────────────────────────────────────────────────────

print("SECTION 2: Loading Order Items and Products\n")

if not os.path.exists(ITEMS_CSV) or not os.path.exists(PRODUCTS_CSV):
    print(f"🛑 Error: Could not find CSV files in {DATA_DIR}.")
    spark.stop()
    sys.exit(1)

df_items = spark.read.csv(ITEMS_CSV, header=True, inferSchema=True)
df_products = spark.read.csv(PRODUCTS_CSV, header=True, inferSchema=True)

items_count = df_items.count()
products_count = df_products.count()

print(f"✅ Items loaded    : {items_count:,} rows (Fact table)")
print(f"✅ Products loaded : {products_count:,} rows (Dimension table)")

pause()

# ─────────────────────────────────────────────────────────────────
# SECTION 3 — Join Strategies: SortMergeJoin vs BroadcastHashJoin
# ─────────────────────────────────────────────────────────────────

print("SECTION 3: Join Strategies — The Cost of Shuffling Two Tables\n")
print("""
TWO WAYS SPARK CONNECTS TABLES:
1. SortMergeJoin (Default for large tables):
   - Spark hashes both tables on the join key (product_id).
   - Shuffles both tables across network so identical keys land on the same worker.
   - Sorts rows on each worker, then merges.
   - Cost: TWO network shuffles + TWO disk sorts.

2. BroadcastHashJoin (F.broadcast):
   - The driver sends a full copy of the small dimension table (products) to every worker.
   - Each worker keeps products in RAM.
   - The large items table streams through its local partitions with ZERO network shuffle!
""")

print("🧠 PREDICT:")
print("   If we broadcast df_products into df_items, will the items table need to be shuffled?")
pause()

# Join A: Standard Join (disabling auto-broadcast temporarily to inspect SortMergeJoin)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

t0 = time.time()
df_smj = df_items.join(df_products, on="product_id", how="inner")
smj_count = df_smj.count()
elapsed_smj = time.time() - t0
print(f"✅ Standard Join (SortMergeJoin) completed in {elapsed_smj:.3f}s. Row count: {smj_count:,}")

# Join B: Explicit Broadcast Join
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 10 * 1024 * 1024)  # restore default 10MB

t0 = time.time()
df_bhj = df_items.join(F.broadcast(df_products), on="product_id", how="inner")
bhj_count = df_bhj.count()
elapsed_bhj = time.time() - t0
print(f"✅ Broadcast Join (BroadcastHashJoin) completed in {elapsed_bhj:.3f}s. Row count: {bhj_count:,}")

print("\n🔎 CONFIRMATION & EXPLANATION:")
print(f"   Notice the performance difference! SortMerge: {elapsed_smj:.3f}s vs Broadcast: {elapsed_bhj:.3f}s.")
print("\n   👀 OBSERVE IN SPARK UI (http://localhost:4040):")
print("   METHOD A: THE 'SQL / DataFrame' TAB (Visual Plans)")
print("   1. Click on the 'SQL / DataFrame' tab in the top navigation bar (URL: http://localhost:4040/SQL/).")
print("   2. You will see multiple queries titled 'count at NativeMethodAccessorImpl.java:0'.")
print("      - The second-to-last query (e.g., ID 4) is your Standard Join (SortMergeJoin).")
print("      - The latest query at the bottom (e.g., ID 5) is your Broadcast Join (BroadcastHashJoin).")
print("   3. Click on Query ID 4 (Standard Join):")
print("      Notice the execution graph has 'Exchange hashpartitioning(product_id)' on BOTH branches!")
print("      Both df_items and df_products were sent through the shuffle network.")
print("   4. Go back and click on Query ID 5 (Broadcast Join):")
print("      Notice the large df_items branch connects DIRECTLY to 'BroadcastHashJoin' with ZERO Exchange nodes!")
print("      Only the small products table has a 'BroadcastExchange' box.")
print("")
print("   METHOD B: THE 'Stages' TAB (Raw Byte Metrics - http://localhost:4040/stages/)")
print("   - In the SortMergeJoin stages: Look at 'Shuffle Read' — it transferred ~3.2 MB of data!")
print("   - In the Broadcast Join stages: Look at the items stage — it had 0.0 B Shuffle Read for the join!")

pause()

# ─────────────────────────────────────────────────────────────────
# SECTION 4 — The Catalyst Optimizer: Reading .explain()
# ─────────────────────────────────────────────────────────────────

print("SECTION 4: The Catalyst Optimizer — Spark's Internal Query Planner\n")
print("""
HOW CATALYST OPTIMIZES YOUR CODE:
You write what you want (DataFrame / SQL code).
Catalyst figures out the fastest physical way to execute it across the cluster:
  1. Parsed Logical Plan   : Verifies syntax
  2. Analyzed Logical Plan : Verifies table names and column types
  3. Optimized Logical Plan: Pushes filters down, prunes unneeded columns
  4. Physical Plan         : Generates the actual JVM bytecode to execute
""")

print("Let's build a query with a filter and column selection:")
df_query = df_items \
    .filter(F.col("price") > 150.0) \
    .select("order_id", "product_id", "price") \
    .filter(F.col("price") < 500.0)

print("\nHere is the formatted execution plan from df_query.explain(mode='formatted'):\n")
df_query.explain(mode="formatted")

print("""
🔎 HOW TO READ THIS PHYSICAL PLAN:
1. PREDICATE PUSHDOWN (PushedFilters):
   Notice that the filters (price > 150.0 AND price < 500.0) are combined and 
   pushed directly to the Scan step at the bottom!
   Spark doesn't read the whole file and then filter in Python.
   It filters AT THE STORAGE LAYER before loading unneeded rows into memory.

2. COLUMN PRUNING (ReadSchema / Project):
   The original CSV has 7 columns (shipping_limit_date, freight_value, seller_id, etc.).
   Notice that the scan only reads the 3 columns you actually selected!
   Catalyst discards unselected columns immediately.
""")

pause()

# ─────────────────────────────────────────────────────────────────
# SECTION 5 — Caching vs Persistence: The Storage Tab
# ─────────────────────────────────────────────────────────────────

print("SECTION 5: Caching & Persistence — When to Remember, When to Forget\n")
print("""
THE GOLDEN RULE OF CACHING:
- Cache only when a DataFrame is used in TWO OR MORE branching actions or iterative loops.
- NEVER cache a DataFrame that is used only once in a linear pipeline!
  (Writing to cache consumes CPU serialization and RAM with zero benefit.)

Let's test this rule:
We will compute two separate business metrics from the same filtered items DataFrame.
""")

df_heavy = df_items.filter(F.col("freight_value") > 25.0)

# Run 1: Without caching (recomputed twice from scratch)
print("Step 1: Running two actions WITHOUT caching...")
t0 = time.time()
count_without = df_heavy.count()
avg_price_without = df_heavy.agg(F.avg("price")).collect()[0][0]
time_without = time.time() - t0
print(f"   Done in {time_without:.3f}s. (Spark read the source CSV TWICE!)")

# Run 2: With caching
print("\nStep 2: Calling df_heavy.cache() and re-running...")
df_heavy_cached = df_heavy.cache()

# The first action materializes the cache into memory
t0 = time.time()
count_with_1 = df_heavy_cached.count()
time_cache_materialize = time.time() - t0

# The second action reads directly from RAM!
t0 = time.time()
avg_price_with_2 = df_heavy_cached.agg(F.avg("price")).collect()[0][0]
time_cache_read = time.time() - t0

print(f"   First action (materialize cache in RAM) : {time_cache_materialize:.3f}s")
print(f"   Second action (reads directly from RAM): {time_cache_read:.3f}s (Instantaneous!)")

print("\n👀 OBSERVE IN SPARK UI (http://localhost:4040):")
print("1. Click on the 'Storage' tab in the top navigation bar.")
print("2. You will see an entry for the cached DataFrame!")
print("3. Check:")
print("   - Storage Level: Memory Deserialized 1x Replicated")
print("   - Fraction Cached: 100%")
print("   - Size in Memory: Actual RAM occupied by these rows")
print("4. This tab is how production engineers ensure they are not filling executor RAM!")

pause()

print("Step 3: Memory Hygiene — Always unpersist() when done!\n")
print("If you do not unpersist, old DataFrames linger in RAM and cause OutOfMemory errors.")
df_heavy_cached.unpersist()
print("✅ df_heavy_cached.unpersist() called.")
print("👀 OBSERVE: Refresh the 'Storage' tab in the Spark UI. It is now completely empty.")

pause()

# ─────────────────────────────────────────────────────────────────
# SECTION 6 — Clean Production Reference
# ─────────────────────────────────────────────────────────────────

print("SECTION 6: Clean Production Reference Code\n")
print("""
# ─────────────────────────────────────────────────────────────────
# PRODUCTION BEST PRACTICES:
# ─────────────────────────────────────────────────────────────────

# 1. BROADCAST JOIN (For small dimension tables < 100MB):
from pyspark.sql import functions as F
df_joined = df_fact_items.join(
    F.broadcast(df_dim_products), 
    on="product_id", 
    how="inner"
)

# 2. INSPECT QUERY PLAN (Validate Predicate Pushdown):
df_joined.explain(mode="formatted")

# 3. CACHING PATTERN (Only for branching logic):
df_filtered = df_orders.filter(F.col("order_status") == "delivered").cache()

# Branch A:
monthly_revenue = df_filtered.groupBy("order_month").sum("revenue")
monthly_revenue.write.parquet("output/monthly_revenue.parquet")

# Branch B:
customer_summary = df_filtered.groupBy("customer_id").count()
customer_summary.write.parquet("output/customer_summary.parquet")

# Always clean up RAM:
df_filtered.unpersist()
""")

pause()

# ─────────────────────────────────────────────────────────────────
# SECTION 7 — Summary & Handoff
# ─────────────────────────────────────────────────────────────────

print("SECTION 7: Summary & What's Next\n")
print("""
🎉 SCRIPT 02 COMPLETE!
What you have seen with your own eyes:
  ✅ SortMergeJoin shuffles both tables, while BroadcastHashJoin eliminates fact-table shuffle.
  ✅ Catalyst automatically pushes filters down and prunes unused columns.
  ✅ .cache() stores DataFrames in RAM for branching actions, visible in the Storage tab.
  ✅ Memory hygiene requires calling .unpersist() to prevent OutOfMemory errors.

👉 NEXT UP:
Run Script 03 to master production data hygiene: Schema Enforcement with StructType, 
treating Null Handling as a deliberate design decision, and understanding why 
Python UDFs are a massive performance bottleneck.

Command to run:
  python scripts/03_production_schemas_nulls_and_udfs.py
""")

spark.stop()
print("SparkSession stopped cleanly.")
