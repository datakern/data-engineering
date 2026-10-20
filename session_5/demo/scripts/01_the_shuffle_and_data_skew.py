"""
DataKern — Session 5 Demo
Script 01: The Shuffle and Data Skew — Seeing the Cost of Redistribution

What you will learn:
  1. The physical difference between Narrow and Wide transformations
  2. How Spark boundaries create Stages: Why groupBy triggers an Exchange (Shuffle)
  3. How to inspect Shuffle Write and Shuffle Read metrics in the Spark UI
  4. What Data Skew looks like in real production data (São Paulo state in Olist)
  5. How to spot straggler tasks using the Spark UI Task Metrics distribution table

Prerequisites:
  - Spark running locally
  - Olist datasets in demo/data/ (or symlinked)

Run from demo/ folder:
  python scripts/01_the_shuffle_and_data_skew.py
"""

import os
import sys
import time

# Prevent macOS binding errors
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ─────────────────────────────────────────────────────────────────
# SECTION 0 — Configuration & Helper Functions
# ─────────────────────────────────────────────────────────────────

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
CUSTOMERS_CSV = os.path.join(DATA_DIR, "olist_customers_dataset.csv")
ORDERS_CSV = os.path.join(DATA_DIR, "olist_orders_dataset.csv")

DIVIDER = "═" * 72
SUB_DIVIDER = "─" * 72


def pause():
    """Pauses execution to allow interactive exploration in the terminal and Spark UI."""
    input("\n🔍 Press ENTER to continue ➔ ")
    print("\n" + SUB_DIVIDER)


# ─────────────────────────────────────────────────────────────────
# SECTION 1 — Start the SparkSession & Open Spark UI
# ─────────────────────────────────────────────────────────────────

print("\n" + DIVIDER)
print("  DATAKERN SESSION 5: SCRIPT 01 — THE SHUFFLE & DATA SKEW")
print(DIVIDER)
print("""
ANALOGY FIRST:
Imagine 4 assistants sitting at 4 tables sorting 10,000 receipts.
- If each assistant simply crosses out invalid receipts (FILTER):
    No assistant needs to stand up or talk to anyone. Zero communication.
- If we now want ALL receipts from São Paulo at Table 1, and ALL receipts 
  from Rio at Table 2 (GROUP BY STATE):
    Assistants MUST stand up, walk across the room, and trade receipts.
    That is the SHUFFLE. In a distributed cluster, workers exchange gigabytes 
    of data over network cards and write temporary spill files to disk.
""")

spark = SparkSession.builder \
    .appName("DataKern_Session5_01_Shuffle_and_Skew") \
    .master("local[*]") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.ui.port", "4040") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print("✅ SparkSession initialized successfully.")
print(f"   Master: {spark.sparkContext.master}")
print(f"   Default Parallelism (CPU Cores): {spark.sparkContext.defaultParallelism}")
print(f"   💡 Note: local[*] mode uses your laptop's CPU threads ({spark.sparkContext.defaultParallelism} cores detected).")
print("      Task counts, partitions, and exact byte sizes will reflect your specific machine configuration.")
print("\n" + "!" * 72)
print("💡 ACTION REQUIRED: OPEN YOUR SPARK UI NOW")
print("   URL: http://localhost:4040")
print("   Keep this browser tab open alongside your terminal!")
print("!" * 72)

pause()

# ─────────────────────────────────────────────────────────────────
# SECTION 2 — Load Data & Verify Files
# ─────────────────────────────────────────────────────────────────

print("SECTION 2: Loading Olist Customers and Orders\n")

if not os.path.exists(CUSTOMERS_CSV) or not os.path.exists(ORDERS_CSV):
    print(f"🛑 Error: Could not find CSV files in {DATA_DIR}.")
    print("   Please ensure olist_customers_dataset.csv and olist_orders_dataset.csv exist.")
    spark.stop()
    sys.exit(1)

df_customers = spark.read.csv(CUSTOMERS_CSV, header=True, inferSchema=True)
df_orders = spark.read.csv(ORDERS_CSV, header=True, inferSchema=True)

print(f"✅ DataFrames loaded (schema inferred for demonstration):")
print(f"   Customers DataFrame schema: {len(df_customers.columns)} columns")
print(f"   Orders DataFrame schema   : {len(df_orders.columns)} columns")
print(f"\n   OBSERVE: Refresh the Spark UI 'Jobs' tab at http://localhost:4040")
print("   You will see small jobs that Spark used to infer schemas from the CSV headers.")

pause()

# ─────────────────────────────────────────────────────────────────
# SECTION 3 — Narrow Transformation: No Shuffle
# ─────────────────────────────────────────────────────────────────

print("SECTION 3: Narrow Transformation (filter + select)\n")
print("🧠 PREDICT:")
print("   If we filter df_orders for 'delivered' status and select 3 columns,")
print("   does Spark need to move data between CPU cores or across the network?")
pause()

t0 = time.time()
df_delivered = df_orders \
    .filter(F.col("order_status") == "delivered") \
    .select("order_id", "customer_id", "order_status")

delivered_count = df_delivered.count()
elapsed_narrow = time.time() - t0

print(f"✅ Filter completed in {elapsed_narrow:.3f}s. Delivered count: {delivered_count:,}")
print("\n🔎 CONFIRMATION & EXPLANATION:")
print("   1. THE FILTER IS 100% NARROW:")
print("      Each partition filters its records in local RAM independently.")
print("      Worker cores never need to exchange raw order rows with each other.")
print("")
print("   2. WHY df.count() CREATES A 2-STAGE AGGREGATION:")
print("      In the Spark DataFrame API (Catalyst), df.count() executes as SQL 'SELECT COUNT(1)'.")
print("      Spark evaluates this using a 2-phase aggregation:")
print("        • Map Stage (N tasks, one per input partition):")
print("          Counts matching rows locally within each partition. Zero raw rows are shuffled!")
print("          Each partition writes only 1 subtotal integer to shuffle storage.")
print("        • Reduce Stage (1 task):")
print("          Reads those N subtotal numbers and sums them to produce the grand total.")
print("      Notice the Shuffle Write/Read: it is only a few hundred BYTES (e.g. ~200-400 B for the subtotals),")
print("      NOT megabytes of raw orders!")
print("")
print("   👀 OBSERVE IN SPARK UI (http://localhost:4040):")
print("   1. Click on the 'Jobs' tab. You will see the job(s) triggered by count().")
print("   2. Click on the 'Stages' tab:")
print("      - Find the stage with multiple tasks (e.g., 2 to 8 tasks, depending on your machine and file splits).")
print("        Notice 'Shuffle Write' is tiny (~a few hundred bytes).")
print("      - Find the final stage with 1 task: it has 'Shuffle Read' matching the written bytes.")
print("      - (If you see 'Skipped Stages: 1', that is Spark 3.x Adaptive Query Execution optimizing stage submission!)")
print("   3. Click on the 'SQL / DataFrame' tab and click on the 'count at ...' query:")
print("      Notice the plan: HashAggregate (partial_count) ➔ Exchange SinglePartition ➔ HashAggregate (count).")

pause()

# ─────────────────────────────────────────────────────────────────
# SECTION 4 — Wide Transformation: Triggering a Shuffle
# ─────────────────────────────────────────────────────────────────

print("SECTION 4: Wide Transformation (groupBy customer_state)\n")
print("🧠 PREDICT:")
print("   To count customers by state (groupBy('customer_state').count()),")
print("   can a worker finish without sending data to other workers?")
print("   How many Stages will Spark divide this Job into?")
pause()

t0 = time.time()
# Wide transformation: groupBy requires repartitioning by customer_state
df_state_counts = df_customers \
    .groupBy("customer_state") \
    .count() \
    .orderBy(F.desc("count"))

print("Top 5 Brazilian States by Customer Count:")
df_state_counts.show(5)
elapsed_wide = time.time() - t0

print(f"✅ GroupBy and OrderBy completed in {elapsed_wide:.3f}s.")
print("\n🔎 CONFIRMATION & EXPLANATION:")
print("   GroupBy is a true WIDE transformation.")
print("   Unlike count() which only passed a few subtotal bytes, here Spark MUST redistribute")
print("   actual customer records so that every record with customer_state = 'SP' lands on the SAME worker.")
print("   This boundary cuts the execution into stages via an 'Exchange hashpartitioning' operator.")
print("\n   👀 OBSERVE IN SPARK UI (http://localhost:4040):")
print("   1. Click the 'Stages' tab:")
print("      Notice the Shuffle Write and Shuffle Read columns now show significant data (kilobytes or megabytes),")
print("      representing full customer records being shuffled, not just subtotal numbers!")
print("   2. Click on the 'SQL / DataFrame' tab and click on the query execution.")
print("      Notice the blue box titled 'Exchange hashpartitioning(customer_state)'.")
print("      That blue box IS the physical shuffle.")

pause()

# ─────────────────────────────────────────────────────────────────
# SECTION 5 — Data Skew: When One Worker Does All the Heavy Lifting
# ─────────────────────────────────────────────────────────────────

print("SECTION 5: Data Skew Live Demonstration\n")
print("""
WHAT IS DATA SKEW?
Data skew occurs when the partition key is not evenly distributed in the real world.
In Brazil:
  - São Paulo (SP) is the economic capital.
  - What percentage of Brazilian e-commerce customers live in SP alone?
""")

total_customers = df_customers.count()
sp_customers = df_customers.filter(F.col("customer_state") == "SP").count()
sp_percentage = (sp_customers / total_customers) * 100

print(f"📊 Real-World Distribution:")
print(f"   Total customers in Brazil : {total_customers:,}")
print(f"   Customers in SP alone     : {sp_customers:,} ({sp_percentage:.1f}% of entire country!)")
print(f"   Customers in RR (Roraima) : {df_customers.filter(F.col('customer_state') == 'RR').count():,}")

print("""
🧠 PREDICT:
If Spark hashes partitions by `customer_state`:
- One task gets 41,000+ customer records to process.
- Another task gets 46 customer records.
What will happen to the Task completion time in the Spark UI?
Will the cluster run at the speed of its fastest core, or its slowest core?
""")
pause()

# Repartition explicitly by state to simulate skewed partition processing across cores
cores = spark.sparkContext.defaultParallelism
print(f"Executing heavy aggregation across your laptop's {cores} CPU cores (partitions)...")
df_skewed = df_customers.repartition(cores, "customer_state")

t0 = time.time()
skew_result = df_skewed.groupBy("customer_state").agg(
    F.count("customer_id").alias("total_cust"),
    F.countDistinct("customer_zip_code_prefix").alias("unique_zipcodes")
).collect()
elapsed_skew = time.time() - t0

print(f"✅ Skewed aggregation completed in {elapsed_skew:.3f}s.")
print("\n🔎 CONFIRMATION & EXPLANATION:")
print("   The pipeline is only as fast as its SLOWEST task (the straggler).")
print(f"   Cores processing tiny states (RR, AP, AC) finish in milliseconds and sit idle,")
print("   while whichever core was assigned 'SP' is bottlenecked by CPU and memory!")
print("\n   👀 HOW TO DETECT SKEW IN SPARK UI (http://localhost:4040):")
print("   1. Go to the 'Stages' tab.")
print("   2. Click on the Stage corresponding to the aggregation above.")
print("   3. Scroll down to 'Summary Metrics for Completed Tasks':")
print("      - Look at the row: 'Duration'")
print("      - Compare 'Min' (fastest task) vs 'Median' vs 'Max' (slowest task).")
print("      - In a healthy pipeline without skew: Min ≈ Median ≈ Max.")
print("      - In a SKEWED pipeline: Max is several times larger than Median!")
print("   4. Look at the 'Event Timeline' at the top of the Stage detail page:")
print("      Notice the uneven horizontal bars — one long task bar while others finish quickly.")

pause()

# ─────────────────────────────────────────────────────────────────
# SECTION 6 — Clean Production Reference
# ─────────────────────────────────────────────────────────────────

print("SECTION 6: Clean Production Reference Code\n")
print("""
Here is the clean pattern for monitoring and thinking about Shuffles:

# ─────────────────────────────────────────────────────────────────
# PRODUCTION RULE:
# 1. Narrow transformations (filter, select, withColumn) cost almost nothing in network I/O.
# 2. Wide transformations (groupBy, join, distinct, repartition) trigger a Shuffle Exchange.
# 3. Always filter FIRST (Narrow) before joining or grouping (Wide) to minimize Shuffle Write!
# ─────────────────────────────────────────────────────────────────

# GOOD (Filter first, shuffle less data):
df_clean = df_orders.filter(F.col("order_status") == "delivered")
df_summary = df_clean.groupBy("order_status").count()

# IN SPARK UI:
# Always check Stage Summary Metrics:
# If Max Duration >> 3 * Median Duration, you have DATA SKEW on your group/join key.
""")

pause()

# ─────────────────────────────────────────────────────────────────
# SECTION 7 — Summary & Handoff
# ─────────────────────────────────────────────────────────────────

print("SECTION 7: Summary & What's Next\n")
print(f"""
🎉 SCRIPT 01 COMPLETE!
What you have seen with your own eyes:
  ✅ Filter and Select = 100% Narrow (0 raw rows exchanged; count() aggregates via a 2-stage partial sum).
  ✅ GroupBy = Wide transformation separated by an Exchange node, redistributing full records by key.
  ✅ Data Skew = São Paulo holds 41.7% of data, causing uneven task duration (stragglers) across your CPU cores.

👉 NEXT UP:
Run Script 02 to learn how to eliminate shuffles using Broadcast Joins, 
how to read the Catalyst Optimizer execution plan, and how to cache in memory!

Command to run:
  python scripts/02_join_strategies_and_catalyst_optimizer.py
""")

spark.stop()
print("SparkSession stopped cleanly.")
