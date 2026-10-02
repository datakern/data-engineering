"""
DataKern — Session 4 Demo
Script 05: The Spark UI — Seeing Every Concept Live

What you will learn:
  1. How to navigate the Spark UI at http://localhost:4040
  2. That lazy evaluation is visible — Jobs tab is empty until you call an action
  3. That one action = one Job in the UI
  4. That Jobs break into Stages at shuffle boundaries (joins, groupBys)
  5. That Stages contain Tasks — one task per data partition (the RDD in action)
  6. How to read the Catalyst DAG in the SQL/DataFrame tab

IMPORTANT: Open http://localhost:4040 in a browser before running this script.
The script will pause and wait for you to explore the UI at each step.
Press ENTER in the terminal to continue to the next step.

Run from the demo/ folder:
  python scripts/05_spark_ui_tour.py
"""

import os
import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ─────────────────────────────────────────────────────────────────
# SECTION 0 — Configuration
# ─────────────────────────────────────────────────────────────────

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")

DIVIDER = "\n" + "─" * 60


# ─────────────────────────────────────────────────────────────────
# SECTION 1 — SparkSession
# ─────────────────────────────────────────────────────────────────

print("\n" + "=" * 60)
print("SPARK UI TOUR")
print("=" * 60)

print("""
Before we start:
  1. Open your browser
  2. Go to: http://localhost:4040

  (The URL will appear below once Spark starts.)
""")

spark = SparkSession.builder \
    .appName("DataKern_SparkUI_Tour") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print(f"""
✅ SparkSession started.
{DIVIDER}
  Spark UI: http://localhost:4040
{DIVIDER}

Open that URL now. You should see the Spark UI with these tabs:

  Jobs | Stages | Storage | Environment | Executors | SQL/DataFrame

The Jobs tab should be EMPTY.
Nothing has happened yet — we haven't called any actions.
""")

input("   → Open the UI and look at the Jobs tab. Press ENTER when ready.")


# ─────────────────────────────────────────────────────────────────
# SECTION 2 — Read CSVs (no action → Jobs tab stays empty)
# ─────────────────────────────────────────────────────────────────

print("\n" + "=" * 60)
print("STEP 1: Read the CSV files (transformations — lazy)")
print("=" * 60)

df_orders = spark.read.csv(
    os.path.join(DATA_DIR, "olist_orders_dataset.csv"),
    header=True, inferSchema=True
)
df_items = spark.read.csv(
    os.path.join(DATA_DIR, "olist_order_items_dataset.csv"),
    header=True, inferSchema=True
)
df_products = spark.read.csv(
    os.path.join(DATA_DIR, "olist_products_dataset.csv"),
    header=True, inferSchema=True
)

print("""
✅ Three DataFrames created.

  Go to the Jobs tab in the browser.

  How many jobs are listed?

  Answer: Should be ZERO, if no action performed on the data yet.

  We called spark.read.csv() three times.
  That is a transformation — not an action.
  No data has been read. No job has been created.

  This is lazy evaluation — visible in the UI.
  Think and try to guess if you see any jobs in the UI after performing the read operations.
""")

input("   → Confirm the Jobs tab. Press ENTER to continue.")


# ─────────────────────────────────────────────────────────────────
# SECTION 3 — First action: .count() → Job 0 appears
# ─────────────────────────────────────────────────────────────────

print("\n" + "=" * 60)
print("STEP 2: First ACTION — df_orders.count()")
print("=" * 60)

print("""
  We are about to call df_orders.count().
  This is an action.

  Before running: predict what you will see in the Jobs tab.
    → How many jobs will appear?
    → Answer: exactly 1 (one action = one job)

  Keep the Jobs tab open and watch while the count runs.
""")

print("   Running df_orders.count() ...\n")
t0 = time.time()
count_result = df_orders.count()
elapsed = time.time() - t0

print(f"   Result : {count_result:,} rows in the orders file")
print(f"   Time   : {elapsed:.2f} seconds")

print(f"""
  Go to the Jobs tab now.

  You should see: New jobs created

  The job description mentions "count" — that is the action we called.
  Click on latest Jobs to see what it contains.

  What you will see inside Job:
    STAGES — the job was broken into stages.

  Each stage is a unit of work between two shuffle boundaries.
  A "shuffle" is when Spark needs to reorganize data across partitions
  (for example, to aggregate across all rows in a count).
""")

input("   → Click on Job → explore its stages. Press ENTER to continue.")


# ─────────────────────────────────────────────────────────────────
# SECTION 4 — Look at Tasks inside a Stage
# ─────────────────────────────────────────────────────────────────

print("\n" + "=" * 60)
print("STEP 3: Tasks inside a Stage — the RDD in action")
print("=" * 60)

print("""
  While still inside Job , click on one of its Stages.

  You will see a list of Tasks.

  What is a Task?
    One Task = one partition of the data being processed
               by one Executor (one CPU core in local mode)

  This is the RDD in action:
    The orders CSV was split into partitions when it was read.
    Spark assigned one Task per partition.
    Each Task ran on a local CPU core (our "executor").

  In a real cluster with 10 machines:
    Each Task would run on a different machine.
    That is horizontal scaling — visible here in the Tasks list.

  Key columns to look at:
    Task Index  → which task in this stage (0, 1, 2, ...)
    Status      → SUCCEEDED (the task finished without error)
    Duration    → how long this task took
    Input Size  → how much data this partition held
""")

input("   → Click into a Stage → explore the Tasks. Press ENTER to continue.")


# ─────────────────────────────────────────────────────────────────
# SECTION 5 — Complex pipeline: multiple stages from joins and groupBy
# ─────────────────────────────────────────────────────────────────

print("\n" + "=" * 60)
print("STEP 4: Complex pipeline — see how stages multiply")
print("=" * 60)

print("""
  Now we run the full Finance pipeline:
    filter → join → join → groupBy → agg

  Predict before running:
    How many stages will this job have?

    Hint: every join and every groupBy causes a shuffle.
    A shuffle = end of one stage, start of the next.

  Keep the Jobs tab open and count the stages when the job finishes.
""")

print("   Running the Finance pipeline ...\n")
t0 = time.time()

df_finance = df_items \
    .join(
        df_orders.filter(F.col("order_status") == "delivered")
                 .withColumn("order_month", F.date_trunc("month", F.col("order_purchase_timestamp"))),
        on="order_id", how="inner"
    ) \
    .join(df_products, on="product_id", how="inner") \
    .groupBy(F.col("order_month"), F.col("product_category_name")) \
    .agg(
        F.sum("price").alias("item_sales_value"),
        F.countDistinct("order_id").alias("distinct_orders")
    ) \
    .orderBy("order_month", F.desc("item_sales_value"))

df_finance.show(5)
elapsed = time.time() - t0

print(f"\n   Pipeline ran in {elapsed:.2f} seconds")

print("""
  Go to the Jobs tab. A new Job has appeared.
  Click into it and count the stages.

  You should see MORE stages than the simple count() job from Step 2.
  That is because:
    - Each join caused a shuffle (stage boundary)
    - The groupBy caused a shuffle (stage boundary)
    - Spark batched all the work between shuffles into one stage

  This is WHY lazy evaluation matters:
    Spark saw the full plan before running anything.
    Catalyst (Spark's optimizer) arranged the steps into
    the minimum number of shuffles — automatically.
""")

input("   → Click the new Job → count its stages. Press ENTER to continue.")


# ─────────────────────────────────────────────────────────────────
# SECTION 6 — SQL / DataFrame tab: the visual Catalyst DAG
# ─────────────────────────────────────────────────────────────────

print("\n" + "=" * 60)
print("STEP 5: SQL / DataFrame tab — the visual query plan")
print("=" * 60)

print("""
  Click the "SQL / DataFrame" tab at the top of the Spark UI.

  You will see a list of queries that have been executed.
  Click on the most recent one — the Finance pipeline.

  You will see a visual box-and-arrow diagram. This is the DAG:
    Directed Acyclic Graph — the actual execution plan Spark built.

  Read it from top to bottom:
    Scan CSV      → reading the source files
    Filter        → only delivered orders
    Project       → selecting and renaming columns
    SortMergeJoin → the join operation
    HashAggregate → the groupBy and agg
    Sort          → the orderBy

  Notice:
    The Filter step appears BEFORE the SortMergeJoin.
    You may have written the join first — but Catalyst moved the
    filter earlier to reduce the number of rows entering the join.
    This is called predicate pushdown — Spark did it automatically.

  The numbers in each box show:
    How many rows flowed through that operation.
    How long that operation took.

  This is the Catalyst optimizer's plan — not exactly what you wrote,
  but the most efficient version of what you asked for.
""")

input("   → Explore the SQL tab and read the DAG. Press ENTER to continue.")


# ─────────────────────────────────────────────────────────────────
# SECTION 7 — Executors tab: the architecture diagram — live
# ─────────────────────────────────────────────────────────────────

print("\n" + "=" * 60)
print("STEP 6: Executors tab — the architecture diagram made real")
print("=" * 60)

print("""
  Click the "Executors" tab at the top.

  You will see two rows:
    Driver      → your Python script — coordinates the job
    Executor 0  → your machine — does the actual computation

  In local mode, the Executor IS your machine (one CPU core pool).
  In a real cluster, you would see 10, 50, or 500 Executor rows here —
  one per worker machine. Each would list its own:
    Address     → where that machine is in the network
    Tasks       → how many tasks it completed
    Input       → total data it read

  This is the three-box architecture from the session slides — live:
    Driver (top) → coordinates
    Executor(s)  → do the work

  The Resources column shows how much memory and CPU this executor used.
""")

input("   → Look at the Executors tab. Press ENTER to continue.")


# ─────────────────────────────────────────────────────────────────
# SECTION 8 — Environment tab: Spark configuration
# ─────────────────────────────────────────────────────────────────

print("\n" + "=" * 60)
print("STEP 7: Environment tab — Spark's configuration")
print("=" * 60)

print("""
  Click the "Environment" tab.

  This shows the full configuration of this Spark session.
  Scroll through "Spark Properties" and find these:

    spark.master = local[*]
      → This is what we set in SparkSession.builder.
      → In a Databricks cluster, this would show the cluster address instead.

    spark.app.name
      → The name we gave in .appName("DataKern_SparkUI_Tour")

    spark.sql.adaptive.enabled = true
      → Adaptive Query Execution (AQE) is on.
      → This means Spark can adjust the number of partitions
        after a shuffle, based on actual data size.
        It is an additional optimization on top of Catalyst.

  In production, the Environment tab is useful for debugging
  configuration issues — if a pipeline behaves unexpectedly,
  this is where you check whether the settings are what you intended.
""")

input("   → Explore the Environment tab. Press ENTER to see the summary.")


# ─────────────────────────────────────────────────────────────────
# SECTION 9 — Summary: what the UI proved
# ─────────────────────────────────────────────────────────────────

print("\n" + "=" * 60)
print("STEP 8: Summary — what you observed in the Spark UI")
print("=" * 60)

print("""
  The Spark UI gave you visual proof of every concept from the session:

  CONCEPT              WHAT YOU SAW IN THE UI
  ─────────────────    ────────────────────────────────────────────────
  Lazy evaluation   →  Jobs tab was EMPTY before any action was called
  Actions           →  Each .count() / .show() created exactly ONE job
  Stages            →  Each job broke into stages at shuffle boundaries
  Tasks             →  Each stage had tasks — one per data partition
  RDD partitions    →  The Tasks tab showed each partition being worked
  Catalyst DAG      →  SQL tab showed the optimized visual plan
  Architecture      →  Executors tab showed Driver + Executor

  WHAT WOULD CHANGE ON A REAL CLUSTER:
  ─────────────────────────────────────────────────────────────────
  Executors tab     → 10, 50, or 500 workers listed (not just 1)
  Tasks per Stage   → more tasks running in parallel across machines
  Duration          → shorter for large data (parallel computation)
  spark.master      → a cluster address, not local[*]

  Everything you saw today in local mode is identical in concept
  to what you would see on a production Databricks cluster.
  The scale changes. The model stays the same.
""")

print("✅ Spark UI Tour complete.")
print()
print("   The SparkSession is still running.")
print(f"   You can keep exploring the UI at http://localhost:4040")
print("   Press ENTER to shut down the session and close the UI.")

input()

spark.stop()
print()
print("   SparkSession stopped.")
print("   The Spark UI at http://localhost:4040 is no longer available.")
print()
print("   You have now completed spark tour demo:")
print("     Script 05 → Spark UI tour")
