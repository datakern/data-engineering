"""
DataKern — Session 4 Demo
Script 01: Hello Spark — SparkSession, Reading Data, Inspecting

What you will learn:
  1. How to start a SparkSession — Spark's entry point
  2. How to read a CSV file into a Spark DataFrame
  3. How to inspect the schema, view sample rows, and count rows
  4. Why .show() feels slower than you expect (it's doing more than showing)

Expected runtime: 20–40 seconds on first run (JVM startup takes a moment).
Subsequent scripts run faster once the JVM is warm.

Run from the demo/ folder:
  python scripts/01_hello_spark.py
"""

import os
import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ─────────────────────────────────────────────────────────────────
# SECTION 0 — Configuration
# ─────────────────────────────────────────────────────────────────

# Adjust this path if your CSV files are somewhere else
DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")


def pause():
    input("\n🔍 Press ENTER to continue ➔ ")
    print("\n" + "─" * 70)


# ─────────────────────────────────────────────────────────────────
# SECTION 1 — Start the SparkSession
# ─────────────────────────────────────────────────────────────────

print("\n" + "=" * 70)
print("SECTION 1: Starting the SparkSession")
print("=" * 70)

# The SparkSession is your entry point to Spark.
# Compare it to psycopg2.connect() for PostgreSQL, or pd.read_csv() for Pandas:
# you always open the connection before doing any work.
#
# master("local[*]") means:
#   → Run on this machine
#   → Use ALL available CPU cores
# In a cloud cluster (Databricks), this one line changes to the cluster address.
# Everything else stays exactly the same.
spark = SparkSession.builder \
    .appName("DataKern_Session4_HelloSpark") \
    .master("local[*]") \
    .getOrCreate()

# Suppress verbose Spark internal logs — keep output focused on what matters
spark.sparkContext.setLogLevel("ERROR")

print(f"\n✅ SparkSession started.")
print(f"   App name : {spark.sparkContext.appName}")
print(f"   Master   : {spark.sparkContext.master}")
print(f"\n   💡 OBSERVE: The Spark UI is now running at: http://localhost:4040")
print(f"      Open that link in your browser right now.")
print(f"      Keep it open side-by-side with this terminal.")
print(f"      Look at the 'Jobs' tab. It should be completely empty.")

pause()

# ─────────────────────────────────────────────────────────────────
# SECTION 2 — Read a CSV file
# ─────────────────────────────────────────────────────────────────

print("SECTION 2: Reading the orders CSV into a DataFrame\n")

orders_path = os.path.join(DATA_DIR, "olist_orders_dataset.csv")

# spark.read.csv() creates a DataFrame — a distributed, structured table.
# Compare with Pandas: pd.read_csv("orders.csv")
# Same intent. Very different engine.
#
# inferSchema=True: Spark reads the file twice — once to guess column types,
# once to actually load the data. This is fine for learning but costly at scale.
# In production you define the schema explicitly so Spark reads the file once.
df_orders = spark.read.csv(
    orders_path,
    header=True,
    inferSchema=True
)

print(f"✅ spark.read.csv() returned.")
print(f"   Path: {orders_path}")
print(f"   Type: {type(df_orders)}")
print(f"\n   OBSERVE: Check the Spark UI 'Jobs' tab again and refresh.")
print(f"   Did any jobs run? Yes, multiple small jobs ran just to")
print(f"   figure out the column names and types (inferSchema), but the full data")
print(f"   has NOT been processed yet.")

pause()

# ─────────────────────────────────────────────────────────────────
# SECTION 3 — Inspect the schema
# ─────────────────────────────────────────────────────────────────

print("SECTION 3: Inspect the column names and types\n")

# printSchema() shows column names and their inferred data types.
# Compare with Pandas: df.dtypes
#
# Notice the types Spark inferred: string, timestamp, integer.
# Remember Session 3: you defined explicit column types in PostgreSQL.
# Same discipline applies here — types are not decoration.
# They are a correctness and performance contract.
df_orders.printSchema()

print("💡 Spark inferred these types from the CSV data.")
print("   In Session 3 you defined PostgreSQL types explicitly.")
print("   In production Spark, you do the same — explicit is better than inferred.")

pause()

# ─────────────────────────────────────────────────────────────────
# SECTION 4 — Show sample rows (your first true ACTION)
# ─────────────────────────────────────────────────────────────────

print("SECTION 4: View sample rows — df.show()\n")

print("PREDICT: Before running the next line, think about this:")
print("   We are about to run df_orders.show(5).")
print("   This is an ACTION.")
print("   The moment it runs, Spark will:")
print("     1. Read the entire CSV file (if it hasn't cached it)")
print("     2. Return the first 5 rows to display")

input("\n🔍 Press ENTER to trigger the ACTION ➔ ")

t0 = time.time()
df_orders.show(5, truncate=False)
elapsed = time.time() - t0

print(f"\n⏱️  Completed in {elapsed:.2f} seconds")
print("   OBSERVE: Check the Spark UI 'Jobs' tab and refresh.")
print("   You should see a new Job appear because .show() is an ACTION.")

pause()

# ─────────────────────────────────────────────────────────────────
# SECTION 5 — Count rows (another ACTION)
# ─────────────────────────────────────────────────────────────────

print("SECTION 5: Count all rows — df.count()\n")

print("PREDICT: We are about to run df_orders.count().")
print("Will this create a new Spark Job?")

input("\n🔍 Press ENTER to find out ➔ ")

# .count() is another action — it triggers another read of the file.
# Compare with Pandas: len(df)
row_count = df_orders.count()

print(f"\n   Total rows in orders: {row_count:,}")
print()
print("   OBSERVE: Refresh the Spark UI. A new Job just completed!")
print("   💡 Notice .count() ran again from scratch.")
print("      Spark did not cache the data from Section 4.")
print("      By default, Spark recomputes every time unless you explicitly cache.")

pause()

# ─────────────────────────────────────────────────────────────────
# SECTION 6 — Group by order status to summarise the data
# ─────────────────────────────────────────────────────────────────

print("SECTION 6: What order statuses exist in the dataset?\n")

print("We will run this pipeline:")
print("   .groupBy(\"order_status\")   ← transformation (lazy — just a plan)")
print("   .count()                   ← transformation (in this context)")
print("   .orderBy(F.desc(\"count\"))  ← transformation")
print("   .show()                    ← ACTION (this triggers everything)")

input("\n🔍 Press ENTER to run the pipeline ➔ ")

df_orders.groupBy("order_status") \
    .count() \
    .orderBy(F.desc("count")) \
    .show()

print("\nOBSERVE: Refresh the Spark UI one last time to see the new Job.")

print("\n" + "=" * 70)
print("✅ Script 01 complete.")
print("=" * 70)
print()
print("   What you observed in this script:")
print("     • Started a SparkSession (the entry point to Spark)")
print("     • Read a CSV into a Spark DataFrame (lazy — no data moved yet)")
print("     • Inspected the schema with printSchema()")
print("     • Called .show() — your first action — and watched Spark execute")
print("     • Counted rows and grouped by status")
print()
print("   Key takeaway:")
print("     spark.read.csv() + printSchema() = lazy (plan only)")
print("     .show() and .count() = actions (actually execute and create Jobs)")
print()
print("   Next → run python scripts/02_dataframe_pipeline.py")

print("\n" + "─" * 70)
print("🛑 Before you leave: The Spark UI is still running at http://localhost:4040.")
print("Take a final look around. When you press ENTER below, the script will call")
print("spark.stop(). The JVM will shut down, and the UI will disappear immediately.")
input("\n🔍 Press ENTER to stop the SparkSession and exit ➔ ")

spark.stop()
print("✅ SparkSession stopped. See you in the next script!")
