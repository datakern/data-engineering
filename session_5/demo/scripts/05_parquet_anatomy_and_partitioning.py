"""
DataKern — Session 5 Demo
Script 05: Parquet Anatomy, Column Pruning, and Partitioning

What you will learn:
  1. Why big data abandoned CSV: Columnar storage vs Row-oriented storage
  2. What is inside a Parquet file: Row Groups, Column Chunks, and the Footer
  3. Disk size and query speed benchmark: CSV vs Parquet
  4. How partitionBy("column") creates directory hierarchies on disk
  5. How Partition Pruning skips unneeded folders at query time
  6. The Goldilocks Rule of partitioning to avoid the Small Files Problem

Prerequisites:
  - Spark running locally
  - Olist datasets in demo/data/ (or symlinked)

Run from demo/ folder:
  python scripts/05_parquet_anatomy_and_partitioning.py
"""

import os
import shutil
import sys
import time

# Prevent macOS binding errors
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ─────────────────────────────────────────────────────────────────
# SECTION 0 — Configuration & Helper Functions
# ─────────────────────────────────────────────────────────────────

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")
OUTPUT_DIR = os.path.join(BASE_DIR, "output")
ORDERS_CSV = os.path.join(DATA_DIR, "olist_orders_dataset.csv")

PARQUET_DIR = os.path.join(OUTPUT_DIR, "orders_plain_parquet")
PARTITIONED_DIR = os.path.join(OUTPUT_DIR, "orders_partitioned_parquet")

DIVIDER = "═" * 72
SUB_DIVIDER = "─" * 72


def pause():
    """Pauses execution to allow interactive exploration in the terminal and Spark UI."""
    input("\n🔍 Press ENTER to continue ➔ ")
    print("\n" + SUB_DIVIDER)


def get_dir_size(path):
    """Calculates total size of files inside a directory."""
    total = 0
    for root, dirs, files in os.walk(path):
        for f in files:
            fp = os.path.join(root, f)
            if not os.path.islink(fp):
                total += os.path.getsize(fp)
    return total


# ─────────────────────────────────────────────────────────────────
# SECTION 1 — Start the SparkSession
# ─────────────────────────────────────────────────────────────────

print("\n" + DIVIDER)
print("  DATAKERN SESSION 5: SCRIPT 05 — PARQUET ANATOMY & PARTITIONING")
print(DIVIDER)
print("""
THE REVOLUTION OF COLUMNAR STORAGE:
- CSV is ROW-ORIENTED:
    If a table has 50 columns and you only need 'order_id' and 'order_status',
    the hard drive still reads EVERY SINGLE BYTE of all 50 columns across every row.
- PARQUET is COLUMN-ORIENTED:
    Data for each column is stored contiguously in memory and on disk.
    If you query 2 columns, Spark reads ONLY those 2 columns!
    The other 48 columns are completely skipped.
""")

spark = SparkSession.builder \
    .appName("DataKern_Session5_05_Parquet_Anatomy") \
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
# SECTION 2 — Write Data to Parquet & Compare Disk Footprint
# ─────────────────────────────────────────────────────────────────

print("SECTION 2: Converting CSV to Parquet — Disk Footprint Comparison\n")

df_orders = spark.read.csv(ORDERS_CSV, header=True, inferSchema=True)
csv_size = os.path.getsize(ORDERS_CSV)

print(f"Original CSV file on disk: {csv_size / (1024 * 1024):.2f} MB")

# Clean existing output directory if it exists
if os.path.exists(PARQUET_DIR):
    shutil.rmtree(PARQUET_DIR)

print("\nWriting orders to Parquet (Snappy compression)...")
t0 = time.time()
df_orders.write.mode("overwrite").parquet(PARQUET_DIR)
time_write = time.time() - t0

parquet_size = get_dir_size(PARQUET_DIR)
reduction = (1 - (parquet_size / csv_size)) * 100

print(f"✅ Parquet write completed in {time_write:.2f}s.")
print(f"\n📊 DISK COMPARISON:")
print(f"   CSV Disk Size     : {csv_size / (1024 * 1024):.2f} MB")
print(f"   Parquet Disk Size : {parquet_size / (1024 * 1024):.2f} MB")
print(f"   Storage Reduction : {reduction:.1f}% space saved!")

print("\n📁 WHAT IS INSIDE THE PARQUET DIRECTORY?")
for entry in os.listdir(PARQUET_DIR):
    if not entry.startswith("."):
        print(f"   ├── {entry}")

print(f"""
🔎 WHY DOES SPARK WRITE DIRECTORIES, NOT SINGLE FILES?
Because Spark is distributed! Each partition writes its own slice of data 
into a separate 'part-*.parquet' file in parallel.
Notice: The number of part files corresponds to your DataFrame's partitions 
(which scales with your laptop's CPU core configuration).
The '_SUCCESS' file is an empty marker confirming all partition write tasks completed cleanly.
""")

pause()

# ─────────────────────────────────────────────────────────────────
# SECTION 3 — Column Pruning: The Columnar Advantage
# ─────────────────────────────────────────────────────────────────

print("SECTION 3: Column Pruning — Reading Only What You Need\n")

print("Let's benchmark reading just 2 columns ('order_id', 'order_status') from both formats:")

# Read from CSV
t0 = time.time()
csv_count = spark.read.csv(ORDERS_CSV, header=True, inferSchema=True) \
    .select("order_id", "order_status") \
    .filter(F.col("order_status") == "delivered") \
    .count()
time_csv = time.time() - t0

# Read from Parquet
t0 = time.time()
parquet_count = spark.read.parquet(PARQUET_DIR) \
    .select("order_id", "order_status") \
    .filter(F.col("order_status") == "delivered") \
    .count()
time_parquet = time.time() - t0

print(f"✅ CSV Query Time     : {time_csv:.3f}s")
print(f"✅ Parquet Query Time : {time_parquet:.3f}s")
speedup = (time_csv / time_parquet) if time_parquet > 0 else 1.0
print(f"🚀 Parquet was {speedup:.1f}x FASTER due to Column Pruning and Snappy decompression!")

pause()

# ─────────────────────────────────────────────────────────────────
# SECTION 4 — Directory Partitioning: partitionBy
# ─────────────────────────────────────────────────────────────────

print("SECTION 4: Partitioning by Column (partitionBy)\n")
print("""
WHAT IS PARTITIONING?
Instead of dumping all data into one directory, Spark splits the data into subdirectories
based on the value of a column (e.g. order_status).

Let's write orders partitioned by 'order_status':
""")

if os.path.exists(PARTITIONED_DIR):
    shutil.rmtree(PARTITIONED_DIR)

t0 = time.time()
df_orders.write.partitionBy("order_status").mode("overwrite").parquet(PARTITIONED_DIR)
time_part_write = time.time() - t0

print(f"✅ Partitioned write completed in {time_part_write:.2f}s.")
print(f"\n📁 INSPECTING THE GENERATED DIRECTORY STRUCTURE ON DISK:")
print(f"   Path: {PARTITIONED_DIR}")

for entry in sorted(os.listdir(PARTITIONED_DIR)):
    if not entry.startswith(".") and not entry.startswith("_"):
        sub_path = os.path.join(PARTITIONED_DIR, entry)
        sub_files = len(os.listdir(sub_path)) if os.path.isdir(sub_path) else 0
        print(f"   ├── {entry}/ ({sub_files} parquet files)")

print("""
🔎 LOOK AT THE DIRECTORY NAMES:
  'order_status=delivered/'
  'order_status=shipped/'
  'order_status=canceled/'
Notice the Hive-style 'column=value' folder naming convention!
Spark automatically detects this when reading the folder back.
""")

pause()

# ─────────────────────────────────────────────────────────────────
# SECTION 5 — Partition Pruning: Skipping Unneeded Directories
# ─────────────────────────────────────────────────────────────────

print("SECTION 5: Partition Pruning — Eliminating I/O Before File Scan\n")
print("""
WHEN YOU FILTER ON THE PARTITION COLUMN:
    spark.read.parquet(...).filter(F.col("order_status") == "delivered")

Spark's Catalyst optimizer doesn't open the shipped, canceled, or unavailable folders AT ALL!
It prunes them at the filesystem level.
""")

df_delivered_part = spark.read.parquet(PARTITIONED_DIR) \
    .filter(F.col("order_status") == "delivered")

print("Execution Plan for Partitioned Read:")
df_delivered_part.explain(mode="formatted")

print("""
🔎 NOTICE:
In the Scan node, look at 'PartitionFilters: [isnotnull(order_status), (order_status = delivered)]'.
Spark only reads the 'order_status=delivered/' folder!

⚠️ THE GOLDILOCKS RULE OF PARTITIONING:
1. GOOD PARTITION KEYS:
   - Low to medium cardinality (10 to 1,000 distinct values).
   - Examples: year, month, region, order_status.
2. BAD PARTITION KEYS (THE SMALL FILES TRAP):
   - High cardinality columns!
   - If you partition by `customer_id` (100,000 values) or `order_purchase_timestamp`:
     Spark creates 100,000 tiny folders with 5KB files.
     The cluster spends 99% of its time doing filesystem metadata lookups and crashes!
""")

pause()

# ─────────────────────────────────────────────────────────────────
# SECTION 6 — Clean Production Reference
# ─────────────────────────────────────────────────────────────────

print("SECTION 6: Clean Production Reference Code\n")
print("""
# ─────────────────────────────────────────────────────────────────
# PRODUCTION PARQUET PATTERNS:
# ─────────────────────────────────────────────────────────────────

# 1. Writing standard Parquet (Snappy compressed by default):
df.write \\
  .mode("overwrite") \\
  .parquet("output/curated_orders.parquet")

# 2. Writing Partitioned Parquet (by low/medium cardinality column):
df.write \\
  .partitionBy("order_year", "order_month") \\
  .mode("overwrite") \\
  .parquet("output/partitioned_orders.parquet")

# 3. Reading with Column Pruning (Only select columns you need):
df_subset = spark.read.parquet("output/curated_orders.parquet") \\
  .select("order_id", "order_status")

# 4. Partition Pruning in action (Fast directory skip):
df_recent = spark.read.parquet("output/partitioned_orders.parquet") \\
  .filter((F.col("order_year") == 2018) & (F.col("order_month") == 8))
""")

pause()

# ─────────────────────────────────────────────────────────────────
# SECTION 7 — Summary & Handoff
# ─────────────────────────────────────────────────────────────────

print("SECTION 7: Summary & What's Next\n")
print("""
🎉 SCRIPT 05 COMPLETE!
What you have seen with your own eyes:
  ✅ Parquet reduced disk footprint by ~70% compared to raw CSV.
  ✅ Column Pruning makes Parquet queries orders of magnitude faster.
  ✅ partitionBy creates key=value folder hierarchies on disk.
  ✅ Partition Pruning skips unneeded directories before any file I/O begins.

👉 NEXT UP:
Run Script 06 for the ultimate Lakehouse evolution: Delta Lake, ACID Transactions, 
Time Travel, Schema Evolution, and the Medallion Architecture!

Command to run:
  python scripts/06_delta_lake_and_medallion_architecture.py
""")

spark.stop()
print("SparkSession stopped cleanly.")
