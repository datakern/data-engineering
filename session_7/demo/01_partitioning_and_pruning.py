"""
DataKern | Session 7 | Demo 1 of 5 | PARTITIONING: ONE FOLDER PER MONTH, AND HOW TO READ LESS
Slide 13 (Act 2: Storage)

Our question for the whole session: WHERE DO THE ROWS LIVE, AND HOW MANY TIMES DO THEY MOVE?
Today we split the orders table into folders and watch Spark open fewer files. You will see with
numbers (not just pictures) how Spark reads LESS data, and the one mistake that stops it.

  Step 1  Start Spark and meet the orders table
  Step 2  partitionBy: one folder per month
  Step 3  Partition pruning, and the trap that defeats it
  Step 4  Choosing a partition key
  Step 5  Two ways to read less

Run:   python 01_partitioning_and_pruning.py          (about 7 minutes)
Next:  02_repartition_and_tidy_write.py
"""
from pyspark.sql import functions as F

import demo_utils as u

# ============================================================== STEP 1
u.header(1, "Start Spark and meet the orders table")
print("First run only: Spark downloads the Delta Lake jars (1 to 2 minutes). Later runs start fast.")
spark = u.get_spark("S7 demo 1: partitioning and pruning")
u.pause("Open the Spark UI address above in your browser, then press ENTER")

orders = u.build_orders(spark)
u.sample("the orders table: ONE ROW = ONE ORDER", orders, n=5)
print(f"Rows: {orders.count():,}   Columns: {len(orders.columns)}")
u.why("Every demo today uses this table. 'order_value' is the money, 'year_month' is the column we will partition by.")
u.pause()

# ============================================================== STEP 2
u.header(2, "partitionBy: one folder per month")
print("Partitioning splits the table into FOLDERS, and the choice of column is yours.")
u.predict("We partition by year_month. Our data spans about 25 months. How many folders will appear?")

by_month_dir = u.fresh_dir("s01", "orders_by_month")
orders.write.partitionBy("year_month").parquet(u.uri(by_month_dir))
folders = [p for p in by_month_dir.iterdir() if p.is_dir()]
u.ok(f"Written: {len(folders)} folders, {u.count_files(by_month_dir)} files in total")
u.show_tree(by_month_dir, max_items=6)
print("\n   Look inside one folder:")
u.show_tree(sorted(folders)[8], max_items=4, indent="      ")
u.observe(
    "your folder demo_output/s01/orders_by_month/",
    "folders named year_month=2017-11 and so on, each with part-*.parquet files. Count the files per folder",
    "the month is now in the FOLDER NAME, not in the file. Notice several files per folder: that is the small-file problem, demo 2 fixes it.",
)
u.pause()

# ============================================================== STEP 3
u.header(3, "Partition pruning, and the trap that defeats it")
print("Three queries, all asking about March 2018. Only the first is written the way partitioning wants.")
u.predict("How many files will each query open? (a) year_month = '2018-03'  (b) purchase_ts in March  (c) no filter")

table = spark.read.parquet(u.uri(by_month_dir))
q_all = table
q_prune = table.where(F.col("year_month") == "2018-03")
q_trap = table.where((F.col("purchase_ts") >= "2018-03-01") & (F.col("purchase_ts") < "2018-04-01"))

u.scan_report(spark, q_all, "(c) no filter          ")
u.scan_report(spark, q_prune, "(a) year_month = ...   ")
u.scan_report(spark, q_trap, "(b) purchase_ts in March")
print("\n   The plan of (a) and of (b), only the interesting line:")
print("   (a)", end=" ")
u.show_plan_lines(q_prune, "PartitionFilters")
print("   (b)", end=" ")
if not u.show_plan_lines(q_trap, "PartitionFilters"):
    print("       ↑ no PartitionFilters line at all: there is nothing Spark can use to skip a folder")

u.observe(
    "Spark UI ➜ SQL tab ➜ the three 'Scan parquet' boxes",
    "'number of files read' and 'number of partitions read'. (a) opens one folder. (b) opens ALL of them even though it asks for the same month",
    "Spark prunes only when you filter on the PARTITION COLUMN itself. purchase_ts is not the partition column, so Spark cannot know which folders to skip.",
)
u.why("This is the most common partitioning bug in real projects: the data is partitioned, but the query does not use the partition column.")
u.pause()

# ============================================================== STEP 4
u.header(4, "Choosing a partition key")
print("Rule from the slide: FEW values, filtered OFTEN. Let us count the distinct values of four candidates (no writing here).")
u.predict("Which of year_month, state, customer_unique_id, order_id would make a sensible folder structure?")

counts = orders.agg(*[F.countDistinct(c).alias(c) for c in ["year_month", "state", "customer_unique_id", "order_id"]]).collect()[0]
for c in ["year_month", "state", "customer_unique_id", "order_id"]:
    n = counts[c]
    verdict = "✅ sensible" if n < 100 else "🛑 would create that many folders of tiny files"
    print(f"   {c:<20} {n:>8,} distinct values   {verdict}")
u.observe(
    "the terminal above",
    "year_month and state are small. The other two would create ~100,000 folders",
    "a folder per customer means 100k folders with one tiny file each: slower than no partitioning at all.",
)
u.pause()

# ============================================================== STEP 5
u.header(5, "Two ways to read less")
print("Putting it together, with the numbers from this demo:\n")
only_value = spark.read.parquet(u.uri(by_month_dir)).select("order_value")
print("   1. Partition pruning   you choose partitionBy and filter on that column")
print("      → step 3: 1 folder (5 files) opened instead of 25 folders (116 files)")
print("   2. Column pruning      Catalyst reads only the columns you use. The plan says so:")
u.show_plan_lines(only_value, "ReadSchema")
u.why("Both mean fewer bytes read, and fewer bytes read means a faster and cheaper query.")

u.reference(
    '''
# --- the clean reference: demo 1 without pauses --------------------------------
orders = build_orders(spark)                      # one row per order

# partition on disk, and prune
orders.write.partitionBy("year_month").parquet("orders_by_month")
spark.read.parquet("orders_by_month").where(F.col("year_month") == "2018-03")   # opens 1 folder
# WRONG column: filtering purchase_ts opens every folder

# column pruning
spark.read.parquet("orders_by_month").select("order_value").explain()           # ReadSchema: only order_value
'''
)
print("\n➡️  NEXT: python 02_repartition_and_tidy_write.py")
print("   Today you saw the small-file problem in step 2. Demo 2 fixes it with repartition, and builds the BRONZE layer.")
u.finish(spark)
