"""
DataKern | Spark session demos | 02  PARQUET, PARTITIONING, REPARTITION vs COALESCE   (slides 13 to 16)

Story: Olist stores everything as CSV. We move it to Parquet, look INSIDE a Parquet file,
lay it out in folders by month, and learn the difference between the partitions in
MEMORY (repartition/coalesce) and the partitions on DISK (partitionBy).

How every step runs:
  📥 INPUT DATA   the table the query works on  ➜ explain the query on this data
  🤔 PREDICT      a question to answer before the query runs (press ENTER to run)
  📤 OUTPUT       what the query produced       ➜ ENTER: then the OBSERVE tells you what to look at

Keep these open while you run it:
  Spark UI ➔ Stages tab: look at the "Input Size" column
  Spark UI ➔ SQL tab: open a query and look at the scan node ("number of files read")
  File Explorer / Finder: the folder  demo_output/   (watch the folders appear)
Run:   python 02_parquet_and_partitioning.py          (about 6-10 minutes)
"""
import os
import time

import demo_utils as u
from pyspark.sql import functions as F

RUN_QUERY = "Press ENTER to run the query"
SEE_OBSERVE = "Press ENTER to see what to observe"

spark = u.get_spark("02 parquet and partitioning")

# ------------------------------------------------------------------------------------------
u.header(1, "CSV has no types: inferSchema guesses (and costs a full extra pass)")
csv_path = str(u.DATA_DIR / "olist_customers_dataset.csv")
print("📥 INPUT DATA: the raw CSV file. It is plain text: no column types are stored anywhere")
u.peek_file("olist_customers_dataset.csv", n=5)
print("\n   Look at customer_zip_code_prefix: zip codes can start with a zero.")
u.predict("If Spark guesses the type of customer_zip_code_prefix, what will it pick? What happens to the zip '01046'?")
with u.Timer("read with inferSchema=True "):
    guessed = spark.read.csv(csv_path, header=True, inferSchema=True)
with u.Timer("read with a declared schema"):
    declared = spark.read.csv(csv_path, header=True, schema="customer_id STRING, customer_unique_id STRING, customer_zip_code_prefix STRING, customer_city STRING, customer_state STRING")
print("\n📤 OUTPUT: what Spark made of the zip code column")
print("   guessed type :", dict(guessed.dtypes)["customer_zip_code_prefix"])
print("   declared type:", dict(declared.dtypes)["customer_zip_code_prefix"])
zip_guess = [r[0] for r in guessed.select("customer_zip_code_prefix").limit(3).collect()]
zip_decl = [r[0] for r in declared.select("customer_zip_code_prefix").limit(3).collect()]
print(f"   first zips guessed : {zip_guess}   (leading zeros are GONE if this is a number)")
print(f"   first zips declared: {zip_decl}")
u.pause(SEE_OBSERVE)
u.observe(
    "Terminal, the 'guessed' and 'declared' lines just printed",
    "Compare the type of customer_zip_code_prefix and the first zip codes. Did a leading zero disappear (01046 vs 1046)?",
    "A guessed number type silently destroys the leading zero. A declared STRING keeps the zip code intact.",
)
u.observe(
    "Spark UI ➜ Jobs tab, the first jobs at the bottom of the list",
    "A job named 'csv at ...' exists only because of the inferSchema read: Spark had to read the file to guess the types. "
    "The declared-schema read did not need one.",
    "inferSchema costs a full extra pass over the file, and it repeats on every read.",
)
u.why("CSV does not store types, so Spark re-guesses on every read. Parquet stores the schema inside the file.")
u.pause()

# ------------------------------------------------------------------------------------------
u.header(2, "Build a bigger sales table and save it as CSV and as Parquet (slide 13)")
AMPLIFY = int(os.environ.get("DEMO_AMPLIFY", "10"))
base = u.build_sales(spark).withColumn("year_month", F.date_format("purchase_ts", "yyyy-MM"))
sales_big = (
    base.withColumn("copy", F.explode(F.sequence(F.lit(1), F.lit(AMPLIFY))))
    .withColumn("order_id", F.concat_ws("-", "order_id", "copy"))
    .withColumn("price", F.round(F.col("price") * (1 + F.rand(7) * 0.2), 2))  # vary the copies so they are not identical
    .drop("copy")
    .repartition(4)
)
u.sample(
    f"'sales' from demo 1 with a year_month column. We repeat every row {AMPLIFY} times (small random price changes) to get a bigger table",
    base.select("order_id", "category", "price", "state", "year_month"),
)
u.predict("How much smaller is the Parquet folder? Twice? Ten times?")
n = sales_big.count()
u.ok(f"sales_big has {n:,} rows (Olist x {AMPLIFY}, with small random price changes). Real data is bigger, but the idea is the same.")
csv_dir, pq_dir = u.fresh_dir("s02", "sales_csv"), u.fresh_dir("s02", "sales_parquet")
with u.Timer("write CSV    "):
    sales_big.write.csv(u.uri(csv_dir), header=True)
with u.Timer("write Parquet"):
    sales_big.write.parquet(u.uri(pq_dir))
csv_mb, pq_mb = u.folder_size_mb(csv_dir), u.folder_size_mb(pq_dir)
print("\n📤 OUTPUT: the two folders on disk (the first files of each)")
print("   sales_csv:")
u.show_tree(csv_dir, 4)
print("   sales_parquet:")
u.show_tree(pq_dir, 4)
print(f"\n   CSV     : {csv_mb:6.1f} MB")
print(f"   Parquet : {pq_mb:6.1f} MB   ({csv_mb / pq_mb:.1f}x smaller)")
u.pause(SEE_OBSERVE)
u.observe(
    "File Explorer / Finder ➜ demo_output/s02/ ➜ open sales_csv and sales_parquet",
    "Check the folder sizes (right click ➜ Get Info / Properties) against the numbers printed above. Open a CSV part file in a text editor "
    "(you can read it). Then try the .parquet file (binary, unreadable by eye).",
    "Parquet stores each column compressed and typed. The same rows take a fraction of the space.",
)
u.pause()

# ------------------------------------------------------------------------------------------
u.header(3, "Same question on CSV and on Parquet: read ONE column (slide 13)")
sales_csv = spark.read.csv(u.uri(csv_dir), header=True, schema=sales_big.schema)
sales_pq = spark.read.parquet(u.uri(pq_dir))
u.sample("ONE row of the table (it has 14 columns). We only need the 'price' column", sales_pq, n=1, truncate=22, vertical=True)
u.predict("We only need the 'price' column out of 14. Which read touches less data?")
def run_sum(df):
    t = time.time()
    df.agg(F.sum("price")).collect()
    return time.time() - t
run_sum(sales_csv), run_sum(sales_pq)  # warm-up run
t_csv, t_pq = run_sum(sales_csv), run_sum(sales_pq)
price_mb, all_mb = u.parquet_mb(pq_dir, ["price"]), u.parquet_mb(pq_dir)
print(f"\n   SUM(price) on CSV     : {t_csv:5.2f}s   must read all {csv_mb:5.1f} MB (every byte of every line)")
print(f"   SUM(price) on Parquet : {t_pq:5.2f}s   must read only the price column: {price_mb:4.1f} MB of {all_mb:.1f} MB in the files")
print(f"   ➜ Parquet reads {csv_mb / max(price_mb, 0.01):.0f}x fewer bytes here and was {t_csv / max(t_pq, 0.01):.1f}x faster")
u.result(
    "SUM(price): the same answer from the CSV and the Parquet copy",
    sales_csv.agg(F.round(F.sum("price"), 2).cast("decimal(18,2)").alias("sum_price_from_csv")).crossJoin(
        sales_pq.agg(F.round(F.sum("price"), 2).cast("decimal(18,2)").alias("sum_price_from_parquet"))
    ),
)
print("The plan looks the same for both (each asks for just the 'price' column):")
u.show_plan_lines(sales_csv.select("price"), "ReadSchema", mode="formatted")
u.show_plan_lines(sales_pq.select("price"), "ReadSchema", mode="formatted")
u.pause(SEE_OBSERVE)
u.observe(
    "Terminal, the two 'SUM(price)' lines just printed",
    "Compare the MB each version must read (all of the CSV vs the price column only) and the speed-up factor.",
    "Column pruning: Parquet keeps columns in separate blocks, so 13 of 14 columns are never read from disk.",
)
u.observe(
    "Spark UI ➜ SQL / DataFrame tab ➜ the newest queries ➜ click the scan node",
    "'ReadSchema' lists only price in both. For CSV every byte is still read. (In local mode the Stages tab 'Input Size' of Parquet jobs can "
    "under-report, so trust the MB we computed from the Parquet footers.)",
    "Asking for fewer columns helps a lot on Parquet and barely at all on CSV.",
)
u.why("CSV rows are text: to find the price column Spark must still read every byte of every line. Parquet stores each column in its own block, so the other 13 columns are never read from disk.")
u.pause()

# ------------------------------------------------------------------------------------------
u.header(4, "Inside a Parquet file: row groups and min/max statistics (slide 14)")
import pyarrow.parquet as pq

def write_one_file(df, name):
    d = u.fresh_dir("s02", name)
    df.coalesce(1).write.option("parquet.block.size", 1024 * 1024).parquet(u.uri(d))
    f = [os.path.join(d, x) for x in os.listdir(d) if x.endswith(".parquet")][0]
    return f

def footer_table(path, col="price"):
    md = pq.ParquetFile(path).metadata
    ci = [md.schema.column(i).name for i in range(md.num_columns)].index(col)
    rows = []
    for g in range(md.num_row_groups):
        st = md.row_group(g).column(ci).statistics
        rows.append((g, md.row_group(g).num_rows, st.min, st.max))
    return rows

def show_footer(rows, limit=500.0, max_rows=8):
    skippable = 0
    print(f"   {'row group':>9} | {'rows':>8} | {'min price':>10} | {'max price':>10} | filter: price > {limit:.0f}")
    for g, nrows, mn, mx in rows:
        skip = mx <= limit
        skippable += skip
        if g < max_rows:
            print(f"   {g:>9} | {nrows:>8,} | {mn:>10.2f} | {mx:>10.2f} | {'SKIP (cannot match)' if skip else 'must read'}")
    if len(rows) > max_rows:
        print(f"   ... {len(rows) - max_rows} more row groups")
    print(f"   ➜ {skippable} of {len(rows)} row groups could be skipped for price > {limit:.0f}")
    return skippable

u.sample("'sales_big' as it arrived: the prices are in random order", sales_big.select("order_id", "price"))
u.sample("the SAME rows sorted by price (what we write into the second file)", sales_big.select("order_id", "price").orderBy("price"))
u.predict("Same rows in both files. In which file can Spark skip MORE row groups for 'price > 500'? Why?")
f_unsorted = write_one_file(sales_big, "one_file_unsorted")
f_sorted = write_one_file(sales_big.sortWithinPartitions("price"), "one_file_sorted")
print("\n📤 OUTPUT: the footer of each Parquet file (min and max price per row group)")
print("File written in random order (the data as it arrived):")
s1 = show_footer(footer_table(f_unsorted))
print("\nSame rows, SORTED by price before writing:")
s2 = show_footer(footer_table(f_sorted))
u.pause(SEE_OBSERVE)
u.observe(
    "Terminal, the two footer tables just printed",
    "Compare the min price / max price columns of the two files. In the random-order file every row group spans nearly the whole price range. "
    "In the sorted file the ranges are narrow and do not overlap. Count the rows saying SKIP.",
    "A row group can be skipped only if its min..max range cannot contain the value. Narrow ranges = many skips.",
)
u.why("The footer stores min and max per row group. A filter can skip a group only if its range cannot match.")
u.why("Random order means every group spans the whole price range, so almost nothing can be skipped. Sorted data skips a lot. This is exactly the idea behind Z-order in demo 4.")
u.observe(
    "Terminal, the 'PushedFilters' line printed below",
    "It lists price > 500. Spark hands the filter DOWN to the Parquet reader.",
    "That hand-over is what allows the reader to check the min/max numbers above and skip whole row groups.",
)
u.show_plan_lines(sales_pq.filter("price > 500").select("price"), "PushedFilters", mode="formatted")
u.pause()

# ------------------------------------------------------------------------------------------
u.header(5, "Partitioning on disk: one folder per month (slide 15)")
part_dir = u.fresh_dir("s02", "sales_by_month")
u.sample(
    "'sales_big' with its year_month column. partitionBy('year_month') will write one folder per month",
    sales_big.select("order_id", "category", "price", "year_month"),
)
u.pause(RUN_QUERY)
with u.Timer("write partitionBy(year_month)"):
    sales_big.write.partitionBy("year_month").parquet(u.uri(part_dir))
u.ok("Look at the folder names. The month is stored in the FOLDER NAME, not inside the files.")
print("\n📤 OUTPUT: the folder on disk")
u.show_tree(part_dir, 8)
u.pause(SEE_OBSERVE)
u.observe(
    "File Explorer / Finder ➜ demo_output/s02/sales_by_month",
    "One folder per month (year_month=2018-03 ...). Open one: the part files inside hold the other columns.",
    "The folder name IS the value. Spark can pick folders without opening any file.",
)
folders = [d for d in os.listdir(part_dir) if d.startswith("year_month=")]
print(f"\n   {len(folders)} folders, {u.count_files(part_dir)} Parquet files in total")
u.pause()

by_month = spark.read.parquet(u.uri(part_dir))
flat = spark.read.parquet(u.uri(pq_dir))
target = sorted(folders)[len(folders) // 2].split("=")[1]
folder_files = u.count_files(os.path.join(part_dir, f"year_month={target}"))
u.sample(
    "the same data read back from the by-month folders. The year_month column comes from the FOLDER NAMES",
    by_month.select("order_id", "price", "year_month"),
)
u.predict(f"We filter for month {target}. How many of the {u.count_files(part_dir)} files will Spark open with partitioning? And how much data will each version read?")
print(f"\n   WITH partitioning: Spark opens only the folder year_month={target}: {folder_files} of {u.count_files(part_dir)} files")
print("   The plan shows the pruning:")
u.show_plan_lines(by_month.filter(F.col("year_month") == target).select("price"), "PartitionFilters", mode="formatted")
def run_month(df):
    t = time.time()
    df.filter(F.col("year_month") == target).agg(F.sum("price")).collect()
    return time.time() - t
run_month(flat), run_month(by_month)  # warm-up
t_flat, t_part = run_month(flat), run_month(by_month)
mb_flat = u.parquet_mb(pq_dir, ["price", "year_month"])
mb_part = u.parquet_mb(os.path.join(part_dir, f"year_month={target}"), ["price"])
print(f"\n   flat Parquet     : {t_flat:.2f}s   must read the price + year_month columns of ALL files: {mb_flat:.1f} MB")
print(f"   by-month folders : {t_part:.2f}s   must read the price column of ONE folder: {mb_part:.2f} MB")
u.result(
    f"SUM(price) for month {target}: the same answer from both layouts",
    flat.filter(F.col("year_month") == target).agg(F.round(F.sum("price"), 2).cast("decimal(18,2)").alias("sum_from_flat_files")).crossJoin(
        by_month.filter(F.col("year_month") == target).agg(F.round(F.sum("price"), 2).cast("decimal(18,2)").alias("sum_from_month_folder"))
    ),
)
u.pause(SEE_OBSERVE)
u.observe(
    "Spark UI ➜ SQL / DataFrame tab ➜ open the newest by-month query ➜ click the scan node",
    "'number of files read' and 'number of partitions read' are tiny compared with the flat Parquet query. "
    "The 'PartitionFilters' line printed above shows the filter on year_month.",
    "Partition pruning: the other months' folders are never opened.",
)
u.pause()

# ------------------------------------------------------------------------------------------
u.header(6, "The over-partitioning trap (slide 15)")
sample = sales_big.filter(F.pmod(F.hash("customer_unique_id"), F.lit(50)) == 0).repartition(1)  # about 2% of customers, written by 1 task
u.sample(
    "a 2% sample of the customers' order items. We write it twice: partitioned by state, and by customer_unique_id",
    sample.select("order_id", "customer_unique_id", "state", "price"),
)
u.predict("We partition a 2% sample by customer_unique_id instead of by state. How many folders will that create?")
d_state, d_cust = u.fresh_dir("s02", "trap_state"), u.fresh_dir("s02", "trap_customer")
with u.Timer("partitionBy(state)               "):
    sample.write.partitionBy("state").parquet(u.uri(d_state))
with u.Timer("partitionBy(customer_unique_id)  "):
    sample.write.partitionBy("customer_unique_id").parquet(u.uri(d_cust))
print("\n📤 OUTPUT: the two folders on disk (the first entries of each)")
print("   partitionBy(state):")
u.show_tree(d_state, 4)
print("   partitionBy(customer_unique_id):")
u.show_tree(d_cust, 4)
print(f"\n   by state            : {len([x for x in os.listdir(d_state) if '=' in x]):>6,} folders, {u.count_files(d_state):>6,} files, {u.folder_size_mb(d_state):6.1f} MB")
print(f"   by customer_unique_id: {len([x for x in os.listdir(d_cust) if '=' in x]):>6,} folders, {u.count_files(d_cust):>6,} files, {u.folder_size_mb(d_cust):6.1f} MB")
u.pause(SEE_OBSERVE)
u.observe(
    "Terminal, the two lines above (and Finder ➜ demo_output/s02/trap_customer)",
    "Compare folders, files and MB. Same data, but one version has thousands of folders with one tiny file each. In Finder, try opening that folder.",
    "Every file and folder has a fixed cost (listing, opening, metadata). Too many distinct keys = the small-files problem.",
)
u.warn("Same data, but thousands of tiny files. On the full table (about 96,000 customers) it would be ~50x worse.")
u.why("Good partition key: few distinct values that you filter on often (month, country). Bad key: millions of values.")
u.pause()

# ------------------------------------------------------------------------------------------
u.header(7, "Memory partitions: repartition vs coalesce (slide 16)")
uneven = spark.range(0, 1_200_000, numPartitions=6).filter(F.col("id") < 700_000)  # later partitions end up smaller or empty
def sizes(df):
    got = {r["p"]: r["count"] for r in df.groupBy(F.spark_partition_id().alias("p")).count().collect()}
    return [got.get(i, 0) for i in range(df.rdd.getNumPartitions())]  # empty partitions show as 0
u.sample("'uneven': a column id (0 to 699,999) spread over 6 partitions", uneven)
print(f"   start: {uneven.rdd.getNumPartitions()} partitions, rows per partition: {sizes(uneven)}")
u.predict("We go from 6 to 3 partitions. Which method shuffles? Which one keeps the sizes even?")
rep3, coa3 = uneven.repartition(3), uneven.coalesce(3)
print(f"\n   repartition(3): {sizes(rep3)}   <- even, but a full shuffle")
print(f"   coalesce(3)   : {sizes(coa3)}   <- no shuffle, glued neighbours, may be uneven")
print("\n   Plans:")
print("   repartition:"); u.show_plan_lines(rep3, "Exchange")
print("   coalesce   :"); u.show_plan_lines(coa3, "Coalesce")
u.pause(SEE_OBSERVE)
u.observe(
    "Terminal, the 'rows per partition' lists and the Plans just printed",
    "repartition(3) gives three roughly equal numbers and its plan has an 'Exchange'. coalesce(3) can be lopsided and its plan has only 'Coalesce'.",
    "Exchange = shuffle (costly but balanced). Coalesce just glues neighbouring partitions (cheap but uneven).",
)
u.why("repartition(n) can go up or down and balances sizes (costly). coalesce(n) only goes down and is cheap (no shuffle) but can be lopsided.")
u.pause()

# ------------------------------------------------------------------------------------------
u.header(8, "Putting it together: the tidy-write pattern (slides 15 + 16)")
many = sales_big.repartition(8)  # pretend 8 upstream tasks, like a real busy job
a, b = u.fresh_dir("s02", "write_plain"), u.fresh_dir("s02", "write_tidy")
u.sample("'many': sales_big in 8 partitions (8 tasks). Every task holds rows of EVERY month", many.select("order_id", "year_month", "price"))
u.predict("Each of the 8 tasks holds rows of EVERY month. How many files will each month folder get?")
many.write.partitionBy("year_month").parquet(u.uri(a))
many.repartition("year_month").write.partitionBy("year_month").parquet(u.uri(b))
def per_folder(root):
    return [u.count_files(os.path.join(root, d)) for d in os.listdir(root) if d.startswith("year_month=")]
fa, fb = per_folder(a), per_folder(b)
one_month = sorted(d for d in os.listdir(a) if d.startswith("year_month="))[0]
print(f"\n📤 OUTPUT: the files inside ONE month folder ({one_month}) in each version")
print("   plain partitionBy:")
u.show_tree(os.path.join(a, one_month), 12)
print("   repartition('year_month') first:")
u.show_tree(os.path.join(b, one_month), 12)
print(f"\n   plain partitionBy                  : {sum(fa)} files in {len(fa)} folders (about {sum(fa) / len(fa):.1f} per folder)")
print(f"   repartition('year_month') first    : {sum(fb)} files in {len(fb)} folders (about {sum(fb) / len(fb):.1f} per folder)")
u.pause(SEE_OBSERVE)
u.observe(
    "Terminal, the two lines just printed (and Finder ➜ demo_output/s02/write_plain vs write_tidy)",
    "Open one month folder in each and count the files. Plain partitionBy gives up to one file per task. The tidy version gives one.",
    "Many tasks each holding a bit of every month = many small files per folder. Repartition by the same column first to fix that.",
)
u.why("repartition by the SAME column sends all rows of a month to one task, so each folder gets one tidy file.")
u.warn("coalesce(1) also gives one file, but ONE task writes the entire table. Fine for 10 MB, a bottleneck for 10 GB.")
u.pause()

# ------------------------------------------------------------------------------------------
print("\n" + "=" * 72)
print("CLEAN REFERENCE (copy this; no pauses, no prints)")
print("=" * 72)
print('''
# explicit schema, no inference pass
df = spark.read.csv(path, header=True, schema="id STRING, price DOUBLE")

# CSV to Parquet, partitioned by month, one tidy file per folder
(df.withColumn("year_month", F.date_format("purchase_ts", "yyyy-MM"))
   .repartition("year_month")
   .write.mode("overwrite").partitionBy("year_month").parquet("orders/"))

# a filter on the partition column opens one folder only
spark.read.parquet("orders/").filter("year_month = '2018-03'")

# in-memory partitions
df.repartition(8)             # shuffle, balanced, up or down
df.repartition("year_month")  # shuffle by a column
df.coalesce(2)                # no shuffle, only down
''')

u.finish(spark, "NEXT: python 03_delta_acid.py\n   You will learn: how Delta adds a transaction log to Parquet: safe writes, MERGE, time travel.")
