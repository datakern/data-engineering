"""
DataKern | Spark session demos | 03  DELTA LAKE and ACID     (slides 17 to 19)

Story: the Olist orders table gets a nightly update. Plain Parquet cannot change a row and
loses data when a job crashes mid-overwrite. Delta can, and does not.

How every step runs:
  📥 INPUT DATA   the table the query works on  ➜ explain the query on this data
  🤔 PREDICT      a question to answer before the query runs (press ENTER to run)
  📤 OUTPUT       what the query produced       ➜ ENTER: then the OBSERVE tells you what to look at

Needs: internet the FIRST time you run it (Spark downloads the Delta jars, 1-2 minutes).
Keep open:  Spark UI,  and the folder  demo_output/s03/  (watch the _delta_log folder).
Run:   python 03_delta_acid.py          (about 8-10 minutes)
"""
import json
import os

import demo_utils as u
from pyspark.sql import functions as F

RUN_QUERY = "Press ENTER to run the query"
SEE_OBSERVE = "Press ENTER to see what to observe"

spark = u.get_spark("03 delta and ACID", delta=True)
from delta.tables import DeltaTable

orders = u.load_olist(spark)["orders"].select("order_id", "customer_id", "order_status", "order_purchase_timestamp")
n_orders = orders.count()
pq_path, dl_path = u.fresh_dir("s03", "orders_parquet"), u.fresh_dir("s03", "orders_delta")

# ------------------------------------------------------------------------------------------
u.header(1, "The same orders table, saved as Parquet and as Delta (slide 17)")
u.sample(f"'orders': {n_orders:,} rows, one per order. We save this table twice: as plain Parquet and as Delta", orders)
u.pause(RUN_QUERY)
orders.write.mode("overwrite").parquet(u.uri(pq_path))
orders.write.format("delta").mode("overwrite").save(u.uri(dl_path))
u.ok(f"Both tables hold {n_orders:,} orders.")
print("\n📤 OUTPUT: the two folders on disk")
print("\n📁 Parquet folder:")
u.show_tree(pq_path, 6)
print("\n📁 Delta folder:")
u.show_tree(dl_path, 6)
u.pause(SEE_OBSERVE)
u.observe(
    "File Explorer / Finder ➜ demo_output/s03/ ➜ compare orders_parquet and orders_delta",
    "Both folders hold .snappy.parquet data files. Only the Delta folder has one extra folder, _delta_log. Open it: a small .json file per commit.",
    "Delta = plain Parquet files + a transaction log. Nothing magic about the data files themselves.",
)
u.pause()

# ------------------------------------------------------------------------------------------
u.header(2, "Open the transaction log: the table IS whatever the log says (slide 17)")
log_dir = dl_path / "_delta_log"
first = sorted(f for f in os.listdir(log_dir) if f.endswith(".json"))[0]
print("📥 INPUT DATA: the folder orders_delta/_delta_log. One small .json file per commit")
u.show_tree(log_dir, 5)
u.pause("Press ENTER to open the first commit file")
print(f"📤 OUTPUT: what is inside {first}   (commit number 0)\n")
kinds = {}
with open(log_dir / first) as fh:
    for ln in fh:
        entry = json.loads(ln)
        k = next(iter(entry))
        kinds[k] = kinds.get(k, 0) + 1
        if k == "commitInfo":
            print("   commitInfo ➜ operation:", entry[k].get("operation"), "| mode:", entry[k].get("operationParameters", {}).get("mode"))
        if k == "add" and kinds[k] == 1:
            st = json.loads(entry[k]["stats"]) if entry[k].get("stats") else {}
            print(f"   add ➜ file {entry[k]['path'][:48]}...   numRecords = {st.get('numRecords')}")
print("\n   entries in this commit:", kinds)
u.pause(SEE_OBSERVE)
u.observe(
    "Terminal, the lines just printed (then open the .json file in demo_output/s03/orders_delta/_delta_log with a text editor)",
    "Entries of type 'add' (one per data file), 'metaData' (the schema) and 'commitInfo'. Inside an 'add' line find 'stats': min and max values per column.",
    "The table is not 'whatever files sit in the folder'. It is 'the files the log says were added'.",
)
u.why("Each commit is one small JSON file: 'add this file, remove that file'. A reader replays the log to know which files make the table.")
u.why("Notice 'stats': Delta also stores min/max per column per file in the log. That is what data skipping uses (demo 4).")
u.pause()

# ------------------------------------------------------------------------------------------
u.header(3, "A job crashes in the middle of an overwrite: Parquet vs Delta (slide 17)")
def explode_on_cancelled(s):
    if s == "canceled":
        raise ValueError("💥 simulated crash while processing a row")
    return s
boom = F.udf(explode_on_cancelled, "string")
bad_batch = orders.withColumn("order_status", boom("order_status"))

u.sample("'orders': the table we are about to overwrite (both copies hold these rows today)", orders)
u.sample(
    "how many orders have each order_status. Our batch crashes as soon as it meets 'canceled'",
    orders.groupBy("order_status").count().orderBy(F.desc("count")),
    n=10,
)
u.warn("We now OVERWRITE both tables with a batch that will crash halfway.")
u.predict("After the crash: how many rows does the Parquet table have? And the Delta table?")
print("\n📤 OUTPUT: what is left after the crash")
print("--- Parquet overwrite ---")
try:
    bad_batch.write.mode("overwrite").parquet(u.uri(pq_path))
except Exception as e:
    print("   job failed ➜", str(e).splitlines()[0][:110])
try:
    left = spark.read.parquet(u.uri(pq_path)).count()
    print(f"   Parquet table now has {left:,} rows")
except Exception as e:
    left = 0
    print("   Parquet table cannot even be read now ➜", str(e).splitlines()[0][:110])

print("\n--- Delta overwrite ---")
try:
    bad_batch.write.format("delta").mode("overwrite").save(u.uri(dl_path))
except Exception as e:
    print("   job failed ➜", str(e).splitlines()[0][:110])
delta_after = spark.read.format("delta").load(u.uri(dl_path)).count()
print(f"   Delta table still has {delta_after:,} rows")
print(f"\n   Parquet: {n_orders:,} ➜ {left:,} rows     Delta: {n_orders:,} ➜ {delta_after:,} rows")
dt = DeltaTable.forPath(spark, u.uri(dl_path))
print(f"   Delta history shows {dt.history().count()} version(s): the failed write never became a commit.")
u.pause(SEE_OBSERVE)
u.observe(
    "Terminal, the line 'Parquet: ... ➜ ...     Delta: ... ➜ ...' and the history line below it",
    "Parquet's row count after the crash vs Delta's. Delta's history still shows only the versions that succeeded.",
    "Parquet overwrite deletes first and writes second, so a crash in between loses data. Delta only commits when the write finished.",
)
u.why("Spark's overwrite first DELETES the old Parquet files, then writes new ones. A crash in between = data loss. Delta writes new files first and only a final log entry makes them visible.")
u.warn("Other crash types (killed executors, cloud storage) can leave a MIX of old and new files in a plain Parquet folder. Either way, plain Parquet has no way to say what the table 'really' is.")
files_on_disk, files_in_table = u.count_files(dl_path), dt.detail().collect()[0]["numFiles"]
print(f"\n   Delta folder holds {files_on_disk} Parquet files, but the table only uses {files_in_table}: failed attempts can leave unused files behind. VACUUM (demo 4) cleans those.")
u.pause()

# ------------------------------------------------------------------------------------------
u.header(4, "MERGE: today's file has new orders AND status changes (slide 19)")
current = spark.read.format("delta").load(u.uri(dl_path))
changed_before = current.filter("order_status IN ('shipped', 'invoiced', 'processing')").limit(100).cache()
sample = changed_before.limit(1).collect()[0]  # an order that WILL be updated, so the before/after demo works
sample_id = sample["order_id"]
changed = changed_before.withColumn("order_status", F.lit("delivered"))
new_rows = current.limit(50).withColumn("order_id", F.concat(F.lit("NEW-"), F.col("order_id")))
updates = changed.unionByName(new_rows)
print(f"Today's file: {changed.count()} changed orders + {new_rows.count()} brand-new orders = {updates.count()} rows")
print(f"Order {sample_id} status BEFORE: {sample['order_status']}")
before = current.count()
u.sample("the TARGET: the Delta table 'orders' as it is now", current)
u.sample("today's file, part 1: 100 CHANGED orders (same order_id as in the table, new status 'delivered')", changed, n=3)
u.sample("today's file, part 2: 50 brand-new orders (order_id starts with NEW-, not in the table yet)", new_rows, n=3)
u.predict(f"The table has {before:,} rows. After MERGE, how many? (changed orders update, new orders insert)")
(
    dt.alias("t")
    .merge(updates.alias("u"), "t.order_id = u.order_id")
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)
new_order_id = new_rows.first()["order_id"]
u.result(
    "the table after MERGE: the changed order was updated in place, the new order was inserted",
    spark.read.format("delta").load(u.uri(dl_path)).filter(F.col("order_id").isin(sample_id, new_order_id)).orderBy("order_id"),
    truncate=22,
)
after = spark.read.format("delta").load(u.uri(dl_path)).count()
print(f"\n   rows before: {before:,}   after: {after:,}   (+{after - before} new orders; the {changed.count()} others were updated in place)")
status_now = spark.read.format("delta").load(u.uri(dl_path)).filter(F.col("order_id") == sample_id).first()["order_status"]
print(f"   Order {sample_id} status AFTER: {status_now}")
u.pause(SEE_OBSERVE)
u.observe(
    "Terminal, the 'rows before / after' line and the two 'status' lines just printed",
    "The row count grew by exactly the number of NEW orders, while the changed orders added no rows. The sample order's status changed.",
    "One MERGE updated the matching rows and inserted the rest, in a single atomic commit.",
)
u.why("MERGE = update the rows that match, insert the rest, in ONE atomic commit. Plain Parquet cannot do this at all.")
u.pause()

# ------------------------------------------------------------------------------------------
u.header(5, "History and time travel (slide 19)")
u.sample(
    "the table history: one row per commit (the transaction log, as a table)",
    dt.history().select("version", "operation", F.col("operationMetrics.numOutputRows").alias("rows_written")).orderBy("version"),
)
latest_v = dt.history().agg(F.max("version")).first()[0]
u.predict(f"What status did order {sample_id} have in version 0 (before the MERGE)?")
v0 = spark.read.format("delta").option("versionAsOf", 0).load(u.uri(dl_path))
v_now = spark.read.format("delta").load(u.uri(dl_path))
u.result(
    f"order {sample_id[:8]}... read as of version 0 and as of now",
    v0.filter(F.col("order_id") == sample_id).select(F.lit("version 0").alias("read_as_of"), "order_id", "order_status")
    .unionByName(
        v_now.filter(F.col("order_id") == sample_id).select(F.lit(f"version {latest_v} (now)").alias("read_as_of"), "order_id", "order_status")
    ),
    truncate=22,
)
print(f"   version 0       : {v0.filter(F.col('order_id') == sample_id).first()['order_status']}   ({v0.count():,} rows)")
print(f"   version {latest_v} (now): {v_now.filter(F.col('order_id') == sample_id).first()['order_status']}   ({v_now.count():,} rows)")
u.pause(SEE_OBSERVE)
u.observe(
    "Terminal, the history table and the two 'version' lines just printed",
    "One row per commit (WRITE, MERGE ...) with the version number. Version 0 shows the ORIGINAL status and row count, the newest version shows the changed one.",
    "Nothing was overwritten in place: every version still points at its own files, so you can read the table as of any version.",
)
u.why("Every commit is a version. versionAsOf reads the files of that version. Old files are kept until VACUUM removes them.")
u.pause()

# ------------------------------------------------------------------------------------------
u.header(6, "Schema enforcement: bad data is rejected (slide 19)")
extra = orders.limit(5).withColumn("coupon_code", F.lit("SUMMER10"))
u.sample("the TARGET: the Delta table today (columns: " + ", ".join(spark.read.format("delta").load(u.uri(dl_path)).columns) + ")", spark.read.format("delta").load(u.uri(dl_path)), n=3)
u.sample("the NEW batch: 5 rows with an EXTRA column 'coupon_code' that the table does not have", extra)
u.predict("We append 5 rows that have an extra column the table does not know. Accepted or rejected?")
print("📤 OUTPUT: first a plain append, then an append with mergeSchema")
try:
    extra.write.format("delta").mode("append").save(u.uri(dl_path))
    print("   appended (unexpected!)")
except Exception as e:
    print("   🛑 REJECTED ➜", str(e).strip().splitlines()[0][:160])
print(f"   table still has {spark.read.format('delta').load(u.uri(dl_path)).count():,} rows and the same columns.")
print("\nSchema EVOLUTION is an explicit opt-in:  .option('mergeSchema', 'true')")
extra.write.format("delta").mode("append").option("mergeSchema", "true").save(u.uri(dl_path))
evolved = spark.read.format("delta").load(u.uri(dl_path))
print("   columns now:", evolved.columns)
print(f"   old rows have NULL in the new column: {evolved.filter('coupon_code IS NULL').count():,} NULLs, {evolved.filter('coupon_code IS NOT NULL').count()} filled")
u.result(
    "the table after mergeSchema: the 5 new rows have a coupon_code, the older rows have NULL",
    evolved.select("order_id", "order_status", "coupon_code").orderBy(F.col("coupon_code").desc_nulls_last()),
    n=8,
    truncate=22,
)
u.pause(SEE_OBSERVE)
u.observe(
    "Terminal, the REJECTED message and the 'columns now' line just printed",
    "The first append is refused with a schema-mismatch message and the table keeps its rows and columns. "
    "After mergeSchema=true the new column coupon_code appears and every old row has NULL in it.",
    "Delta says NO to surprise columns by default. Changing the schema is a deliberate opt-in.",
)
u.why("Delta checks every write against the table schema, so one bad upstream file cannot silently corrupt the table.")
u.pause()

# ------------------------------------------------------------------------------------------
u.header(7, "Oops: an accidental DELETE, then RESTORE (slide 19)")
good_v = dt.history().agg(F.max("version")).first()[0]
def status_counts():
    return spark.read.format("delta").load(u.uri(dl_path)).groupBy("order_status").count().orderBy(F.desc("count"))
rows_good = spark.read.format("delta").load(u.uri(dl_path)).count()
u.sample(f"the table today: how many orders have each status ({rows_good:,} rows in total)", status_counts(), n=10)
u.warn("Someone runs a DELETE with a condition that matches almost everything...")
dt.delete("order_status = 'delivered'")
rows_bad = spark.read.format("delta").load(u.uri(dl_path)).count()
u.result("the table right after the accidental DELETE (the 'delivered' orders are gone)", status_counts(), n=10)
print(f"   rows: {rows_good:,} ➜ {rows_bad:,}")
u.predict("Can we get the rows back? How?")
dt.restoreToVersion(good_v)
u.result("the table after RESTORE", status_counts(), n=10)
rows_back = spark.read.format("delta").load(u.uri(dl_path)).count()
print(f"   after RESTORE to version {good_v}: {rows_back:,} rows  {'✅ all back' if rows_back == rows_good else '🛑 mismatch'}")
u.result("the history now", dt.history().select("version", "operation").orderBy("version"), n=10)
u.pause(SEE_OBSERVE)
u.observe(
    "Terminal, the 'rows:' line, the 'after RESTORE' line and the history table",
    "Rows dropped after the DELETE and are all back after RESTORE. The history now lists DELETE and RESTORE as new versions at the bottom.",
    "Nothing was erased. The mistake and its repair are both just commits in the log.",
)
u.why("RESTORE is just another commit that points the table back at old files. It only works while those old files still exist (see VACUUM).")
u.pause()

# ------------------------------------------------------------------------------------------
print("\n" + "=" * 72)
print("CLEAN REFERENCE (copy this; no pauses, no prints)")
print("=" * 72)
print('''
from delta.tables import DeltaTable

df.write.format("delta").mode("overwrite").save(path)          # create / replace
dt = DeltaTable.forPath(spark, path)

(dt.alias("t").merge(updates.alias("u"), "t.order_id = u.order_id")
   .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())   # upsert

dt.history().show()                                              # versions
spark.read.format("delta").option("versionAsOf", 0).load(path)   # time travel
dt.restoreToVersion(3)                                           # undo
df.write.format("delta").mode("append").option("mergeSchema", "true").save(path)  # opt-in schema change
''')

u.finish(spark, "NEXT: python 04_skew_and_table_health.py\n   You will learn: spotting data skew in the Spark UI, and keeping a Delta table healthy (OPTIMIZE, Z-order, VACUUM).")
