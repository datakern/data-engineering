"""
DataKern | Session 7 | Demo 3 of 5 | DELTA = PARQUET FILES + A TRANSACTION LOG, AND WHY THAT GIVES YOU ACID
Slides 14, 15, 16 (Act 2: Storage)

Bronze is plain Parquet: a folder of files with no memory. Today we turn it into a Delta table,
open the transaction log with our own eyes, and then CRASH a write on purpose, on plain Parquet and on Delta.

  Step 1  Write the SILVER Delta table (the real one, used by demos 4 and 5)
  Step 2  Open the transaction log: _delta_log/00000.json
  Step 3  Crash a write on plain Parquet
  Step 4  Crash the same write on Delta
  Step 5  What a reader sees while the table changes, and ACID

Run:   python 03_delta_log_and_acid.py          (about 9 minutes)
Next:  04_merge_time_travel_and_schema.py
"""
import json
import os

from pyspark.sql import functions as F

import demo_utils as u

spark = u.get_spark("S7 demo 3: delta log and ACID")
u.pause("Open the Spark UI address above, then press ENTER")
u.write_bronze(spark)  # only builds bronze if demo 2 was skipped


def log_files(table_dir):
    """The commit files in _delta_log (hides Spark's .crc checksum files)."""
    d = os.path.join(table_dir, "_delta_log")
    return sorted(f for f in os.listdir(d) if f.endswith(".json"))


# ============================================================== STEP 1
u.header(1, "Write the SILVER Delta table")
print(f"Our story: today is {u.CUTOFF}. The silver table holds every order purchased BEFORE today (bronze has the rest, they arrive in demo 4).")
print("The only change from a Parquet write: format('delta') instead of parquet().")
u.predict("What will the folder contain? The same Parquet files as before, plus what?")

bronze = spark.read.parquet(u.uri(u.BRONZE))
silver_rows = bronze.where(F.col("purchase_ts") < u.CUTOFF)
u.fresh_dir("lakehouse", "silver_orders")
(
    silver_rows.repartition("year_month")
    .write.format("delta").partitionBy("year_month").save(u.uri(u.SILVER))
)
silver_df = spark.read.format("delta").load(u.uri(u.SILVER))
u.ok(f"SILVER written: {silver_df.count():,} orders (bronze has {bronze.count():,}), {u.count_files(u.SILVER)} Parquet files, {u.folder_size_mb(u.SILVER):.2f} MB")
u.show_tree(u.SILVER, max_items=5)
u.say("Look in demo_output/lakehouse/silver_orders/: the month folders with .parquet files (same as before) AND a new folder called _delta_log. Delta table = Parquet data files + the _delta_log folder. The log is the only new thing.")
u.pause()

# ============================================================== STEP 2
u.header(2, "Open the transaction log")
print("The log is just JSON. Every line is one ACTION: 'the table has these properties' or 'this file was added'.")
u.predict("What do you expect Delta to write down about each data file it added?")

first_commit = os.path.join(u.SILVER, "_delta_log", log_files(u.SILVER)[0])
print(f"\n   📄 {os.path.basename(first_commit)}")
actions = [json.loads(ln) for ln in open(first_commit)]
shown_add = 0
for a in actions:
    kind = next(iter(a))
    body = a[kind]
    if kind == "commitInfo":
        print(f"   commitInfo : operation={body['operation']}, rows written={body['operationMetrics']['numOutputRows']}, "
              f"files={body['operationMetrics']['numFiles']}, engine={body['engineInfo']}")
    elif kind == "metaData":
        cols = [f["name"] for f in json.loads(body["schemaString"])["fields"]]
        print(f"   metaData   : columns={cols}")
        print(f"                partitioned by {body['partitionColumns']}")
    elif kind == "add" and shown_add < 2:
        st = json.loads(body["stats"])
        print(f"   add        : {body['path'][:46]}...")
        print(f"                {st['numRecords']:,} rows | order_value min {st['minValues']['order_value']} max {st['maxValues']['order_value']}")
        shown_add += 1
n_add = sum(1 for a in actions if "add" in a)
print(f"   ... and {n_add - shown_add} more 'add' lines (one per data file)")
u.observe(
    f"open demo_output/lakehouse/silver_orders/_delta_log/{os.path.basename(first_commit)} in your editor",
    "one 'add' line per Parquet file, each with its row count and the min / max of every column",
    "the log says what the table IS. A file that is not in the log is not part of the table. Every later commit is ONE new JSON file here; nothing is ever edited. Min / max per file: remember it for demo 5.",
)
u.pause()

# ============================================================== STEP 3
u.header(3, "Crash a write on plain Parquet")
print("We simulate a job that dies half way: one order from the state RR raises an error while the files are being written.")
print("This is a real failure inside the real engine. We only chose when it happens.")
u.predict("A plain Parquet table holds our silver rows. An overwrite job crashes. What does a reader see afterwards?")

pq_dir = u.fresh_dir("s03", "scratch_parquet")
silver_df.write.parquet(u.uri(pq_dir))
print(f"   Before the crash: {spark.read.parquet(u.uri(pq_dir)).count():,} rows in {u.count_files(pq_dir)} files")

crashing = silver_df.repartition("year_month").withColumn(
    "order_value", F.when(F.col("state") == "RR", F.raise_error("💥 injected crash")).otherwise(F.col("order_value"))
)
try:
    crashing.write.mode("overwrite").parquet(u.uri(pq_dir))
except Exception as e:
    u.warn(f"the job FAILED: {type(e).__name__}: {str(e).splitlines()[0][:90]}")

print(f"   After the crash : {u.count_files(pq_dir)} data files in the folder")
try:
    spark.read.parquet(u.uri(pq_dir)).count()
except Exception as e:
    u.warn(f"reading the table now fails: {str(e).splitlines()[0][:110]}")
u.observe(
    "your folder demo_output/s03/scratch_parquet/",
    "the folder exists but the data files are GONE",
    "overwrite = delete the old files, then write the new ones. The crash happened in between. The old table is destroyed and the new one never arrived.",
)
u.pause()

# ============================================================== STEP 4
u.header(4, "Crash the same write on Delta")
print("Same data, same crash. Only the format is different.")
u.predict("The same overwrite crashes on a Delta table. What does a reader see afterwards?")

dl_dir = u.fresh_dir("s03", "scratch_delta_crash")
silver_df.write.format("delta").partitionBy("year_month").save(u.uri(dl_dir))
before = spark.read.format("delta").load(u.uri(dl_dir)).count()
print(f"   Before the crash: {before:,} rows, log has {len(log_files(dl_dir))} commit(s)")
try:
    crashing.write.format("delta").mode("overwrite").save(u.uri(dl_dir))
except Exception as e:
    u.warn(f"the job FAILED: {type(e).__name__}: {str(e).splitlines()[0][:90]}")

after = spark.read.format("delta").load(u.uri(dl_dir)).count()
u.ok(f"After the crash : {after:,} rows (before: {before:,}), log still has {len(log_files(dl_dir))} commit(s)")
u.observe(
    "the two lines above, and demo_output/s03/scratch_delta_crash/_delta_log/",
    "the table is complete and unchanged; no new JSON file appeared in the log",
    "the commit is the LAST thing a write does. If the job dies before it, the log never mentions the new files, so no reader ever sees them.",
)
u.why("Atomicity: a write lands completely or not at all. If a failed job leaves half-written files behind, they are just orphans that VACUUM removes later (demo 5).")
u.pause()

# ============================================================== STEP 5
u.header(5, "What a reader sees while the table changes, and ACID")
print("Readers use the log too. But there is a detail that surprises people.")
u.predict("df = read the Delta table (92k rows). Then another job appends 10 rows. We call df.count() again. Old or new count?")

live = spark.read.format("delta").load(u.uri(dl_dir))
frozen = spark.read.format("delta").option("versionAsOf", 0).load(u.uri(dl_dir))
print(f"   before the append: live df = {live.count():,} rows | frozen (versionAsOf 0) = {frozen.count():,} rows")
silver_df.limit(10).write.format("delta").mode("append").save(u.uri(dl_dir))
print(f"   after the append : live df = {live.count():,} rows | frozen (versionAsOf 0) = {frozen.count():,} rows")
u.observe(
    "the two lines above",
    "the DataFrame we created BEFORE the append sees the 10 new rows on its next action. The versionAsOf one does not",
    "a Delta DataFrame is not a frozen copy: each action reads the latest log. To pin a result, read an explicit version. The count jumps from old to new in one step, never in between.",
)
u.pause()

print("\nACID, in four sentences, each one something you just saw:")
print("  A  Atomicity    step 4: the crashed write left no trace. All or nothing.")
print("  C  Consistency  demo 4: a file with the wrong columns is rejected on write.")
print("  I  Isolation    step 5: the count went from 92,909 to 92,919 in one step. It was never 92,914.")
print("  D  Durability   step 2: once the JSON line is in the log, the commit is permanent.")
u.why("Honest limit: ACID is per TABLE. A job that updates two Delta tables can crash between the two commits.")

u.reference(
    '''
# --- the clean reference: demo 3 without pauses --------------------------------
silver_rows = spark.read.parquet("lakehouse/bronze_orders").where(F.col("purchase_ts") < "2018-08-01")

# Parquet → Delta: change the format, nothing else
silver_rows.repartition("year_month").write.format("delta").partitionBy("year_month").save("lakehouse/silver_orders")

# the log: _delta_log/00000000000000000000.json  (commitInfo, metaData, one 'add' per file with min/max stats)
DeltaTable.forPath(spark, path).history().show()          # one row per commit

# a crashed overwrite: plain Parquet loses the table, Delta keeps the last committed version
df.write.mode("overwrite").parquet(path)                  # crash → folder empty, unreadable
df.write.format("delta").mode("overwrite").save(path)     # crash → table unchanged

# pin a result to a version
spark.read.format("delta").option("versionAsOf", 0).load(path)
'''
)
print("\n➡️  NEXT: python 04_merge_time_travel_and_schema.py")
print("   The silver table is at version 0 and holds orders up to July 2018. In demo 4 'today's file' arrives:")
print("   new orders AND corrections. You will MERGE them in one atomic commit, then break the table and undo it.")
u.finish(spark)
