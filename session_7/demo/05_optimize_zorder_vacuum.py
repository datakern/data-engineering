"""
DataKern | Session 7 | Demo 5 of 5 | SMALL FILES, OPTIMIZE, Z-ORDER AND VACUUM
Slides 22, 23, 24, 25 (Act 3: Speed)

The earlier demos were about how the data is WRITTEN. This demo is about the FILES that pile up afterwards.
A table that is updated all day collects thousands of tiny files. We create that mess, clean it step by step, and see what each command
really changes: OPTIMIZE (how MANY files), Z-ORDER (WHAT is inside them), VACUUM (what stays on disk).

  Step 1  Make a mess: 40 small commits
  Step 2  OPTIMIZE: compaction, and why it is not enough
  Step 3  OPTIMIZE ... ZORDER BY: put similar rows together
  Step 4  VACUUM: what it deletes, and what it costs you

Run:   python 05_optimize_zorder_vacuum.py          (about 12 minutes)
This is the last demo of the series. Next session: Databricks.
"""
import json
import os

from delta.tables import DeltaTable
from pyspark.sql import functions as F

import demo_utils as u

spark = u.get_spark("S7 demo 5: optimize, zorder, vacuum")
u.pause("Open the Spark UI address above, then press ENTER")
u.write_silver(spark)  # only builds silver if demo 3 was skipped

# Real Delta compacts into files of about 1 GB. Our table is 8 MB, so we ask for 1 MB files to get several.
spark.conf.set("spark.databricks.delta.optimize.maxFileSize", str(1024 * 1024))


def health(label, table):
    d = table.detail().select("numFiles", "sizeInBytes").collect()[0]
    avg = d["sizeInBytes"] / max(d["numFiles"], 1) / 1024
    print(f"   {label:<34} {d['numFiles']:>3} files in the table | {d['sizeInBytes'] / 1024 / 1024:>5.2f} MB | average file {avg:>6.0f} KB")


def active_files(table_dir):
    """Replay the Delta log ourselves: the files that are IN the table right now, with their min/max stats."""
    live = {}
    log = os.path.join(table_dir, "_delta_log")
    for name in sorted(f for f in os.listdir(log) if f.endswith(".json")):
        for line in open(os.path.join(log, name)):
            action = json.loads(line)
            if "add" in action:
                live[action["add"]["path"]] = json.loads(action["add"]["stats"])
            elif "remove" in action:
                live.pop(action["remove"]["path"], None)
    return list(live.values())


def show_ranges(table_dir, limit=9):
    print(f"      {'file':<6}{'rows':>7}   {'state min..max':<16}{'year_month min..max'}")
    for i, st in enumerate(active_files(table_dir)[:limit]):
        mn, mx = st["minValues"], st["maxValues"]
        print(f"      {i:<6}{st['numRecords']:>7,}   {mn['state'] + '..' + mx['state']:<16}{mn['year_month']}..{mx['year_month']}")


# ============================================================== STEP 1
u.header(1, "Make a mess: 40 small commits")
silver = DeltaTable.forPath(spark, u.uri(u.SILVER))
health("silver_orders (the real table)", silver)
u.say("A healthy table: a handful of files per month folder, each a decent size. Now we build a sick one.")
print("Streaming writes, frequent MERGEs and over-partitioning all do this: every commit leaves its own small files.")
print("We copy the real silver rows into a NEW table in 40 small batches, as a streaming job would.")
u.predict("40 commits, one small batch each. How many files will the table have, and how many JSON files in _delta_log?")

mess_dir = u.fresh_dir("s06", "orders_microbatches")
mess_path = u.uri(mess_dir)
batches = spark.read.format("delta").load(u.uri(u.SILVER)).withColumn("batch", F.pmod(F.hash("order_id"), F.lit(40)))
for i in range(40):
    batches.where(F.col("batch") == i).drop("batch").coalesce(1).write.format("delta").mode("append").save(mess_path)
mess = DeltaTable.forPath(spark, mess_path)
health("orders_microbatches", mess)
print(f"   _delta_log holds {len([f for f in os.listdir(mess_dir / '_delta_log') if f.endswith('.json')])} commit files")

query = lambda: spark.read.format("delta").load(mess_path).where("state = 'RS' AND year_month = '2018-03'")
print(f"\n   Our test query: orders from RS in March 2018 → {query().count()} rows")
u.scan_report(spark, query(), "before OPTIMIZE")
u.observe(
    "Spark UI ➜ SQL tab ➜ the last query with a 'Scan parquet' box",
    "'number of files read' is every file in the table",
    "to find 418 rows Spark opened 40 files. Each open costs a request to storage and a footer read. Tiny files make everything slower.",
)
u.pause()

# ============================================================== STEP 2
u.header(2, "OPTIMIZE: compaction, and why it is not enough")
print("OPTIMIZE reads the small files and rewrites them as a few big ones. It fixes how MANY files, not what is inside.")
u.predict("After OPTIMIZE: how many files in the TABLE, and how many Parquet files in the FOLDER on disk?")

m = mess.optimize().executeCompaction().collect()[0]["metrics"]
u.ok(f"OPTIMIZE: {m['numFilesRemoved']} small files removed from the table, {m['numFilesAdded']} bigger files added")
health("orders_microbatches", mess)
print(f"   Parquet files on disk in the folder: {u.count_files(mess_dir)}  ({u.folder_size_mb(mess_dir):.1f} MB, the table itself is only {mess.detail().select('sizeInBytes').collect()[0][0] / 1024 / 1024:.1f} MB)")
u.scan_report(spark, query(), "after OPTIMIZE ")
u.observe(
    f"your folder demo_output/s06/orders_microbatches/ and the numbers above",
    "the table has 9 files but the FOLDER still holds 49: the 40 old files are still there",
    "OPTIMIZE never deletes. The old files stay so that time travel and running readers keep working. The folder got BIGGER, not smaller. VACUUM (step 4) is what shrinks it.",
)

print("\n   Our query still opened every file. Why? Look at what each file contains (the min / max stats in the Delta log):")
show_ranges(mess_dir)
u.say("Every file spans state AC..TO and months 2016..2018 (these are the log's 'add' stats, the same numbers Delta uses to skip files). Min / max is useless when every file contains every value, so the query cannot skip a single file. Sorted data skips files, shuffled data does not.")
u.pause()

# ============================================================== STEP 3
u.header(3, "OPTIMIZE ... ZORDER BY: put similar rows together")
print("Z-ORDER rewrites the files so that rows with similar (state, year_month) sit in the same file.")
print("Rule from the slide: 1 to 3 columns, the ones you FILTER on, and not the partition column.")
u.predict("After Z-ORDER BY (state, year_month): how many files will the same query open?")

z = mess.optimize().executeZOrderBy("state", "year_month").collect()[0]["metrics"]
u.ok(f"Z-ORDER: {z['numFilesRemoved']} files removed, {z['numFilesAdded']} added")
show_ranges(mess_dir)
print(f"\n   The same query again: {query().count()} rows (the answer did not change)")
u.scan_report(spark, query(), "after Z-ORDER   ")
u.observe(
    "the ranges table above, and Spark UI ➜ SQL tab ➜ the last 'Scan parquet' box",
    "files now cover narrow state ranges, and 'number of files read' dropped",
    "Delta compared the query to each file's min / max and skipped the files that cannot match. OPTIMIZE fixes the number of files, ZORDER fixes what is in them.",
)
u.pause()

# ============================================================== STEP 4
u.header(4, "VACUUM: what it deletes, and what it costs you")
print(f"The folder holds {u.count_files(mess_dir)} Parquet files, but the table needs only {mess.detail().select('numFiles').collect()[0][0]}. VACUUM deletes the rest.")
u.predict("We run a plain VACUUM right now. How many files does it delete?")

path_sql = f"delta.`{mess_path}`"
default_dry = spark.sql(f"VACUUM {path_sql} DRY RUN").collect()
u.ok(f"VACUUM DRY RUN with the default retention (7 days): would delete {len(default_dry)} file(s)")
print("   Nothing: every old file is only minutes old, and the default keeps 7 days of history.")

print("\n   A colleague says: 'just VACUUM with 0 hours'. We try:")
try:
    mess.vacuum(0)
    u.warn("VACUUM(0) ran. Unexpected.")
except Exception as e:
    u.ok(f"REFUSED: {type(e).__name__}: {str(e).splitlines()[0][:100]}")
u.say("Never retention 0 hours: a query that started a minute ago may still be reading those files. Always DRY RUN first.")

u.pause("Press ENTER to override the safety check on this SCRATCH table only")
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
would = spark.sql(f"VACUUM {path_sql} RETAIN 0 HOURS DRY RUN").collect()
print(f"   DRY RUN with RETAIN 0 HOURS: would delete {len(would)} files")
size_before = u.folder_size_mb(mess_dir)
mess.vacuum(0)
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "true")
u.ok(f"VACUUM done. Folder: {size_before:.1f} MB → {u.folder_size_mb(mess_dir):.1f} MB, {u.count_files(mess_dir)} files left (the table needs exactly these)")

print("\n   The price: time travel to version 0 (the very first small batch).")
v0 = spark.read.format("delta").option("versionAsOf", 0).load(mess_path)
print(f"      v0.count()                 → {v0.count():,}   (answered from the log's statistics, no file is opened!)")
try:
    v0.agg(F.sum("order_value")).collect()
    u.warn("reading the data worked (unexpected)")
except Exception as e:
    text = str(e)
    reason = next((ln.strip() for ln in text.splitlines() if "FileNotFound" in ln or "does not exist" in ln or "No such file" in ln), type(e).__name__)
    u.warn(f"v0.agg(sum(order_value)) → FAILS: {reason.lstrip(chr(58)+chr(32))[:110]}")
u.observe(
    "the lines above",
    "count() 'works' but reading actual data fails: the files version 0 needs are gone",
    "VACUUM is the only command that deletes data files, and it permanently ends time travel to old versions. The default 7-day retention is your safety margin. Keep it.",
)
u.pause()

# ============================================================== STEP 7
print("\n" + "=" * 70)
print("RECAP: the optimisation ladder")
print("=" * 70)
print(f"   {'symptom':<40}{'reach for'}")
for symptom, fix in [
    ("A query reads too much data", "Parquet / Delta + partition pruning (demo 1)"),
    ("Thousands of tiny files", "repartition before the write (demo 2), OPTIMIZE (here)"),
    ("A filter still opens many files", "OPTIMIZE ... ZORDER BY the filter columns"),
    ("Storage keeps growing", "VACUUM, mind the retention"),
    ("Half-written or clashing writes", "Delta log + MERGE (demos 3 and 4)"),
]:
    print(f"   {symptom:<40}{fix}")
u.say("Schedule OPTIMIZE then VACUUM as a regular maintenance job, not inside every write.")

u.reference(
    '''
# --- the clean reference: demo 5 without pauses --------------------------------
dt = DeltaTable.forPath(spark, path)
dt.detail().select("numFiles", "sizeInBytes").show()            # table health

dt.optimize().executeCompaction()                               # many small files → few big ones
dt.optimize().executeZOrderBy("state", "year_month")            # similar rows together → files can be skipped
# SQL: OPTIMIZE tbl ZORDER BY (state, year_month)

spark.sql("VACUUM delta.`path` DRY RUN")                        # ALWAYS look first (default retention: 7 days)
dt.vacuum()                                                     # deletes files older than the retention
# never: dt.vacuum(0) in production. It breaks time travel and running queries.
'''
)
print("\n🎉 Series complete. You built a mini lakehouse: Parquet bronze → Delta silver (MERGE, time travel) → fast, tidy tables.")
print("➡️  NEXT SESSION: Databricks. The same Spark and Delta code, on managed compute with notebooks, jobs and Unity Catalog.")
u.finish(spark)
