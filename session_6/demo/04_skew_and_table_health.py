"""
DataKern | Spark session demos | 04  DATA SKEW and TABLE HEALTH    (slides 20 to 23)

Part A (steps 1-5): one task does most of the work. We SEE it, then fix it three ways.
Part B (steps 6-9): a Delta table with too many small files: OPTIMIZE, Z-ORDER, VACUUM.

How every step runs:
  📥 INPUT DATA   the table the query works on  ➜ explain the query on this data
  🤔 PREDICT      a question to answer before the query runs (press ENTER to run)
  📤 OUTPUT       what the query produced       ➜ ENTER: then the OBSERVE tells you what to look at

Keep open:  Spark UI ➔ Stages tab ➔ click a stage ➔ look at the task table (Duration column)
Needs:  internet the first time (Delta jars). Run:   python 04_skew_and_table_health.py   (about 10-15 minutes)
"""
import os
import re
import time

import demo_utils as u
from pyspark.sql import functions as F

HAVE_DELTA = os.environ.get("DEMO_NO_DELTA") != "1"  # DEMO_NO_DELTA=1 skips Part B (used for quick tests)
RUN_QUERY = "Press ENTER to run the query"
SEE_OBSERVE = "Press ENTER to see what to observe"
HOT = 4  # how much we inflate Sao Paulo (SP)
BASE_AMP = 5
# For the skew demo we turn OFF the helpers that would hide skew, then turn them on one by one.
spark = u.get_spark(
    "04 skew and table health",
    delta=HAVE_DELTA,
    shuffle_partitions=8,
    extra_conf={
        "spark.sql.adaptive.enabled": "false",
        "spark.sql.autoBroadcastJoinThreshold": "-1",
    },
)

# ------------------------------------------------------------------------------------------
u.header(1, "Where is the skew? Olist rows per state")
sales = u.build_sales(spark)
total = sales.count()
u.sample("'sales': one row per order item. The column 'state' is where the customer lives", sales.select("order_id", "customer_unique_id", "state", "price"))
print("   We count the rows of each state.")
u.pause(RUN_QUERY)
by_state = sales.groupBy("state").count().orderBy(F.desc("count"))
print("📤 OUTPUT: rows per state (top 5)")
print(f"   {'state':<6}{'rows':>9}{'share':>8}")
for r in by_state.limit(5).collect():
    print(f"   {r['state']:<6}{r['count']:>9,}{100 * r['count'] / total:>7.1f}%")
u.pause(SEE_OBSERVE)
u.observe(
    "Terminal, the 'rows per state' table just printed",
    "Compare the share of the first state (SP) with the next ones.",
    "A few keys carry most of the rows. Real data is always lopsided like this, and that is where skew comes from.",
)
u.why("SP (Sao Paulo) holds the biggest share of customers. Real data is always lopsided like this.")
u.warn(f"On a laptop this natural skew is mild, so we INFLATE SP by {HOT}x to make the problem visible. Say this out loud: it is a teaching trick, not Olist's real data.")
events = (
    sales.withColumn("copy", F.explode(F.sequence(F.lit(1), F.when(F.col("state") == "SP", F.lit(BASE_AMP * HOT)).otherwise(F.lit(BASE_AMP)))))
    .withColumn("order_id", F.concat_ws("-", "order_id", "copy"))
    .drop("copy")
    .cache()
)
n_events = events.count()
sp_rows = events.filter("state = 'SP'").count()
u.ok(f"events: {n_events:,} rows, of which SP = {sp_rows:,} ({100 * sp_rows / n_events:.0f}%)")
u.result("'events' = sales with every row repeated, SP more often: rows per state now", events.groupBy("state").count().orderBy(F.desc("count")), n=5)
u.pause()

# ------------------------------------------------------------------------------------------
u.header(2, "A shuffle sends every row of one key to ONE partition (slides 5 and 20)")
def sizes(df):
    got = {r["p"]: r["count"] for r in df.groupBy(F.spark_partition_id().alias("p")).count().collect()}
    return [got.get(i, 0) for i in range(df.rdd.getNumPartitions())]  # empty partitions show as 0
u.sample("'events': the table we shuffle. The key of the shuffle is the column 'state'", events.select("order_id", "state", "price"))
u.predict("We hash-partition the events by state into 8 partitions. Will the 8 partitions have similar sizes?")
by_key = events.repartition(8, "state")
sz = sizes(by_key)
print("\n📤 OUTPUT: rows per partition")
for i, c in enumerate(sz):
    print(f"   partition {i}: {c:>9,} {'█' * int(40 * c / max(sz))}")
u.pause(SEE_OBSERVE)
u.observe(
    "Terminal, the bars just printed",
    "One bar is far longer than the others, although we asked for 8 'equal' partitions.",
    "A shuffle by key sends every row of the same key to the same partition. The hot key makes one partition huge.",
)
u.why("All SP rows hash to the same partition. One task gets a mountain, the others get a hill. The stage ends when the slowest task ends.")
u.pause()

# ------------------------------------------------------------------------------------------
u.header(3, "The skewed join, as a real query (slide 20)")
regions = spark.createDataFrame(
    [(s, r) for s, r in [("SP", "southeast"), ("RJ", "southeast"), ("MG", "southeast"), ("ES", "southeast"),
                         ("RS", "south"), ("PR", "south"), ("SC", "south"),
                         ("BA", "northeast"), ("PE", "northeast"), ("CE", "northeast"), ("MA", "northeast"), ("PB", "northeast"),
                         ("DF", "center-west"), ("GO", "center-west"), ("MT", "center-west"), ("MS", "center-west"),
                         ("PA", "north")]],
    "state STRING, region STRING",
)
def work(joined):
    """Join, then do some CPU work per row, then a small result (so collect() is cheap)."""
    return joined.withColumn("h", F.sha2(F.concat_ws("|", "order_id", "seller_id", "product_id"), 256)).groupBy("region").agg(
        F.approx_count_distinct("h").alias("distinct_hashes"), F.round(F.sum("price"), 2).cast("decimal(18,2)").alias("revenue")
    )
skewed_query = work(events.join(regions, "state"))
u.sample("'events' (the BIG side of the join)", events.select("order_id", "seller_id", "product_id", "state", "price"))
u.sample("'regions' (the SMALL side): which region each state belongs to", regions)
print("   The query: join events with regions on state, hash every row (CPU work), then add up revenue per region.")
print("Plan (AQE and broadcast are OFF, so Spark must do a sort-merge join):")
u.show_plan_lines(skewed_query, "SortMergeJoin [state", "Exchange hashpartitioning(state")
u.predict("Before we run it: which task will be the slowest, and roughly how many times slower than the median?")
work(events.limit(60000).join(regions, "state")).collect()  # warm-up so the first timed run is not slowed by JVM start-up
since = u.last_stage_id(spark)
with u.Timer("skewed join") as t_skew:
    skewed_query.collect()
r_skew = u.task_report(spark, since, "skewed:")
u.result("the join result: revenue per region", skewed_query.orderBy("region"), n=5)
u.pause(SEE_OBSERVE)
u.observe(
    "Terminal, the 'skewed: busiest stage N ...' line. Then Spark UI ➜ Stages tab ➜ click that stage number N",
    "The terminal already says how much slower the slowest task is than the median. In the stage page, click the 'Duration' header of the task table: "
    "ONE task is far longer than the rest. In 'Summary Metrics' compare Median with Max. Open 'Event Timeline' to see one long bar.",
    "A stage ends only when its slowest task ends. While that one task works the other cores sit idle. That is skew.",
)
u.pause()

# ------------------------------------------------------------------------------------------
u.header(4, "Fix 1: let Spark split the hot partition itself (AQE skew join)")
u.sample("the SAME 'events' and 'regions' as in step 3. Only the Spark settings change: AQE is switched on", events.select("order_id", "state", "price"), n=3)
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "2MB")  # default is 256MB: real clusters have big data
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "2MB")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "false")
aqe_query = work(events.join(regions, "state"))
u.pause(RUN_QUERY)
since = u.last_stage_id(spark)
with u.Timer("AQE skew join") as t_aqe:
    aqe_query.collect()
r_aqe = u.task_report(spark, since, "AQE:   ")
print("\n   Final plan, look for skew=true:")
u.show_plan_lines(aqe_query, "skew=true", "SortMergeJoin [state")
u.result("the join result: the same revenue per region as in step 3", aqe_query.orderBy("region"), n=5)
u.pause(SEE_OBSERVE)
u.observe(
    "Terminal, compare the 'skewed:' and 'AQE:' lines and the 'skew=true' plan line. Then Spark UI ➜ Stages tab ➜ open the stage number in the 'AQE:' line",
    "AQE's slowest task is close to its median (about 1.0x) and the stage has MORE than 8 tasks. The plan shows SortMergeJoin(skew=true).",
    "AQE noticed the oversized partition while running and cut it into several smaller tasks.",
)
u.why("AQE (Adaptive Query Execution) sees the real partition sizes at run time and splits an oversized partition into several tasks. On by default in modern Spark.")
u.warn("We lowered the size thresholds so it triggers on laptop data. On a cluster the defaults (256 MB) apply. AQE helps joins; it will NOT fix a hot key in a groupBy.")
u.pause()

# ------------------------------------------------------------------------------------------
u.header(5, "Fix 2: broadcast the small side.   Fix 3: salting (slide 20)")
spark.conf.set("spark.sql.adaptive.enabled", "false")
bcast_query = work(events.join(F.broadcast(regions), "state"))
u.sample(f"'regions': only {regions.count()} rows, tiny. We tell Spark to send a full copy of it to every task (broadcast)", regions)
u.pause(RUN_QUERY)
since = u.last_stage_id(spark)
with u.Timer("broadcast join") as t_b:
    bcast_query.collect()
r_b = u.task_report(spark, since, "broadcast:")
print("\n   Plan, the join now looks like this:")
u.show_plan_lines(bcast_query, "BroadcastHashJoin", "BroadcastExchange")
u.result("the join result: the same revenue per region", bcast_query.orderBy("region"), n=5)
u.pause(SEE_OBSERVE)
u.observe(
    "Spark UI ➜ Stages tab ➜ compare the 'Shuffle Read' column of the stage numbers printed in the 'skewed:' and 'broadcast:' lines",
    "The skewed join's stage read many MB through the shuffle. The broadcast join's stage reads 0. The plan lines printed above say "
    "'BroadcastHashJoin' fed by a 'BroadcastExchange' (in the SQL tab ignore the old joins hidden inside the cached 'events' box).",
    "The small table is copied to every executor, so the big table never needs to be shuffled.",
)
u.why("The small table is copied to every executor, the big table is NOT shuffled, so there is no hot partition at all. Use it whenever one side is small.")
u.pause()

K = 8
print(f"Salting: spread the hot key over {K} sub-keys. The big side gets a random salt; the small side is copied {K} times.")
salted_events = events.withColumn("salt", F.when(F.col("state") == "SP", F.floor(F.rand(11) * K).cast("int")).otherwise(F.lit(0)))
salted_regions = regions.withColumn("salt", F.explode(F.sequence(F.lit(0), F.lit(K - 1))))
u.sample("'events' of the hot key SP with a random 'salt' (0 to 7) added: SP is now 8 sub-keys", salted_events.filter("state = 'SP'").select("order_id", "state", "salt"))
u.sample("'regions' copied 8 times, one copy per salt (only SP shown), so every sub-key finds its match", salted_regions.filter("state = 'SP'").orderBy("salt"), n=8)
u.predict("If we split key SP into SP-0 ... SP-7, what will the rows-per-partition picture look like?")
sz2 = sizes(salted_events.repartition(8, "state", "salt"))
print("\n📤 OUTPUT: rows per partition AFTER salting")
for i, c in enumerate(sz2):
    print(f"   partition {i}: {c:>9,} {'█' * int(40 * c / max(sz))}")
salt_query = work(salted_events.join(salted_regions, ["state", "salt"]))
since = u.last_stage_id(spark)
with u.Timer("salted join") as t_s:
    salt_query.collect()
r_s = u.task_report(spark, since, "salted:   ")
u.result("the join result: the same revenue per region", salt_query.orderBy("region"), n=5)
u.why("Salting is the manual fix: slower to write, and the small side must be duplicated. Use it when you cannot broadcast and AQE is not enough.")
u.why("The split is not perfectly even: the (state, salt) pairs are hashed into only 8 partitions, so some collide. More salt values or more shuffle partitions would even it out.")
print("\n📊 Summary (this laptop, 2-8 cores; ratios matter more than seconds)")
print(f"   {'method':<22}{'time':>8}{'slowest task':>16}{'vs median':>10}")
for name, tt, rr in (("skewed (AQE off)", t_skew, r_skew), ("AQE skew join", t_aqe, r_aqe), ("broadcast", t_b, r_b), ("salting", t_s, r_s)):
    slow, ratio = rr if rr else (None, None)
    print(f"   {name:<22}{tt.seconds:>7.1f}s{(f'{slow} ms' if slow else 'see UI'):>16}{(f'{ratio:.1f}x' if ratio else ''):>10}")
u.pause(SEE_OBSERVE)
u.observe(
    "Terminal, the bars 'AFTER salting' compared with the bars of step 2",
    "The single huge SP bar is now spread over several smaller bars.",
    "Salting adds a random number to the hot key so its rows land on different partitions.",
)
u.observe(
    "Terminal, the Summary table just printed",
    "Do not only read 'time'. Compare the 'slowest task' and 'vs median' columns of the four methods.",
    "Skew shows up as one task far above the median. Every fix is judged by how much it shrinks that gap.",
)
u.warn("A 4th fix: remove junk keys first. A single NULL or 'unknown' key is often the hot key.")
u.pause()

if not HAVE_DELTA:
    print("\n(Part B skipped: DEMO_NO_DELTA=1)")
    u.finish(spark)
    raise SystemExit

# ============================================================ PART B: table health
from delta.tables import DeltaTable
import json

spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10MB")
u.header(6, "A Delta table that grew many small files (slide 21)")
table_path = u.fresh_dir("s04", "sales_delta")
tbl = (
    sales.withColumn("year_month", F.date_format("purchase_ts", "yyyy-MM"))
    .withColumn("copy", F.explode(F.sequence(F.lit(1), F.lit(3))))
    .withColumn("order_id", F.concat_ws("-", "order_id", "copy"))
    .drop("copy")
    .repartition(60)  # 60 tasks, each holding a random mix of states and months, like many small streaming batches
)
u.sample(
    "'sales' with a year_month column, copied 3 times. We spread it over 60 tasks, each holding a random mix of states and months",
    tbl.select("order_id", "state", "year_month", "price"),
)
u.pause(RUN_QUERY)
tbl.write.format("delta").save(u.uri(table_path))
dt = DeltaTable.forPath(spark, u.uri(table_path))
print("\n📤 OUTPUT: the Delta folder on disk (the first entries): every task wrote its own small file")
u.show_tree(table_path, 6)
def health(label):
    d = dt.detail().collect()[0]
    avg = d["sizeInBytes"] / max(d["numFiles"], 1) / 1024
    print(f"   {label:<28} files: {d['numFiles']:>4}   size: {d['sizeInBytes'] / 1024 / 1024:6.1f} MB   avg file: {avg:8.0f} KB")
    return d["numFiles"]
cond = (F.col("state") == "RJ") & (F.col("year_month") == "2018-03")
def files_with_matches():
    return spark.read.format("delta").load(u.uri(table_path)).filter(cond).select(F.input_file_name()).distinct().count()
def timed_query(label):
    t = time.time()
    spark.read.format("delta").load(u.uri(table_path)).filter(cond).agg(F.sum("price")).collect()
    print(f"⏱️  {label}: {time.time() - t:.2f}s")
u.sample(
    "the rows our query wants: state = 'RJ' AND month = '2018-03' (a tiny slice of the table)",
    spark.read.format("delta").load(u.uri(table_path)).filter(cond).select("order_id", "state", "year_month", "price"),
)
n0 = health("start")
m0 = files_with_matches()
print(f"   query: state = 'RJ' AND month = '2018-03' ➜ rows sit in {m0} of {n0} files")
timed_query("query time")
u.pause(SEE_OBSERVE)
u.observe(
    "File Explorer / Finder ➜ demo_output/s04/sales_delta, and the 'start' line plus the query line just printed",
    "Dozens of tiny .parquet files (see 'avg file' in KB). The query for ONE state and ONE month still needs rows from almost all of them.",
    "Many small files = a lot of opening, listing and task overhead, and no file can be skipped because each holds a random mix of rows.",
)
u.pause()

# ------------------------------------------------------------------------------------------
u.header(7, "OPTIMIZE: compact small files, then Z-ORDER: cluster similar values (slide 21)")
spark.conf.set("spark.databricks.delta.optimize.maxFileSize", str(4 * 1024 * 1024))  # demo only: 4 MB target (default 1 GB) so this 50 MB table compacts into several files
print("📥 INPUT DATA: the table from step 6. Its folder holds 60 small files, each with a random mix of states and months:")
u.show_tree(table_path, 6)
u.predict("OPTIMIZE (compaction) merges many small files into fewer big ones. Will a filter on state+month now touch fewer files?")
print("\n📤 OUTPUT: the table after OPTIMIZE (compaction)")
res = dt.optimize().executeCompaction()
n1 = health("after OPTIMIZE (compaction)")
m1 = files_with_matches()
print(f"   rows for RJ + 2018-03 sit in {m1} of {n1} files")
timed_query("query time")
u.pause(SEE_OBSERVE)
u.observe(
    "Terminal, the 'after OPTIMIZE' line and the line 'rows for RJ + 2018-03 sit in X of Y files'",
    "The number of files dropped (60 to a few). But the query still needs most of the remaining files.",
    "Compaction fixes the file COUNT, not the file CONTENT.",
)
u.why("Compaction fixes the file COUNT but not the file CONTENT: every file still holds a random mix, so the filter still needs most of them.")
u.pause()
def show_file_stats(title):
    """Print min..max of state and month for the first 3 data files of the newest commit that added files."""
    log_dir = os.path.join(table_path, "_delta_log")
    for name in sorted((f for f in os.listdir(log_dir) if f.endswith(".json")), reverse=True):
        entries = [json.loads(ln) for ln in open(os.path.join(log_dir, name))]
        adds = [e["add"] for e in entries if "add" in e and e["add"].get("stats")]
        if adds:
            print(f"\n{title} (from {name}, 3 of {len(adds)} files):")
            for add in adds[:3]:
                st = json.loads(add["stats"])
                print(f"   {add['path'][:28]}...  state {st['minValues'].get('state')}..{st['maxValues'].get('state')}   month {st['minValues'].get('year_month')}..{st['maxValues'].get('year_month')}")
            return


show_file_stats("📥 INPUT DATA: the min..max of state and month that Delta stores per file BEFORE the Z-ORDER")
u.predict("Now we rewrite the data so similar values sit together: Z-ORDER BY (state, year_month). What changes?")
dt.optimize().executeZOrderBy("state", "year_month")
print("\n📤 OUTPUT: the table after Z-ORDER")
n2 = health("after Z-ORDER")
m2 = files_with_matches()
print(f"   rows for RJ + 2018-03 sit in {m2} of {n2} files")
timed_query("query time")
show_file_stats("📤 OUTPUT: the statistics Delta stores per file AFTER the Z-ORDER")
u.pause(SEE_OBSERVE)
u.observe(
    "Terminal, the 'after Z-ORDER' line, the new 'sit in X of Y files' line and the per-file statistics",
    "The query now needs far fewer files than before. Compare the two statistics tables: before, EVERY file spanned the full range of states and months; "
    "after, the ranges are narrower (some files cover only part of the states or only part of the months).",
    "Z-ORDER clusters similar values together, so Delta's min/max statistics can skip files that cannot match.",
)
u.why("Z-ORDER sorts rows so that each file covers a narrow range of the columns you filter on. Delta's min/max stats can then SKIP files that cannot match. That is data skipping.")
u.warn("Z-order on the columns you filter on most, a few at most. There are no row-level index structures in Spark/Delta: skipping comes from partitions, min/max stats and Z-order.")
u.pause()

# ------------------------------------------------------------------------------------------
u.header(8, "VACUUM: the old files are still on disk (slide 22)")
on_disk, in_table = u.count_files(table_path), dt.detail().collect()[0]["numFiles"]
print("📥 INPUT DATA: the table folder after OPTIMIZE and Z-ORDER")
u.show_tree(table_path, 8)
print(f"\n   Parquet files on disk: {on_disk}      files the table uses now: {in_table}")
u.predict("OPTIMIZE finished. Did the folder get smaller? Why / why not?")
print("\n📤 OUTPUT: the folder size")
print(f"   folder size: {u.folder_size_mb(table_path):.1f} MB (the table itself is {dt.detail().collect()[0]['sizeInBytes'] / 1024 / 1024:.1f} MB)")
u.pause(SEE_OBSERVE)
u.observe(
    "Terminal, the two lines just printed (and Finder ➜ demo_output/s04/sales_delta)",
    "Parquet files on disk vs files the table uses now. The folder is bigger than the table itself.",
    "OPTIMIZE wrote new files but kept the old ones on disk. Only VACUUM removes them.",
)
u.why("OPTIMIZE and MERGE never delete: older versions (time travel) and running queries still need the old files.")
u.pause()
print("Try a dangerous VACUUM first:   dt.vacuum(0)")
print("\n📤 OUTPUT: what Delta answers")
try:
    dt.vacuum(0)
    print("   ran (unexpected)")
except Exception as e:
    msg = str(e).replace("\n", " ")
    print("   🛑 Delta REFUSED ➜", msg[:230])
print("\nDry run with the default 7-day retention:")
res = spark.sql(f"VACUUM delta.`{u.uri(table_path)}` DRY RUN")
print(f"   files that would be deleted: {res.count()}   (zero: everything is newer than 7 days)")
u.pause(SEE_OBSERVE)
u.observe(
    "Terminal, the REFUSED message and the dry-run line",
    "Delta refuses vacuum(0) and mentions the retention period. The dry run with the default retention would delete zero files.",
    "A safety net protects files that old versions and running queries may still need.",
)
u.why("The default retention is 7 days (168 hours). Delta blocks anything shorter, because a running query or a slow reader could still need those files.")
u.pause()

u.warn("DEMO ONLY: we switch the safety check off to see VACUUM delete files. Never do this on a shared table.")
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
dry = spark.sql(f"VACUUM delta.`{u.uri(table_path)}` RETAIN 0 HOURS DRY RUN").count()
print("📥 INPUT DATA: the same table, now with the safety check switched off (DEMO ONLY)")
print(f"   dry run with 0 hours: would delete {dry} files")
dt.vacuum(0)
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "true")
print(f"   Parquet files on disk now: {u.count_files(table_path)}   (table uses {dt.detail().collect()[0]['numFiles']})   folder: {u.folder_size_mb(table_path):.1f} MB")
u.predict("We just cleaned up. Can we still time-travel to version 0 (the original 60 small files)?")
print("\n📤 OUTPUT: reading version 0 of the table")
try:
    # sum(price) needs the real data files (a plain count() could be answered from the log statistics)
    spark.read.format("delta").option("versionAsOf", 0).load(u.uri(table_path)).agg(F.sum("price")).collect()
    print("   version 0 still readable")
except Exception as e:
    error_class = next(iter(re.findall(r"\[([A-Z_]+(?:\.[A-Z_]+)*)\]", str(e))), "read error")
    print(f"   🛑 version 0 is GONE ➜ Spark cannot read it any more ({error_class}): the data files it needs were deleted")
u.pause(SEE_OBSERVE)
u.observe(
    "Terminal, the 'Parquet files on disk now' line and the result of time-travel to version 0",
    "Files on disk dropped to the number the table uses, the folder shrank, and reading version 0 now FAILS (the 'GONE' message).",
    "VACUUM deleted the files that old versions needed. Cleaning up and time travel pull in opposite directions.",
)
u.why("VACUUM is the janitor, time travel is the CCTV: the more you clean, the less footage you keep. Typical practice: run OPTIMIZE then VACUUM on a schedule, keep the default retention.")
u.pause()

# ------------------------------------------------------------------------------------------
u.header(9, "Your optimisation ladder (slide 23)")
for symptom, fix in [
    ("Query reads too much data", "Parquet / Delta + partition pruning"),
    ("Thousands of tiny files", "repartition before write, OPTIMIZE"),
    ("One task runs forever", "skew: AQE, broadcast, salting"),
    ("Storage keeps growing", "VACUUM (mind the retention)"),
    ("Half-written or clashing writes", "Delta transactions + MERGE"),
]:
    print(f"   {symptom:<34} ➜ {fix}")
print("\nRule: when a job is slow, do not guess. Open the Spark UI, find the symptom, apply the matching fix.")

print("\n" + "=" * 72)
print("CLEAN REFERENCE (copy this; no pauses, no prints)")
print("=" * 72)
print('''
dt = DeltaTable.forPath(spark, path)
dt.optimize().executeCompaction()                        # fewer, bigger files
dt.optimize().executeZOrderBy("state", "year_month")     # cluster for data skipping
dt.vacuum()                                              # delete unused files older than 7 days

# skew
spark.conf.set("spark.sql.adaptive.enabled", "true")           # on by default in modern Spark
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
big.join(F.broadcast(small), "key")                            # no shuffle of the big side
''')
u.finish(spark, "🎉 That was the last demo. Next session: the same code on Databricks.")
