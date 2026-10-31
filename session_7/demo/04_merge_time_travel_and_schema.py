"""
DataKern | Session 7 | Demo 4 of 5 | MERGE, TIME TRAVEL, SCHEMA ENFORCEMENT AND RESTORE
Slides 17, 18, 19 (Act 2: Storage)

The silver table holds orders up to July 2018. "Today's file" arrives: new orders AND corrections.
We load it with ONE statement, travel back in time, get rejected for sending the wrong columns,
then delete the whole table by accident and bring it back.

  Step 1  Today's file: new orders and corrections
  Step 2  MERGE: the duplicate-key trap, then the real thing (update and insert in one commit)
  Step 3  History and time travel
  Step 4  Schema enforcement, and the explicit opt-in
  Step 5  The accident: DELETE with no filter, then RESTORE

Run:   python 04_merge_time_travel_and_schema.py          (about 10 minutes)
Next:  05_optimize_zorder_vacuum.py
"""
import shutil

from delta.tables import DeltaTable
from pyspark.sql import functions as F

import demo_utils as u

spark = u.get_spark("S7 demo 4: merge, time travel, schema")
u.pause("Open the Spark UI address above, then press ENTER")
u.write_silver(spark)  # only builds silver if demo 3 was skipped

silver_path = u.uri(u.SILVER)
silver = DeltaTable.forPath(spark, silver_path)


def table():
    """Always read the latest version of silver."""
    return spark.read.format("delta").load(silver_path)


# ============================================================== STEP 1
u.header(1, "Today's file: new orders and corrections")
if silver.history().count() > 1:
    print("ℹ️  You ran this demo before. We roll silver back to version 0 so that we all start from the same place.")
    print("   (RESTORE is exactly what you learn in step 5.)")
    silver.restoreToVersion(0)
u.result("silver, orders per status", table().groupBy("order_status").count().orderBy(F.desc("count")), n=10)
latest = table().agg(F.date_format(F.max("purchase_ts"), "yyyy-MM-dd HH:mm").alias("m")).collect()[0]["m"]
print(f"   {table().count():,} orders. The newest purchase in silver is from {latest}.")
u.result("history: one commit so far", silver.history().select("version", "operation", "timestamp"), n=5, truncate=30)

print("\nNow today's file. Two kinds of rows arrive in one file:")
print("   NEW         the 6,532 real orders purchased in August 2018 or later (from bronze). Real data.")
print("   CORRECTION  300 orders silver still shows as 'shipped'. Today they are 'delivered'.")
u.say("Be honest with your students: Olist is a frozen dataset, so the corrections are CONSTRUCTED to give us updates. The new orders are real.")

bronze = spark.read.parquet(u.uri(u.BRONZE))
new_orders = bronze.where(F.col("purchase_ts") >= u.CUTOFF)
corrections = table().where("order_status = 'shipped'").limit(300).withColumn("order_status", F.lit("delivered"))
todays_file = new_orders.unionByName(corrections)
sample_order = corrections.select("order_id").first()["order_id"]

u.sample("a few NEW orders", new_orders.select("order_id", "state", "purchase_ts", "order_status", "order_value"), n=3)
u.sample("a few CORRECTIONS (status already changed to delivered)", corrections.select("order_id", "state", "purchase_ts", "order_status", "order_value"), n=3)
print(f"   today's file: {todays_file.count():,} rows = {new_orders.count():,} new + {corrections.count():,} corrections")
u.predict("After the MERGE: how many rows will the table have, and how many rows will be UPDATED?")
u.pause()

# ============================================================== STEP 2
u.header(2, "MERGE: the duplicate-key trap, then the real thing")
print("First the trap. MERGE needs the key (order_id) to be UNIQUE in the source. What if today's file lists one order twice?")
u.predict("We add 5 duplicated corrections to the file and run the MERGE. Does it work, pick one, or fail?")

dirty_file = todays_file.unionByName(corrections.limit(5))
try:
    (
        silver.alias("t").merge(dirty_file.alias("s"), "t.order_id = s.order_id")
        .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    )
    u.warn("The merge went through. That is unexpected.")
except Exception as e:
    u.warn(f"MERGE REFUSED: {type(e).__name__}")
    print("   " + str(e).splitlines()[0][:160])
print(f"   rows in silver after the failed merge: {table().count():,} (unchanged)")
u.say("DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE, and the table has the same number of rows (the red ERROR lines are Spark logging the same failure). Delta refuses to guess which duplicate wins, and the failed MERGE committed nothing. Fix: dropDuplicates on the key first.")
u.pause()

print("\nNow the real MERGE: update and insert in one atomic commit.")
print("Matched on order_id → UPDATE. Not matched → INSERT. One statement, one commit.")
clean_file = todays_file.dropDuplicates(["order_id"])

with u.Timer("MERGE"):
    (
        silver.alias("t").merge(clean_file.alias("s"), "t.order_id = s.order_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
m = silver.history(1).select("operationMetrics").collect()[0][0]
u.ok(f"MERGE committed. Rows in the source: {int(m['numSourceRows']):,}")
print(f"   inserted : {int(m['numTargetRowsInserted']):,}")
print(f"   updated  : {int(m['numTargetRowsUpdated']):,}")
print(f"   files    : {m['numTargetFilesRemoved']} old files removed, {m['numTargetFilesAdded']} new files added")
print(f"   copied   : {int(m['numTargetRowsCopied']):,} rows were re-written although they did not change")
shipped_now = table().where("order_status = 'shipped'").count()
print(f"\n   silver now has {table().count():,} orders; 'shipped' dropped from 1,059 to {shipped_now:,}")
u.observe(
    "Spark UI ➜ SQL tab ➜ the MERGE query, and demo_output/lakehouse/silver_orders/_delta_log/",
    "a new JSON file with 'remove' lines for the old files and 'add' lines for the new ones",
    "Parquet files are never edited. Delta rewrites the files that contain a changed row, then swaps them in the log in one commit. That is why MERGE costs more than an append.",
)
u.pause()

# ============================================================== STEP 3
u.header(3, "History and time travel")
print("Every commit is a version. You can read the table as it was at any of them.")
u.predict(f"Order {sample_order[:10]}... was 'shipped' at version 0. What does version 0 say now, and what does the latest version say?")

u.result("DeltaTable.history()", silver.history().select("version", "operation", "timestamp"), n=5, truncate=30)
v0 = spark.read.format("delta").option("versionAsOf", 0).load(silver_path)
print(f"   version 0 : {v0.count():,} orders | order {sample_order[:10]}... is '{v0.where(F.col('order_id') == sample_order).select('order_status').first()[0]}'")
print(f"   latest    : {table().count():,} orders | order {sample_order[:10]}... is '{table().where(F.col('order_id') == sample_order).select('order_status').first()[0]}'")
print("\n   The same in SQL:")
print(f"      SELECT count(*) FROM delta.`...silver_orders` VERSION AS OF 0   →",
      spark.sql(f"SELECT count(*) FROM delta.`{silver_path}` VERSION AS OF 0").first()[0], "rows")
u.observe(
    "the lines above",
    "version 0 still has 92,909 orders and the old status; the latest version has everything",
    "nothing was overwritten, so the past is still there. Time travel works until VACUUM deletes the old files (demo 5).",
)
u.pause()

# ============================================================== STEP 4
u.header(4, "Schema enforcement, and the explicit opt-in")
print("A new upstream system starts sending an extra column. And another one sends order_value as text.")
u.predict("We append 5 rows with an extra column 'surprise_col'. Then 5 rows with order_value as a string. Accepted or rejected?")

extra = new_orders.limit(5).withColumn("surprise_col", F.lit(1))
wrong_type = new_orders.limit(5).withColumn("order_value", F.col("order_value").cast("string"))
for label, df in [("extra column", extra), ("wrong type  ", wrong_type)]:
    try:
        df.write.format("delta").mode("append").save(silver_path)
        u.warn(f"{label}: accepted (unexpected)")
    except Exception as e:
        u.ok(f"{label}: REJECTED  {type(e).__name__}: {str(e).splitlines()[0][:70]}")
print(f"   silver still has {table().count():,} rows, columns unchanged: {len(table().columns)}")

print("\n   Evolving the schema is possible, but you must ASK for it. On a scratch copy:")
scratch = u.fresh_dir("s04", "scratch_schema")
shutil.copytree(u.SILVER, scratch)
extra.write.format("delta").mode("append").option("mergeSchema", "true").save(u.uri(scratch))
scratch_cols = spark.read.format("delta").load(u.uri(scratch)).columns
u.ok(f"with option('mergeSchema','true') the scratch table now has {len(scratch_cols)} columns; the new one is '{scratch_cols[-1]}'")
u.observe(
    "the lines above",
    "two REJECTED lines on silver, and 10 columns on the scratch copy only",
    "Consistency: a table keeps its contract. A schema change is a decision you make with mergeSchema, never an accident of a bad file.",
)
u.pause()

# ============================================================== STEP 5
u.header(5, "The accident: DELETE with no filter, then RESTORE")
print("Everybody does this once. A DELETE without a WHERE clause.")
u.predict("We delete everything from silver. Is the data lost? What command brings it back?")

version_before = silver.history(1).select("version").collect()[0][0]
silver.delete()  # same as: DELETE FROM silver_orders
print(f"   after DELETE: {table().count():,} rows (the log is at version {version_before + 1})")
silver.restoreToVersion(version_before)  # same as: RESTORE TABLE silver_orders TO VERSION AS OF <n>
u.ok(f"after RESTORE TO VERSION {version_before}: {table().count():,} rows are back")
u.result("history: the accident is in the log too", silver.history().select("version", "operation"), n=8)
u.observe(
    "the history table above",
    "WRITE, MERGE, DELETE, RESTORE. RESTORE is itself a new version, it does not erase the DELETE",
    "the undo button. It works because the old files were never deleted. Remember: VACUUM is the command that makes this impossible.",
)

u.reference(
    '''
# --- the clean reference: demo 4 without pauses --------------------------------
silver = DeltaTable.forPath(spark, "lakehouse/silver_orders")
todays_file = new_orders.unionByName(corrections).dropDuplicates(["order_id"])   # key must be UNIQUE in the source

silver.alias("t").merge(todays_file.alias("s"), "t.order_id = s.order_id") \\
      .whenMatchedUpdateAll() \\
      .whenNotMatchedInsertAll() \\
      .execute()                                     # one atomic commit

silver.history().show()                                                        # every commit
spark.read.format("delta").option("versionAsOf", 0).load(path)                 # the table as it was
spark.sql("SELECT * FROM delta.`path` VERSION AS OF 0")

df_with_extra_column.write.format("delta").mode("append").save(path)           # REJECTED (schema enforcement)
df_with_extra_column.write.format("delta").mode("append").option("mergeSchema", "true").save(path)   # explicit opt-in

silver.delete()                                  # oops, no filter
silver.restoreToVersion(1)                       # undo: RESTORE TABLE ... TO VERSION AS OF 1
'''
)
print("\n➡️  NEXT: python 05_optimize_zorder_vacuum.py")
print("   Silver is correct and complete: 99,441 orders. Now we keep the table healthy and fast: OPTIMIZE, Z-ORDER, VACUUM.")
u.finish(spark)
