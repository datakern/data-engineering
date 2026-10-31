"""
DataKern | Session 7 | Demo 2 of 5 | REPARTITION vs COALESCE, AND THE TIDY WRITE
Slides 11, 12 (Act 2: Storage)

In demo 1, step 2, partitionBy gave us far more files than folders. Today we find out why, fix it,
and build the BRONZE layer of our mini lakehouse: tidy, partitioned Parquet that the next demos reuse.

  Step 1  repartition vs coalesce: the plan and the partition sizes
  Step 2  The small-file problem: why partitionBy made so many files
  Step 3  The fix: repartition by the same column before you write (this is BRONZE)
  Step 4  The coalesce(1) trap

Run:   python 02_repartition_and_tidy_write.py          (about 8 minutes)
Next:  03_delta_log_and_acid.py
"""
from pyspark.sql import functions as F

import demo_utils as u

# Adaptive Query Execution (AQE) quietly merges small shuffle partitions. That is great in production and
# bad for a demo about partition counts, so we switch it off here. Every other demo leaves it on.
spark = u.get_spark("S7 demo 2: repartition and tidy write", extra_conf={"spark.sql.adaptive.enabled": "false"})
print("ℹ️  AQE is switched off in this demo only, so that the partition counts you see are the ones we ask for.")
u.pause("Open the Spark UI address above, then press ENTER")
orders = u.build_orders(spark)


def rows_per_partition(df):
    """List of row counts, one per partition (empty partitions show up as 0)."""
    return df.rdd.mapPartitions(lambda rows: [sum(1 for _ in rows)]).collect()


def show_sizes(label, df):
    sizes = rows_per_partition(df)
    print(f"   {label:<26} {len(sizes):>3} partitions | rows per partition: {sizes}")
    return sizes


# ============================================================== STEP 1
u.header(1, "repartition vs coalesce: the plan and the partition sizes")
print("A partition is a slice of the data. One partition = one task = one core.")
print(f"The orders table has {orders.rdd.getNumPartitions()} partitions: a number Spark chose, not us. We only choose when we call repartition / coalesce.")
print("\n   First the PLAN. Going down to 3 partitions, the top line of explain():")
print("   df.repartition(3)  →", u.plan_top(orders.repartition(3))[0][:90])
print("   df.coalesce(3)     →", u.plan_top(orders.coalesce(3))[0][:90])
u.say("Exchange = shuffle = every row may cross the network. Coalesce just glues neighbouring partitions together.")

print("\nNow the SIZES. We start from an UNEVEN layout: hash the rows by state into 12 partitions.")
print("São Paulo (SP) is about 42% of all orders, so one partition will be much bigger than the others.")
u.predict("From the uneven start, which one gives EVEN partitions when we go down to 4: repartition(4) or coalesce(4)?")

uneven = orders.repartition(12, "state")
show_sizes("start: repartition(12,state)", uneven)
show_sizes("repartition(4)", uneven.repartition(4))
show_sizes("coalesce(4)", uneven.coalesce(4))
print("\n   And what about going UP? Ask for 20 partitions from 12:")
show_sizes("coalesce(20)", uneven.coalesce(20))
show_sizes("repartition(20)", uneven.repartition(20))
u.observe(
    "the five lines above",
    "repartition(4) is almost perfectly even. coalesce(4) keeps the big partition big. coalesce(20) stays at 12",
    "repartition = shuffle (Exchange in the plan), even sizes, up or down. coalesce = no shuffle, may be lopsided, DOWN only.",
)
u.say("Pulse check from the slide: 200 partitions → 10, cheaply, no shuffle. Which one? coalesce(10).")
u.pause()

# ============================================================== STEP 2
u.header(2, "The small-file problem: why did partitionBy make so many files?")
print("Each TASK writes its own file into every folder it has rows for.")
u.predict("Several tasks, 25 month folders, and every task holds rows of almost every month. How many files, roughly?")

plain_dir = u.fresh_dir("s02", "write_plain")
orders.write.partitionBy("year_month").parquet(u.uri(plain_dir))
n_files = u.count_files(plain_dir)
mb = u.folder_size_mb(plain_dir)
u.ok(f"Plain write: {n_files} files for 25 folders, {mb:.2f} MB in total, average file {mb * 1024 / n_files:.0f} KB")
print("   Files inside one busy month:")
busy = plain_dir / "year_month=2018-01"
for f in sorted(busy.glob("*.parquet")):
    print(f"      {f.stat().st_size / 1024:>6.0f} KB   {f.name[:40]}...")
u.observe(
    f"your folder demo_output/s02/write_plain/year_month=2018-01/",
    "several small files for ONE month",
    "task 1 wrote Jan+Feb+Mar... task 2 wrote Jan+Feb+Mar... Every task leaves a file in every folder. Readers must open them all.",
)
u.pause()

# ============================================================== STEP 3
u.header(3, "The fix: repartition by the same column before you write")
print("If all rows of one month land in ONE task, that task writes ONE file for that month.")
print("We write this one into demo_output/lakehouse/bronze_orders/: it is the BRONZE layer that demos 3 to 5 build on.")
u.predict("We add repartition('year_month') before the write. How many files now?")

bronze_dir = u.fresh_dir("lakehouse", "bronze_orders")
orders.repartition("year_month").write.partitionBy("year_month").mode("overwrite").parquet(u.uri(bronze_dir))
n_tidy = u.count_files(bronze_dir)
mb_tidy = u.folder_size_mb(bronze_dir)
check = spark.read.parquet(u.uri(bronze_dir))
u.ok(f"Tidy write: {n_tidy} files (was {n_files}), {mb_tidy:.2f} MB, average file {mb_tidy * 1024 / n_tidy:.0f} KB. BRONZE has {check.count():,} orders.")
u.observe(
    "your folder demo_output/lakehouse/bronze_orders/",
    "exactly one part-*.parquet in every month folder",
    "same data, same folders, a fraction of the files. The cost: one shuffle before the write. It is worth it.",
)
u.say("Tidy does not mean equal: 2016-09 holds only 4 orders, so its file is tiny. Thousands of tiny files is the problem, not a handful.")
u.pause()

# ============================================================== STEP 4
u.header(4, "The coalesce(1) trap")
print("'One file is tidiest, so let us coalesce(1)!' Let us see who writes it.")
u.predict("coalesce(1) before the write. How many TASKS write the files? Open the Stages tab and count.")

one_dir = u.fresh_dir("s02", "write_one_task")
orders.coalesce(1).write.partitionBy("year_month").parquet(u.uri(one_dir))
tasks = u.last_stage_tasks(spark)
u.ok(f"{u.count_files(one_dir)} files written, by a stage with {tasks} task(s)")
u.observe(
    "Spark UI ➜ Stages tab ➜ the last stage ➜ 'Tasks'",
    "a single task did all the writing while the other cores sat idle",
    "coalesce(1) makes ONE task write everything. On 500 GB that is one core for hours. Use repartition('month'): one task PER month.",
)

u.reference(
    '''
# --- the clean reference: demo 2 without pauses --------------------------------
df.repartition(3)      # shuffle, even sizes, up or down        → Exchange in the plan
df.coalesce(3)         # no shuffle, may be lopsided, down only → Coalesce in the plan

# plain partitioned write: every task writes a file into every folder  → many small files
orders.write.partitionBy("year_month").parquet(path)

# tidy write: all rows of a month meet in one task                     → one file per folder
orders.repartition("year_month").write.partitionBy("year_month").parquet(path)

# trap: coalesce(1) → ONE task writes everything, the other cores idle

# BRONZE layer
orders.repartition("year_month").write.partitionBy("year_month").mode("overwrite").parquet("lakehouse/bronze_orders")
'''
)
print("\n➡️  NEXT: python 03_delta_log_and_acid.py")
print("   Bronze is plain Parquet. Parquet has no memory of what changed and no protection against a crash.")
print("   In demo 3 we add the transaction log and turn it into a Delta table.")
u.finish(spark)
