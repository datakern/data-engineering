"""
DataKern | Spark session demos | 01  WINDOW FUNCTIONS and UDFs      (slides 9 to 12)

Story: the Olist marketplace wants (a) every customer's LATEST order, (b) the top 3 products
per category, (c) running revenue per seller, without losing the detail rows.
That is exactly what groupBy cannot do and window functions can.

How every step runs:
  📥 INPUT DATA   the table the query works on  ➜ ENTER: explain the query on this data
  🤔 PREDICT      a question to answer before the query runs
  📤 OUTPUT       what the query produced       ➜ ENTER: then the OBSERVE tells you what to look at

Keep the Spark UI open (the address is printed when Spark starts):
  Jobs tab   : how many jobs each step starts
  Stages tab : how many TASKS a stage has (watch for "1 task" in step 7)
  SQL tab    : the plan of each query
Run:   python 01_windows_and_udf.py          (about 5-8 minutes)
"""

import demo_utils as u
import numpy as np
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.window import Window

DIVIDER = "-" * 90
RUN_QUERY = "Press ENTER to run the query"
SEE_OBSERVE = "Press ENTER to see what to observe"

# AQE would merge our tiny 8 shuffle partitions into 1, which hides step 7.
# We switch that merging off for this demo only.
spark = u.get_spark(
    "01 windows and UDFs",
    extra_conf={"spark.sql.adaptive.coalescePartitions.enabled": "false"},
)


# ==========================================================================================
# STEP 1 | Load Olist and build one wide 'sales' table
# ==========================================================================================
u.header(1, "Load Olist and build one wide 'sales' table")

print("📥 INPUT DATA: 5 raw CSV files (plain text: the header line, then two data rows). Spark has not read them yet.")
u.peek_file("olist_order_items_dataset.csv")
u.peek_file("olist_orders_dataset.csv")
u.peek_file("olist_customers_dataset.csv")
u.peek_file("olist_products_dataset.csv")
u.peek_file("product_category_name_translation.csv")
print("\n   Items say WHAT was bought (product, seller, price), orders say WHEN and by which customer_id,")
print("   customers say WHO (customer_unique_id) and WHERE, products and the translation give the category name.")
print("   We join them into ONE wide table called 'sales'.")
u.pause(RUN_QUERY)

sales = u.build_sales(spark)
sales.cache()  # we reuse 'sales' many times below, so caching is worth it (recap slide 6!)
n_rows = sales.count()

u.ok(f"sales has {n_rows:,} rows: one row per order item.")
u.result(
    "'sales' = one row per order item (who bought, when, what, for how much)",
    sales.select("order_id", "customer_unique_id", "purchase_ts", "category", "price", "state"),
    n=5,
    truncate=25,
)

u.pause(SEE_OBSERVE)
u.observe(
    "Spark UI ➜ Jobs tab (refresh the page)",
    "The list was empty while we 'loaded' 5 CSV files, joined them and called cache(). Jobs only appeared when count() ran. "
    "Ignore the many small jobs named '$anonfun...' (Spark's own helpers for the joins). Find the one named 'count at ...'.",
    "Spark is LAZY: transformations only build a plan, an ACTION is what starts the work.",
)
u.observe(
    "Spark UI ➜ Storage tab",
    "One cached entry (its name is a long query-plan text: that is our sales table) with 'Fraction Cached' 100% and a size in memory.",
    "cache() was only a promise. count() filled it. From now on we read from memory instead of redoing 5 CSVs and 4 joins.",
)
u.pause()


# ==========================================================================================
# STEP 2 | groupBy vs window (slide 9)
# ==========================================================================================
print(DIVIDER)
u.header(2, "groupBy vs window on a tiny table (slide 9)")

toy = spark.createDataFrame(
    [("ana", 1, 40), ("ana", 2, 25), ("ben", 3, 60), ("ben", 4, 10)],
    "customer STRING, order_no INT, amount INT",
)
u.sample("4 orders of 2 customers", toy)

u.predict("groupBy('customer').sum('amount') on these 4 rows: how many rows come out? And with a window?")

toy_window = Window.partitionBy("customer").orderBy("order_no")
u.result("groupBy('customer').sum('amount')", toy.groupBy("customer").agg(F.sum("amount").alias("total")))
u.result("window: running total per customer", toy.withColumn("running", F.sum("amount").over(toy_window)))

u.pause(SEE_OBSERVE)
u.observe(
    "Terminal, the two small tables just printed",
    "Count the rows in each table (2 vs 4). Which one still shows every single order? Find the new 'running' column.",
    "groupBy COLLAPSES rows (4 in, 2 out). A window KEEPS every row and adds a column (4 in, 4 out).",
)
u.pause()

# ---- the same idea on real data: revenue per seller ----------------------------------
print("Now the same idea on real data: revenue per seller.")
top_seller_id = (
    sales.groupBy("seller_id")
    .agg(F.sum("price").alias("total"))
    .orderBy(F.desc("total"))
    .first()[0]
)
u.sample(
    f"'sales' ({n_rows:,} rows, one per order item). Here: the items of ONE seller ({top_seller_id[:8]}...), the biggest",
    sales.filter(F.col("seller_id") == top_seller_id).select("order_id", "seller_id", "price"),
)

seller_groupby = sales.groupBy("seller_id").agg(F.sum("price").alias("seller_revenue"))
seller_window = sales.withColumn(
    "seller_revenue",
    F.sum("price").over(Window.partitionBy("seller_id")),
)
# Filtering on the window column stops Spark from dropping the Window when we only count rows.
seller_window_kept = seller_window.filter("seller_revenue > 0")

u.predict(f"sales has {n_rows:,} rows. How many rows will each version return?")
u.result(
    "groupBy('seller_id'): that same seller",
    seller_groupby.filter(F.col("seller_id") == top_seller_id),
)
u.result(
    "window over seller_id: the same seller (every item kept, plus seller_revenue)",
    seller_window.filter(F.col("seller_id") == top_seller_id).select("order_id", "seller_id", "price", "seller_revenue"),
)
print(f"   groupBy ➜ {seller_groupby.count():,} rows in total  (one per seller)")
print(f"   window  ➜ {seller_window_kept.count():,} rows in total  (every order item kept, plus seller_revenue)")

print("\nThe plan lines that matter for each version:")
print("   groupBy:")
u.show_plan_lines(seller_groupby, "hashpartitioning(seller_id")
print("   window :")
u.show_plan_lines(seller_window_kept, "Window [", "hashpartitioning(seller_id")

u.pause(SEE_OBSERVE)
u.observe(
    "Terminal, the plan lines just printed",
    "Both plans have an 'Exchange hashpartitioning(seller_id ...)' line. Only the window plan also has a 'Window [...]' line.",
    "Exchange = a shuffle: all rows of one seller must travel to the same place. Both versions pay for it. "
    "The only difference is the output: one row per seller vs every row kept.",
)
u.observe(
    "Spark UI ➜ SQL / DataFrame tab ➜ the two newest queries (older = groupBy, newer = window). The pictures are big because they include how 'sales' was built",
    "Open the newer query, scroll to the bottom and click 'Plan Details' (the plan as text), then use Ctrl+F / Cmd+F to search for 'Window' and 'hashpartitioning'. "
    "The groupBy query has no 'Window' text.",
    "The same plan lines you just saw in the terminal. The picture and the text are two views of one plan.",
)
u.pause()


# ==========================================================================================
# STEP 3 | Latest order per customer (slide 10)
# ==========================================================================================
print(DIVIDER)
u.header(3, "Latest order per customer: partitionBy + orderBy + row_number (slide 10)")

# --- 3a. groupBy: from order ITEMS to ORDERS ---------------------------------------------
multi_item_order = (
    sales.groupBy("order_id").count().filter("count >= 2").orderBy("order_id").first()[0]
)
u.sample(
    "'sales' = one row per order ITEM. Here: ONE order that contains several items",
    sales.filter(F.col("order_id") == multi_item_order).select(
        "order_id", "customer_unique_id", "customer_id", "purchase_ts", "price"
    ),
    truncate=22,
)
print("   groupBy order_id and add up the prices ➜ one row per ORDER, with its order_value.")
u.pause(RUN_QUERY)

orders = sales.groupBy("order_id", "customer_unique_id", "customer_id", "purchase_ts").agg(
    F.round(F.sum("price"), 2).alias("order_value")
)
orders.cache()
orders.count()  # materialise the cache: 'orders' is reused in steps 3, 6 and 7

u.result(
    "'orders': the same order is now ONE row, its items added up in order_value",
    orders.filter(F.col("order_id") == multi_item_order).select(
        "order_id", "customer_unique_id", "customer_id", "purchase_ts", "order_value"
    ),
    truncate=22,
)

u.warn("Olist trap: customer_id is NEW for every order. The real person is customer_unique_id.")
print(f"   distinct customer_id        : {orders.select('customer_id').distinct().count():,}")
print(f"   distinct customer_unique_id : {orders.select('customer_unique_id').distinct().count():,}")
u.why("If we partitioned by customer_id every 'customer' would have exactly 1 order and the demo would be pointless.")
u.pause()

# --- 3b. window: number each customer's orders --------------------------------------------
# One customer who ordered 3+ times, weeks apart: we follow them through steps 3, 6 and 7
repeat_customer_ids = [
    row[0]
    for row in orders.groupBy("customer_unique_id")
    .agg(F.count("*").alias("n_orders"), F.datediff(F.max("purchase_ts"), F.min("purchase_ts")).alias("span_days"))
    .filter("n_orders >= 3 AND span_days >= 90")
    .orderBy("customer_unique_id")
    .limit(1)
    .collect()
]
is_repeat_customer = F.col("customer_unique_id").isin(repeat_customer_ids)

u.sample("'orders' = one row per order", orders.select("order_id", "customer_unique_id", "purchase_ts", "order_value"))
u.sample(
    "ONE customer who ordered several times, oldest first. We want only their NEWEST order",
    orders.filter(is_repeat_customer).orderBy("purchase_ts").select("customer_unique_id", "purchase_ts", "order_value"),
)

# Number each customer's orders from newest (1) to oldest
latest_first = Window.partitionBy("customer_unique_id").orderBy(F.col("purchase_ts").desc())
ranked_orders = orders.withColumn("n", F.row_number().over(latest_first))

u.predict("After keeping only n = 1, how many rows should we have? (hint: one per ...?)")
u.result(
    "the same customer after the window: column n numbers their orders, newest first",
    ranked_orders.filter(is_repeat_customer).orderBy("n").select("customer_unique_id", "purchase_ts", "order_value", "n"),
)
latest_orders = ranked_orders.filter("n = 1")
u.ok(f"keeping only n = 1: {latest_orders.count():,} rows (= number of distinct customer_unique_id)")

u.pause(SEE_OBSERVE)
u.observe(
    "Terminal, compare the input table of that ONE customer with the table just printed",
    "Same orders, now with column n. Read n from top to bottom and compare purchase_ts next to it.",
    "n = 1 is the NEWEST order, n = 2 the one before. Keeping only n = 1 gives exactly one row per customer: the 'latest order' pattern.",
)
u.pause()


# ==========================================================================================
# STEP 4 | row_number vs rank vs dense_rank (slide 10)
# ==========================================================================================
print(DIVIDER)
u.header(4, "row_number vs rank vs dense_rank when there are ties (slide 10)")

ties = spark.createDataFrame(
    [("toys", "ball", 100), ("toys", "kite", 100), ("toys", "robot", 80), ("toys", "puzzle", 50)],
    "category STRING, product STRING, revenue INT",
)
by_revenue = Window.partitionBy("category").orderBy(F.col("revenue").desc())

u.sample("4 products of one category, with their revenue (ball and kite tie)", ties)

u.predict("ball and kite tie at 100. What rank does 'robot' get with rank()? and with dense_rank()?")
u.result(
    "the three ranking functions side by side",
    ties.select(
        "product",
        "revenue",
        F.row_number().over(by_revenue).alias("row_number"),
        F.rank().over(by_revenue).alias("rank"),
        F.dense_rank().over(by_revenue).alias("dense_rank"),
    ),
)

u.pause(SEE_OBSERVE)
u.observe(
    "Terminal, the table just printed: the three rank columns",
    "ball and kite both have 100. Look at the number each function gives to the NEXT row, robot.",
    "row_number never repeats (the winner of a tie is arbitrary). rank leaves a GAP after a tie (1,1,3). dense_rank does not (1,1,2). "
    "One row per group: row_number. Top N including ties: dense_rank.",
)
u.pause()


# ==========================================================================================
# STEP 5 | Top 3 products per category
# ==========================================================================================
print(DIVIDER)
u.header(5, "Top 3 products per category: groupBy FIRST, window SECOND")

u.sample(
    "'sales' = one row per order item. Here: some toys",
    sales.filter("category = 'toys'").select("order_id", "category", "product_id", "price"),
)
print("   First groupBy category + product_id and add up the prices ➜ one row per PRODUCT.")
u.pause(RUN_QUERY)

# groupBy: one row per product
product_revenue = sales.groupBy("category", "product_id").agg(
    F.round(F.sum("price"), 2).alias("revenue")
)
u.result(
    "after the groupBy: ONE ROW PER PRODUCT (here: some toys, in no particular order). This is the input of the window",
    product_revenue.filter("category = 'toys'"),
    n=5,
)
print("   Now rank the products INSIDE each category with dense_rank and keep ranks 1 to 3.")
u.pause(RUN_QUERY)

# window: rank products inside each category
by_category_revenue = Window.partitionBy("category").orderBy(F.col("revenue").desc())
top3_per_category = product_revenue.withColumn(
    "rank_in_category", F.dense_rank().over(by_category_revenue)
).filter("rank_in_category <= 3")

sample_categories = ["health_beauty", "toys", "computers_accessories"]
u.result(
    "the top 3 products of three categories",
    top3_per_category.filter(F.col("category").isin(sample_categories)).orderBy("category", "rank_in_category"),
    n=12,
    truncate=18,
)

u.pause(SEE_OBSERVE)
u.observe(
    "Terminal, the table just printed",
    "For each category check that rank_in_category 1 has the highest revenue and the ranks go down from there.",
    "Real pipelines mix both tools: groupBy builds one row per product, then the window ranks products INSIDE each category.",
)
u.pause()


# ==========================================================================================
# STEP 6 | lag() and a running total (slide 10)
# ==========================================================================================
print(DIVIDER)
u.header(6, "lag() and a running total (slide 10)")

# --- 6a. lag(): days since the customer's previous order ---------------------------------
oldest_first = Window.partitionBy("customer_unique_id").orderBy("purchase_ts")
order_gaps = orders.withColumn("previous_order", F.lag("purchase_ts").over(oldest_first)).withColumn(
    "days_since_previous", F.datediff("purchase_ts", "previous_order")
)

u.sample(
    "'orders': the same repeat customer as in step 3, oldest order first",
    orders.filter(is_repeat_customer).orderBy("purchase_ts").select("customer_unique_id", "purchase_ts"),
)
u.pause(RUN_QUERY)
u.result(
    "after lag(): each order also knows the one before it",
    order_gaps.filter(is_repeat_customer)
    .orderBy("purchase_ts")
    .select("customer_unique_id", "purchase_ts", "previous_order", "days_since_previous"),
)
u.pause()

# --- 6b. running total: revenue per seller, month by month -------------------------------
monthly_revenue = (
    sales.withColumn("month", F.date_trunc("month", "purchase_ts"))
    .groupBy("seller_id", "month")
    .agg(F.round(F.sum("price"), 2).alias("month_revenue"))
)

# the 'frame': every row from the seller's first month up to the current one
running_frame = (
    Window.partitionBy("seller_id")
    .orderBy("month")
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
)

u.sample(
    f"revenue per seller and month (a groupBy of sales). Here: the biggest seller ({top_seller_id[:8]}...), oldest month first",
    monthly_revenue.filter(F.col("seller_id") == top_seller_id).orderBy("month"),
    n=8,
    truncate=25,
)

u.predict("Running total: what is the running value in a seller's FIRST month? And in the last?")
u.result(
    "the same seller after the running total: one more column",
    monthly_revenue.withColumn("running_revenue", F.round(F.sum("month_revenue").over(running_frame), 2))
    .filter(F.col("seller_id") == top_seller_id)
    .orderBy("month")
    .select("month", "month_revenue", "running_revenue"),
    n=8,
    truncate=25,
)

u.pause(SEE_OBSERVE)
u.observe(
    "Terminal, the last two tables printed (lag for one customer, running revenue for one seller)",
    "In the first: previous_order is simply the row above's purchase_ts, and the very first order has NULL (nothing before it). In the second: in the first month "
    "running_revenue equals month_revenue, then it only grows, month after month.",
    "lag() looks at the previous row. rowsBetween(unboundedPreceding, currentRow) is the 'frame': every row from the start up to now.",
)
u.pause()


# ==========================================================================================
# STEP 7 | DANGER: a window without partitionBy (slide 11)
# ==========================================================================================
print(DIVIDER)
u.header(7, "DANGER: a window without partitionBy (slide 11)")

without_partition = orders.withColumn(
    "n", F.row_number().over(Window.orderBy("purchase_ts"))
)
with_partition = orders.withColumn(
    "n",
    F.row_number().over(
        Window.partitionBy("customer_unique_id").orderBy("purchase_ts")
    ),
)

u.sample("'orders' again: one row per order", orders.select("order_id", "customer_unique_id", "purchase_ts"))
print("   We number the orders with row_number() in two ways:")
print("   WITHOUT partitionBy: ONE counter over ALL orders.   WITH partitionBy: a new counter per customer.")
u.pause(RUN_QUERY)

u.result(
    "WITHOUT partitionBy: the 5 oldest orders of the whole table get n = 1, 2, 3, 4, 5",
    without_partition.orderBy("n").select("customer_unique_id", "purchase_ts", "n"),
    n=5,
)
u.result(
    "WITH partitionBy: the repeat customer from step 3 (the counter restarts for each customer)",
    with_partition.filter(is_repeat_customer).orderBy("purchase_ts").select("customer_unique_id", "purchase_ts", "n"),
)

u.predict("Which plan will move all rows into ONE partition? How many partitions will each result use?")

PLAN_ONE_PARTITION = "Exchange SinglePartition"
PLAN_HASH_PARTITION = "Exchange hashpartitioning(customer_unique_id"

print("\nWindow WITHOUT partitionBy, the exchange line of the plan:")
u.show_plan_lines(without_partition, PLAN_ONE_PARTITION, PLAN_HASH_PARTITION)

print("\nWindow WITH partitionBy, the exchange line of the plan:")
u.show_plan_lines(with_partition, PLAN_ONE_PARTITION, PLAN_HASH_PARTITION)

print(f"\n   Partitions used by the window WITHOUT partitionBy: {without_partition.rdd.getNumPartitions()}")
print(f"   Partitions used by the window WITH    partitionBy: {with_partition.rdd.getNumPartitions()}")

with u.Timer("without partitionBy (all rows in ONE task)"):
    u.noop(without_partition)
with u.Timer("with partitionBy (rows spread over tasks)"):
    u.noop(with_partition)

u.pause(SEE_OBSERVE)
u.observe(
    "Terminal, the two 'exchange line' lines and the partition counts just printed",
    "The first plan says 'SinglePartition'. The second says 'hashpartitioning(customer_unique_id'. Compare the partition counts below them.",
    "SinglePartition = Spark moves EVERY row into one partition, so one task has to do all the work.",
)
u.observe(
    "Spark UI ➜ Jobs tab ➜ the two newest jobs named 'save at ...' (ignore the '$anonfun...' rows between them)",
    "Column 'Tasks'. The older job (without partitionBy) ends in a stage with a single task (1/1). The newer job (with partitionBy) ends in a stage of 8 tasks (8/8). "
    "Click the older job, then its single-task stage, and look at how much data that one task reads.",
    "One task = no parallelism. It also needs memory for the whole table.",
)
u.why("Without partitionBy Spark must put EVERY row in one partition to number them. On a big table that one task runs out of memory.")
u.warn(
    "Spark also prints a WARN 'No Partition Defined for Window operation' in the log. "
    "On our small laptop data the time difference is tiny. On 1 billion rows it is minutes versus a crash."
)
u.pause()


# ==========================================================================================
# STEP 8 | UDFs: built-in vs Python UDFs (slide 12)
# ==========================================================================================
print(DIVIDER)
u.header(8, "UDFs: built-in vs Python UDFs (slide 12)")

# Repeat every row 10 times so the cost of a UDF becomes visible
AMPLIFY = 10
big = (
    sales.select("price")
    .withColumn("copy", F.explode(F.sequence(F.lit(1), F.lit(AMPLIFY))))
    .drop("copy")
)
u.ok(f"Amplified to {big.count():,} rows so the cost becomes visible.")
u.sample("the prices of 'sales', every row repeated 10 times (only the price column)", big)
print("   The job: turn every price into a band: budget < 20, mid < 100, premium < 500, else luxury.")
print("   We write it three ways: a plain Python UDF, a pandas UDF, and built-in functions.")


# --- the same "price band" logic, written three ways -------------------------------------
# 1) a plain Python UDF: called once per row
def price_band_py(price: float | None) -> str | None:
    if price is None:
        return None
    if price < 20:
        return "budget"
    if price < 100:
        return "mid"
    if price < 500:
        return "premium"
    return "luxury"


price_band_udf = F.udf(price_band_py, "string")


# 2) a pandas UDF: called once per batch of rows
def price_band_batch(prices: pd.Series) -> pd.Series:
    bands = np.select(
        [prices < 20, prices < 100, prices < 500],
        ["budget", "mid", "premium"],
        default="luxury",
    )
    return pd.Series(bands, index=prices.index)


price_band_pandas = F.pandas_udf(price_band_batch, "string")  # pyright: ignore  (pyspark's type stubs reject this valid call)


# 3) built-in functions: never leave the JVM
price_band_builtin = (
    F.when(F.col("price") < 20, "budget")
    .when(F.col("price") < 100, "mid")
    .when(F.col("price") < 500, "premium")
    .otherwise("luxury")
)

variants = {
    "built-in F.when": big.withColumn("band", price_band_builtin),
    "plain Python UDF": big.withColumn("band", price_band_udf("price")),
    "pandas UDF (batches)": big.withColumn("band", price_band_pandas("price")),
}

u.predict("Three ways to do the same job. Rank them from fastest to slowest BEFORE we run them.")

# warm-up: starts the Python workers once, so the timings below are fair
for df in variants.values():
    df.limit(1000).collect()

timings = {}
for name, df in variants.items():
    with u.Timer(f"{name:<22}") as timer:
        u.noop(df)
    timings[name] = timer.seconds

baseline = timings["built-in F.when"]
print("\n   Slowdown compared with the built-in version:")
for name, seconds in sorted(timings.items(), key=lambda item: item[1]):
    print(f"   {name:<22} {seconds / max(baseline, 0.01):5.1f}x")

u.result(
    "all three versions give the same band for every price (example: one row per band)",
    variants["built-in F.when"].dropDuplicates(["band"]).orderBy("price"),
)

print("The plan lines that mention Python (EvalPython):")
for name, df in variants.items():
    print(f"\n   {name}:")
    u.show_plan_lines(df, "EvalPython")

u.pause(SEE_OBSERVE)
u.observe(
    "Terminal, the 'Slowdown' list just printed",
    "Which variant is 1.0x? How far behind are the two UDFs? Does the result match the ranking you predicted?",
    "Same input, same output, three very different costs. Python UDFs pay for moving every row between the JVM and Python.",
)
u.observe(
    "Spark UI ➜ SQL / DataFrame tab ➜ open the query of each variant (the three newest queries named 'save at ...')",
    "Look for a box with 'EvalPython' in its name (the plan lines above show the same thing). Only the two UDF versions have it. "
    "The built-in version is just a plain Project box.",
    "EvalPython is the point where Spark hands rows to Python. It is also a wall: Spark cannot optimise through it.",
)

u.why("Every UDF row leaves the JVM for a Python process and comes back, and Catalyst cannot look inside the function (no pushdown, no pruning around it).")
u.why("Spark 4 ships even plain UDFs to Python in Arrow batches by default, so the gap between plain and pandas UDFs is smaller than in older Spark. Both still lose to built-in functions.")
u.warn("Rules of thumb change between Spark versions: MEASURE on your own data. Built-in functions win because they never leave the JVM.")
u.pause()


# ------------------------------------------------------------------------------------------
print("\n" + "=" * 72)
print("CLEAN REFERENCE (copy this; no pauses, no prints)")
print("=" * 72)
print('''
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# latest order per customer
w = Window.partitionBy("customer_unique_id").orderBy(F.col("purchase_ts").desc())
latest = orders.withColumn("n", F.row_number().over(w)).filter("n = 1")

# top 3 products per category (groupBy first, window second)
w = Window.partitionBy("category").orderBy(F.col("revenue").desc())
top3 = prod_rev.withColumn("r", F.dense_rank().over(w)).filter("r <= 3")

# previous order and running total
w = Window.partitionBy("customer_unique_id").orderBy("purchase_ts")
orders.withColumn("previous_order", F.lag("purchase_ts").over(w))
w = Window.partitionBy("seller_id").orderBy("month").rowsBetween(Window.unboundedPreceding, Window.currentRow)
monthly.withColumn("running", F.sum("month_revenue").over(w))

# prefer built-in functions over UDFs
F.when(F.col("price") < 20, "budget").when(F.col("price") < 100, "mid").otherwise("premium")
''')

u.finish(
    spark,
    "NEXT: python 02_parquet_and_partitioning.py\n"
    "   You will learn: why the same data is faster as Parquet, and how folders on disk let Spark skip work.",
)
