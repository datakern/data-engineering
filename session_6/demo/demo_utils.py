"""
DataKern | Spark session demos | shared helpers
------------------------------------------------
Every demo imports this file. It gives you:
  pause()      "Observe Mode": stops so you can look at the Spark UI / folders
  predict()    asks you to guess BEFORE the code runs
  get_spark()  one place that builds the SparkSession (plain or with Delta)
  load_olist() loads the Olist CSV files with explicit schemas
  count_files(), folder_size_mb() ... small helpers to look at files on disk

Run any demo from this folder:   python 01_windows_and_udf.py
Skip all pauses (for a quick test run):   DEMO_AUTO=1 python 01_windows_and_udf.py
Windows PowerShell:   $env:DEMO_AUTO=1; python 01_windows_and_udf.py
"""
import os
import sys
import time
import shutil
from pathlib import Path

HERE = Path(__file__).resolve().parent
AUTO = os.environ.get("DEMO_AUTO") == "1"

# Where the Olist CSV files live. Change this line, or set the OLIST_DIR environment variable.
DATA_DIR = Path(os.environ.get("OLIST_DIR", HERE / "data" / "olist"))
# Everything the demos write goes here, so you can open the folders and look. Safe to delete.
OUT_DIR = HERE / "demo_output"

# PySpark must start the SAME Python you are running now (matters on Windows and with venvs).
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


# ---------------------------------------------------------------- Observe mode
def line(char="=", n=72):
    print(char * n)


def header(num, title):
    print()
    line("=")
    print(f"STEP {num}: {title}")
    line("=")


def pause(msg="Press ENTER to continue"):
    if AUTO:
        print("\n   [auto mode: continuing]\n")
        return
    input(f"\n🔍 {msg} ➔ ")
    print("\n" + "─" * 72)


def predict(question):
    print(f"\n🤔 PREDICT: {question}")
    if not AUTO:
        input("   Think first (say it out loud!), then press ENTER to run ➔ ")


def observe(where, look=None, meaning=None):
    """Tell the student WHERE to look, WHAT to look for, and WHY it matters.
    observe("Spark UI ➜ Stages tab", "one task far longer than the rest", "the stage waits for the slowest task")"""
    print(f"\n👀 OBSERVE ➜ {where}")
    if look:
        print(f"   LOOK FOR : {look}")
    if meaning:
        print(f"   MEANS    : {meaning}")


def sample(title, df, n=5, truncate=20, vertical=False):
    """INPUT: show a few rows of the table a query will work on, BEFORE the query, so students see what goes in."""
    print(f"\n📥 INPUT DATA: {title}")
    df.show(n, truncate=truncate, vertical=vertical)


def result(title, df, n=5, truncate=20, vertical=False):
    """OUTPUT: show what the query produced, so students can compare it with the input above."""
    print(f"\n📤 OUTPUT: {title}")
    df.show(n, truncate=truncate, vertical=vertical)


def peek_file(name, n=3, width=150):
    """Print the first lines of a raw data file (plain Python, so Spark runs nothing)."""
    print(f"\n   📄 {name}")
    with open(DATA_DIR / name, encoding="utf-8") as fh:
        for _ in range(n):
            print("      " + fh.readline().rstrip()[:width])


def why(text):
    print(f"💡 WHY: {text}")


def ok(text):
    print(f"✅ {text}")


def warn(text):
    print(f"🛑 {text}")


class Timer:
    """with Timer('label') as t:  ...  then t.seconds"""

    def __init__(self, label=""):
        self.label = label
        self.seconds = 0.0

    def __enter__(self):
        self.t0 = time.time()
        return self

    def __exit__(self, *exc):
        self.seconds = time.time() - self.t0
        if self.label:
            print(f"⏱️  {self.label}: {self.seconds:.2f}s")


# ------------------------------------------------------------------ Spark setup
def get_spark(app="DataKern demo", delta=False, shuffle_partitions=8, extra_conf=None):
    """Build a local SparkSession. delta=True adds Delta Lake (needs internet the first time)."""
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.master("local[*]")
        .appName(app)
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.driver.memory", os.environ.get("DEMO_DRIVER_MEM", "2g"))
    )
    for k, v in (extra_conf or {}).items():
        builder = builder.config(k, str(v))
    if delta:
        from delta import configure_spark_with_delta_pip

        builder = builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension").config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
    else:
        spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    print(f"⚡ Spark {spark.version} is running with {spark.sparkContext.defaultParallelism} cores")
    print(f"🌐 Spark UI ➔ {spark.sparkContext.uiWebUrl}   (open it in your browser NOW and keep it open)")
    return spark


# -------------------------------------------------------------------- Olist data
def _need(name):
    p = DATA_DIR / name
    if not p.exists():
        sys.exit(
            f"\n🛑 Cannot find {p}\n"
            f"   Put the Olist CSV files in: {DATA_DIR}\n"
            f"   or set the OLIST_DIR environment variable to the folder that holds them."
        )
    return str(p)


def load_olist(spark):
    """Return a dict of Olist DataFrames. Explicit schemas: faster than inferSchema, and zip codes stay text."""
    from pyspark.sql import functions as F

    def rd(name, ddl):
        return spark.read.csv(_need(name), header=True, schema=ddl)

    orders = rd(
        "olist_orders_dataset.csv",
        "order_id STRING, customer_id STRING, order_status STRING, order_purchase_timestamp TIMESTAMP, "
        "order_approved_at TIMESTAMP, order_delivered_carrier_date TIMESTAMP, "
        "order_delivered_customer_date TIMESTAMP, order_estimated_delivery_date TIMESTAMP",
    )
    items = rd(
        "olist_order_items_dataset.csv",
        "order_id STRING, order_item_id INT, product_id STRING, seller_id STRING, "
        "shipping_limit_date TIMESTAMP, price DOUBLE, freight_value DOUBLE",
    )
    customers = rd(
        "olist_customers_dataset.csv",
        "customer_id STRING, customer_unique_id STRING, customer_zip_code_prefix STRING, "
        "customer_city STRING, customer_state STRING",
    )
    # Only 2 of the 9 columns are needed; the schema is explicit so Spark does NOT run a
    # hidden job to read the header (that would show up as a stray "csv" job in the Jobs tab).
    products = rd(
        "olist_products_dataset.csv",
        "product_id STRING, product_category_name STRING, product_name_lenght INT, "
        "product_description_lenght INT, product_photos_qty INT, product_weight_g INT, "
        "product_length_cm INT, product_height_cm INT, product_width_cm INT",
    ).select("product_id", "product_category_name")
    translation = rd("product_category_name_translation.csv", "product_category_name STRING, product_category_name_english STRING")
    sellers = rd(
        "olist_sellers_dataset.csv",
        "seller_id STRING, seller_zip_code_prefix STRING, seller_city STRING, seller_state STRING",
    )
    return {
        "orders": orders,
        "items": items,
        "customers": customers,
        "products": products.join(translation, "product_category_name", "left"),
        "sellers": sellers,
    }


def build_sales(spark):
    """One wide 'sales' table: one row per order item, with who bought, when, what and where."""
    from pyspark.sql import functions as F

    t = load_olist(spark)
    sales = (
        t["items"]
        .join(t["orders"], "order_id")
        .join(t["customers"], "customer_id")
        .join(t["products"], "product_id", "left")
        .select(
            "order_id",
            "order_item_id",
            "customer_unique_id",
            "customer_id",
            F.col("order_purchase_timestamp").alias("purchase_ts"),
            "order_status",
            "seller_id",
            "product_id",
            F.coalesce("product_category_name_english", F.lit("unknown")).alias("category"),
            "price",
            "freight_value",
            F.col("customer_state").alias("state"),
            "customer_zip_code_prefix",
        )
    )
    return sales


# -------------------------------------------------------------------- file helpers
def fresh_dir(*parts):
    """Return a clean output folder (deletes the old one so reruns start fresh)."""
    p = OUT_DIR.joinpath(*parts)
    if p.exists():
        shutil.rmtree(p)
    p.parent.mkdir(parents=True, exist_ok=True)
    return p


def uri(path):
    """Path that Spark understands on Windows, Mac and Linux."""
    return Path(path).as_posix()


def count_files(path, suffix=".parquet"):
    return sum(1 for r, _, fs in os.walk(path) for f in fs if f.endswith(suffix))


def folder_size_mb(path):
    total = 0
    for r, _, fs in os.walk(path):
        for f in fs:
            total += os.path.getsize(os.path.join(r, f))
    return total / (1024 * 1024)


def show_tree(path, max_items=12):
    """Print the first folders/files so students can compare with File Explorer / Finder."""
    path = Path(path)
    shown = 0
    for entry in sorted(path.iterdir()):
        if entry.name.startswith(".") or entry.name.endswith(".crc"):
            continue
        kind = "📁" if entry.is_dir() else "📄"
        print(f"   {kind} {entry.name}")
        shown += 1
        if shown >= max_items:
            print("   ...")
            break


def finish(spark, extra=""):
    print()
    line("=")
    if extra:
        print(extra)
    print("🛑 The Spark UI disappears when Spark stops. Take your last look at it now.")
    pause("Press ENTER to stop Spark")
    spark.stop()
    print("👋 Spark stopped. Files in demo_output/ are kept so you can explore them.")


# ------------------------------------------------------------------- plan helpers
def plan_text(df, mode="formatted"):
    """Return explain() output as text, so a demo can show only the interesting lines."""
    import io
    from contextlib import redirect_stdout

    buf = io.StringIO()
    with redirect_stdout(buf):
        df.explain(mode=mode)
    return buf.getvalue()


def show_plan_lines(df, *keywords, mode="simple"):
    """Print only the explain() lines that contain one of the keywords."""
    hits = [ln.strip() for ln in plan_text(df, mode).splitlines() if any(k in ln for k in keywords)]
    if not hits:
        print("   (no matching lines in the plan)")
    for h in hits[:12]:
        print("   ➜", h[:200])
    return hits


def noop(df):
    """Run the whole query but write nothing: the fairest way to time a transformation."""
    df.write.format("noop").mode("overwrite").save()


# ------------------------------------------------------------ task times from the Spark UI API
def last_stage_id(spark):
    """Highest stage id so far. Call before a query, then pass it to task_report() after."""
    import json
    import urllib.request

    try:
        app = spark.sparkContext.applicationId
        base = spark.sparkContext.uiWebUrl
        with urllib.request.urlopen(f"{base}/api/v1/applications/{app}/stages?details=false", timeout=5) as r:
            stages = json.load(r)
        return max((s["stageId"] for s in stages), default=-1)
    except Exception:
        return -1


def task_report(spark, since_stage, label=""):
    """Print how long the tasks of the busiest stage took, straight from the Spark UI's REST API.
    Shows the straggler problem in numbers: slowest task vs the median task."""
    import json
    import statistics
    import urllib.request

    time.sleep(1.0)  # let Spark's UI listener finish recording the last tasks
    try:
        app = spark.sparkContext.applicationId
        base = spark.sparkContext.uiWebUrl
        with urllib.request.urlopen(f"{base}/api/v1/applications/{app}/stages?details=false", timeout=5) as r:
            stages = [s for s in json.load(r) if s["stageId"] > since_stage and s["numTasks"] >= 4 and s["status"] == "COMPLETE"]
        best = None
        for st in stages:
            url = f"{base}/api/v1/applications/{app}/stages/{st['stageId']}/{st['attemptId']}/taskList?length=1000"
            with urllib.request.urlopen(url, timeout=5) as r:
                durs = sorted((t.get("duration", 0) for t in json.load(r)), reverse=True)
            if durs and (best is None or durs[0] > best[0][0]):
                best = (durs, st["stageId"])
        if not best:
            print("   (no multi-task stage found; look at the Stages tab instead)")
            return None
        durs, sid = best
        med = statistics.median(durs)
        ratio = durs[0] / max(med, 1)
        print(f"   {label} busiest stage {sid}: {len(durs)} tasks | slowest {durs[0]} ms | median {int(med)} ms | slowest = {ratio:.1f}x the median")
        return durs[0], ratio
    except Exception as e:
        print(f"   (could not read the Spark UI API: {type(e).__name__}; open the Stages tab instead)")
        return None



def parquet_mb(path, columns=None):
    """Megabytes of (compressed) column data inside the Parquet files under 'path', read from the file footers.
    columns=None means every column. This is the number of bytes Spark MUST read for those columns."""
    import pyarrow.parquet as pq

    total = 0
    for root, _, files in os.walk(path):
        for f in files:
            if not f.endswith(".parquet"):
                continue
            md = pq.ParquetFile(os.path.join(root, f)).metadata
            for g in range(md.num_row_groups):
                for c in range(md.num_columns):
                    col = md.row_group(g).column(c)
                    if columns is None or col.path_in_schema in columns:
                        total += col.total_compressed_size
    return total / (1024 * 1024)
