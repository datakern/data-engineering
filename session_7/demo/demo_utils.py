"""
DataKern | Session 7 | Spark, level up (Part 2) | shared helpers
-----------------------------------------------------------------
The five demos import this file, so the demo scripts stay short and show only the lesson.
It gives you:
  pause() / predict() / observe()   "Observe Mode": stop, guess, then look
  get_spark()                       one place that builds the SparkSession (with Delta)
  build_orders()                    the Olist ORDERS table, one row per order
  write_bronze() / write_silver()   the two layers the demos build on (each demo rebuilds them if missing)
  count_files(), folder_size_mb(), show_tree(), scan_report() ... small helpers to look at files and scans

Run any demo from this folder:            python 01_partitioning_and_pruning.py
Skip all pauses (quick test run only):    DEMO_AUTO=1 python 01_partitioning_and_pruning.py
Windows PowerShell:                       $env:DEMO_AUTO=1; python 01_partitioning_and_pruning.py
"""
import os
import shutil
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
AUTO = os.environ.get("DEMO_AUTO") == "1"

# Where the Olist CSV files live. Set the OLIST_DIR environment variable, or put them in ./data/olist
DATA_DIR = Path(os.environ.get("OLIST_DIR", HERE / "data" / "olist"))
# Everything the demos write goes here, so you can open the folders and look. Safe to delete.
OUT_DIR = HERE / "demo_output"
LAKE = OUT_DIR / "lakehouse"          # the thing we grow from script to script
BRONZE = LAKE / "bronze_orders"       # built in demo 02: partitioned Parquet, tidy files
SILVER = LAKE / "silver_orders"       # built in demo 03: Delta table
CUTOFF = "2018-08-01"                 # "today" in our story: silver holds orders purchased BEFORE this date

# PySpark must start the SAME Python you are running now (matters on Windows and with venvs).
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
# Some laptops cannot resolve their own hostname, and Spark then fails to open its driver port.
os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")


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
    """Tell the student WHERE to look, WHAT to look for, and WHY it matters."""
    print(f"\n👀 OBSERVE ➜ {where}")
    if look:
        print(f"   LOOK FOR : {look}")
    if meaning:
        print(f"   MEANS    : {meaning}")


def sample(title, df, n=5, truncate=22, vertical=False):
    """INPUT: show a few rows of the table a query works on, BEFORE the query."""
    print(f"\n📥 INPUT DATA: {title}")
    df.show(n, truncate=truncate, vertical=vertical)


def result(title, df, n=8, truncate=22, vertical=False):
    """OUTPUT: show what the query produced."""
    print(f"\n📤 OUTPUT: {title}")
    df.show(n, truncate=truncate, vertical=vertical)


def why(text):
    print(f"💡 WHY: {text}")


def ok(text):
    print(f"✅ {text}")


def warn(text):
    print(f"🛑 {text}")


def say(text):
    """A plain sentence the instructor says out loud, shown on screen too."""
    print(f"🗣️  {text}")


def reference(code):
    """The clean reference block at the end of a demo: the same code, no pauses, no prints."""
    print()
    line("=")
    print("📋 CLEAN REFERENCE: the whole demo as plain code, ready to copy")
    line("=")
    print(code.strip("\n"))
    line("=")


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
def get_spark(app="DataKern session 7", shuffle_partitions=8, extra_conf=None):
    """Build a local SparkSession with Delta Lake. Needs internet the FIRST time (downloads the Delta jars)."""
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.master("local[*]")
        .appName(app)
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.driver.memory", os.environ.get("DEMO_DRIVER_MEM", "2g"))
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    for k, v in (extra_conf or {}).items():
        builder = builder.config(k, str(v))
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    print(f"⚡ Spark {spark.version} is running with {spark.sparkContext.defaultParallelism} cores (Delta Lake is on)")
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


def build_orders(spark):
    """The ORDERS table: one row per order (about 99k rows).
    Joins orders + customers + order items, and adds year_month (the partition column we use all session)."""
    from pyspark.sql import functions as F

    def rd(name, ddl):
        return spark.read.csv(_need(name), header=True, schema=ddl)

    orders = rd(
        "olist_orders_dataset.csv",
        "order_id STRING, customer_id STRING, order_status STRING, order_purchase_timestamp TIMESTAMP, "
        "order_approved_at TIMESTAMP, order_delivered_carrier_date TIMESTAMP, "
        "order_delivered_customer_date TIMESTAMP, order_estimated_delivery_date TIMESTAMP",
    ).select("order_id", "customer_id", "order_status", F.col("order_purchase_timestamp").alias("purchase_ts"))
    customers = rd(
        "olist_customers_dataset.csv",
        "customer_id STRING, customer_unique_id STRING, customer_zip_code_prefix STRING, "
        "customer_city STRING, customer_state STRING",
    ).select("customer_id", "customer_unique_id", F.col("customer_state").alias("state"))
    items = rd(
        "olist_order_items_dataset.csv",
        "order_id STRING, order_item_id INT, product_id STRING, seller_id STRING, "
        "shipping_limit_date TIMESTAMP, price DOUBLE, freight_value DOUBLE",
    )
    per_order = items.groupBy("order_id").agg(
        F.count("*").alias("n_items"),
        F.round(F.sum("price"), 2).alias("order_value"),
        F.round(F.sum("freight_value"), 2).alias("freight_value"),
    )
    return (
        orders.join(customers, "customer_id")
        .join(per_order, "order_id", "left")
        .withColumn("n_items", F.coalesce("n_items", F.lit(0)))
        .withColumn("order_value", F.coalesce("order_value", F.lit(0.0)))
        .withColumn("freight_value", F.coalesce("freight_value", F.lit(0.0)))
        .withColumn("year_month", F.date_format("purchase_ts", "yyyy-MM"))
        .select(
            "order_id", "customer_unique_id", "state", "purchase_ts", "year_month",
            "order_status", "n_items", "order_value", "freight_value",
        )
    )


def write_bronze(spark):
    """Demo 02 builds this step by step. Other demos call this if the folder is missing."""
    if BRONZE.exists():
        return
    print("   (the bronze layer from demo 02 is missing, so we build it now)")
    BRONZE.parent.mkdir(parents=True, exist_ok=True)
    build_orders(spark).repartition("year_month").write.partitionBy("year_month").parquet(uri(BRONZE))


def write_silver(spark):
    """Demo 03 builds this step by step. Other demos call this if the table is missing."""
    if (SILVER / "_delta_log").exists():
        return
    from pyspark.sql import functions as F

    write_bronze(spark)
    print("   (the silver Delta table from demo 03 is missing, so we build it now)")
    (
        spark.read.parquet(uri(BRONZE))
        .where(F.col("purchase_ts") < CUTOFF)
        .write.format("delta").mode("overwrite").save(uri(SILVER))
    )


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


def data_files(path):
    """The data files of a Parquet or Delta folder (skips _delta_log and Spark's marker files)."""
    out = []
    for r, dirs, fs in os.walk(path):
        dirs[:] = [d for d in dirs if d != "_delta_log"]
        out += [os.path.join(r, f) for f in fs if f.endswith(".parquet")]
    return out


def count_files(path):
    return len(data_files(path))


def folder_size_mb(path):
    return sum(os.path.getsize(f) for f in data_files(path)) / (1024 * 1024)


def show_tree(path, max_items=10, indent="   "):
    """Print the first folders/files so students can compare with File Explorer / Finder."""
    path = Path(path)
    shown = 0
    entries = [e for e in sorted(path.iterdir()) if not e.name.startswith(".") and not e.name.endswith(".crc") and e.name != "_SUCCESS"]
    for entry in entries:
        kind = "📁" if entry.is_dir() else "📄"
        print(f"{indent}{kind} {entry.name}")
        shown += 1
        if shown >= max_items:
            print(f"{indent}... ({len(entries) - max_items} more)")
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


# ------------------------------------------------------------------- plan and scan helpers
def plan_text(df, mode="formatted"):
    """Return explain() output as text, so a demo can show only the interesting lines."""
    import io
    from contextlib import redirect_stdout

    buf = io.StringIO()
    with redirect_stdout(buf):
        df.explain(mode=mode)
    return buf.getvalue()


def show_plan_lines(df, *keywords, mode="formatted"):
    """Print only the explain() lines that contain one of the keywords."""
    hits = [ln.strip() for ln in plan_text(df, mode).splitlines() if any(k in ln for k in keywords)]
    if not hits:
        print("   (no matching lines in the plan)")
    for h in hits[:12]:
        print("   ➜", h[:210])
    return hits


def plan_top(df, n=1):
    """The first n operators at the TOP of the physical plan (the last thing Spark does), e.g. 'Coalesce 3'."""
    lines = [ln.strip(" +-:") for ln in plan_text(df, "simple").splitlines()]
    ops = [ln for ln in lines if ln and not ln.startswith("==") and not ln.startswith("AdaptiveSparkPlan")]
    return ops[:n]


def noop(df):
    """Run the whole query but write nothing: the fairest way to time a transformation."""
    df.write.format("noop").mode("overwrite").save()


def _sql_executions(spark, details=False):
    """Every SQL execution the Spark UI knows about. The API returns the OLDEST ones first, so we page through all of them."""
    out, offset, page = [], 0, 300
    while True:
        chunk = _ui_get(spark, f"sql?details={str(details).lower()}&planDescription=false&offset={offset}&length={page}")
        out += chunk
        if len(chunk) < page:
            return out
        offset += page


def scan_report(spark, df, label=""):
    """Run the query, then read what the SCAN node really did, from the same numbers as the Spark UI's SQL tab:
    files opened, bytes read, partitions read. This is the number behind 'pruning opens only 4 of 24 files'."""
    try:
        before = max((e["id"] for e in _sql_executions(spark)), default=-1)
        noop(df)
        for _ in range(20):
            time.sleep(0.25)
            runs = [e for e in _sql_executions(spark, details=True) if e["id"] > before]
            if runs and all(e["status"] == "COMPLETED" for e in runs):
                break
        scans = {}
        for run in runs:  # Delta runs extra tiny queries on its own log: keep the scan node that has file numbers
            for node in run["nodes"]:
                found = {m["name"]: m["value"] for m in node["metrics"]}
                if node["nodeName"].startswith("Scan") and "number of files read" in found:
                    scans = found
        files = int(scans["number of files read"].replace(",", ""))
        size = scans.get("size of files read", "n/a")
        parts = scans.get("number of partitions read")
        rows = int(scans.get("number of output rows", "0").replace(",", ""))
        extra = f", {parts} partition folder(s)" if parts else ""
        print(f"   {label}: the scan opened {files} file(s){extra}, read {size}, handed {rows:,} rows to Spark")
        return {"files": files, "size": size, "partitions": parts, "rows": rows}
    except Exception as e:
        print(f"   {label}: (could not read the scan numbers: {type(e).__name__}; open the SQL tab in the Spark UI)")
        return None


# ------------------------------------------------------------ task counts from the Spark UI API
def _ui_get(spark, path):
    import json
    import urllib.request

    app = spark.sparkContext.applicationId
    base = spark.sparkContext.uiWebUrl
    with urllib.request.urlopen(f"{base}/api/v1/applications/{app}/{path}", timeout=5) as r:
        return json.load(r)


def last_stage_tasks(spark):
    """How many tasks the most recent stage had. After a write, that is the number of tasks that wrote the files."""
    time.sleep(1.0)
    try:
        stages = _ui_get(spark, "stages?details=false")
        done = [s for s in stages if s["status"] == "COMPLETE"]
        last = max(done, key=lambda s: s["stageId"])
        return last["numTasks"]
    except Exception:
        return None
