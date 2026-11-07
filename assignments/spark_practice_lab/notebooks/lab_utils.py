"""
DataKern | Spark Practice Lab | shared helpers
-----------------------------------------------
Every notebook imports this file, so the notebooks show only the Spark lesson.

  get_spark()     one place that builds the SparkSession (with Delta Lake)
  olist(name)     full path of an Olist CSV file (with a friendly error if it is missing)
  check()         a self-check: prints a green tick or a red cross
  root_cause()    shortens a long Spark error to the 'Caused by:' lines
  JobCounter      counts how many Spark jobs a block of code launched
  Timer           times a block of code

Where the Olist files are:  put them in  <lab folder>/data/olist/   or set the OLIST_DIR environment variable.
"""
import os
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
LAB_DIR = HERE.parent
DATA_DIR = Path(os.environ.get("OLIST_DIR", LAB_DIR / "data" / "olist"))
OUT_DIR = LAB_DIR / "lab_output"          # everything the lab writes goes here. Safe to delete.

# PySpark must start the SAME Python the notebook kernel is running (matters on Windows and with venvs).
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
# Some laptops cannot resolve their own hostname, and Spark then fails to open its driver port.
os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")


# ------------------------------------------------------------------ Spark setup
def get_spark(app="DataKern Spark Practice Lab", shuffle_partitions=8, extra_conf=None):
    """Build a local SparkSession with Delta Lake. Needs internet the FIRST time (downloads the Delta jars)."""
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.master("local[*]")
        .appName(app)
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.driver.memory", os.environ.get("LAB_DRIVER_MEM", "2g"))
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    for k, v in (extra_conf or {}).items():
        builder = builder.config(k, str(v))
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    # Spark's own log is silenced so that a failing read shows a short Python error, not 200 lines of Java.
    # Your code's errors are still raised as normal Python exceptions.
    spark.sparkContext.setLogLevel("OFF")
    print(f"⚡ Spark {spark.version} is running with {spark.sparkContext.defaultParallelism} cores (Delta Lake is on)")
    print(f"🌐 Spark UI ➔ {spark.sparkContext.uiWebUrl}   (open it in your browser and keep the tab open)")
    return spark


def olist(name):
    """Full path of one Olist file, or a clear message if it is not where we expect it."""
    p = DATA_DIR / name
    if not p.exists():
        raise FileNotFoundError(
            f"\n🛑 Cannot find {p}\n"
            f"   Put the Olist CSV files in: {DATA_DIR}\n"
            f"   or set the OLIST_DIR environment variable to the folder that holds them."
        )
    return str(p)


# ------------------------------------------------------------------ self-checks
_checks = {}


def check(label, ok, hint=""):
    """A self-check: ✅ if true, ❌ with a hint if not.
    Pass a lambda, e.g. check("3 rows", lambda: df.count() == 3). If your variable does not exist yet,
    or the code inside fails, you get a ❌ and the reason instead of a crash."""
    if callable(ok):
        try:
            ok = ok()
        except Exception as e:
            hint = f"could not evaluate this check ({type(e).__name__}: {str(e)[:90]}). {hint}"
            ok = False
    _checks[label] = bool(ok)
    print(("✅ " if ok else "❌ ") + label)
    if not ok and hint:
        print(f"   hint: {hint}")


def checks_summary(expected):
    """Final line of a notebook. `expected` = how many checks the notebook has when every cell has run."""
    passed = sum(_checks.values())
    if len(_checks) < expected:
        print(f"\n⚠️ Only {len(_checks)} of {expected} checks ran ({passed} passed). A cell above stopped with an error. "
              f"Fix it, then Restart and Run All.")
    elif passed == expected:
        print(f"\n🎉 {passed}/{expected} checks passed")
    else:
        print(f"\n🛠️ {passed}/{expected} checks passed. Look for the ❌ lines above.")


# --------------------------------------------------------------- measuring helpers
def root_cause(error):
    """Spark errors are long Java traces. The useful part is the 'Caused by:' lines. Show only those."""
    lines = [l.strip() for l in str(error).splitlines() if l.strip().startswith("Caused by:")]
    return "\n      ".join(l[:230] for l in lines) or str(error).splitlines()[0][:230]


class JobCounter:
    """with JobCounter(spark) as j:  ...   then  j.jobs  = Spark jobs launched inside the block."""

    def __init__(self, spark):
        self._tracker = spark.sparkContext.statusTracker()
        self.jobs = 0

    def _now(self):
        return len(self._tracker.getJobIdsForGroup(None))

    def __enter__(self):
        self._start = self._now()
        return self

    def __exit__(self, *exc):
        self.jobs = self._now() - self._start


class Timer:
    """with Timer() as t:  ...   then  t.seconds"""

    def __enter__(self):
        self._t0 = time.time()
        return self

    def __exit__(self, *exc):
        self.seconds = round(time.time() - self._t0, 2)
