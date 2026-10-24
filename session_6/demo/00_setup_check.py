"""
DataKern | Spark session demos | 00  SETUP CHECK  (run this first, takes 1-2 minutes)

It checks, one by one: Python, Java, PySpark, PyArrow, delta-spark, your Olist files,
and that Spark can really WRITE and READ Parquet and Delta on your machine.
If a check fails it tells you what to fix. Fix it BEFORE the demos.

Run:   python 00_setup_check.py
"""
import os
import platform
import re
import shutil
import subprocess
import sys

import demo_utils as u

problems = []


def check(label, passed, fix=""):
    if passed:
        u.ok(label)
    else:
        u.warn(f"{label}")
        if fix:
            print(f"      FIX: {fix}")
        problems.append(label)


print("🔧 DataKern Spark demos: setup check")
print(f"   Operating system: {platform.system()} {platform.release()}\n")

# 1. Python
check(f"Python {platform.python_version()} (need 3.10 or newer)", sys.version_info >= (3, 10), "Install Python 3.10+ from python.org")

# 2. Java (Spark runs on the JVM)
java = shutil.which("java")
java_ok = False
if java:
    out = subprocess.run([java, "-version"], capture_output=True, text=True)
    text = (out.stderr or "") + (out.stdout or "")
    # Search the whole text: some machines print extra lines (like JAVA_TOOL_OPTIONS) before the version line.
    m = re.search(r'version "(\d+)(?:\.(\d+))?', text)
    major = 0
    if m:
        major = int(m.group(2) or 0) if m.group(1) == "1" else int(m.group(1))
    first = next((ln for ln in text.splitlines() if "version" in ln), text.strip()[:80])
    java_ok = major >= 17
    check(f"Java found: {first.strip()} (need 17 or newer)", java_ok, "Install a JDK 17 or 21 (for example Temurin from adoptium.net), then reopen your terminal")
else:
    check("Java found on PATH", False, "Install a JDK 17 or 21 (for example Temurin from adoptium.net), then reopen your terminal")

# 3. Windows needs Hadoop helper files to write local files
if platform.system() == "Windows":
    hh = os.environ.get("HADOOP_HOME", "")
    has_winutils = bool(hh) and os.path.exists(os.path.join(hh, "bin", "winutils.exe"))
    check(
        "Windows: HADOOP_HOME points to a folder with bin\\winutils.exe",
        has_winutils,
        "Your instructor will share the winutils folder. Set HADOOP_HOME to it and add %HADOOP_HOME%\\bin to PATH.",
    )

# 4. Packages
try:
    import pyspark

    check(f"PySpark {pyspark.__version__}", True)
except ImportError:
    check("PySpark installed", False, "pip install -r requirements.txt")
    sys.exit("Install the requirements first, then run this check again.")

try:
    import pyarrow

    check(f"PyArrow {pyarrow.__version__} (needed for pandas UDFs and reading Parquet footers)", True)
except ImportError:
    check("PyArrow installed", False, "pip install pyarrow")

delta_ok = False
try:
    from importlib.metadata import version

    dv = version("delta-spark")
    check(f"delta-spark {dv}", True)
    delta_ok = True
except Exception:
    check("delta-spark installed", False, "pip install -r requirements.txt")

# 5. Olist files
needed = [
    "olist_orders_dataset.csv",
    "olist_order_items_dataset.csv",
    "olist_customers_dataset.csv",
    "olist_products_dataset.csv",
    "product_category_name_translation.csv",
    "olist_sellers_dataset.csv",
]
missing = [n for n in needed if not (u.DATA_DIR / n).exists()]
check(f"Olist files found in {u.DATA_DIR}", not missing, f"Missing: {', '.join(missing)}. Copy them there or set OLIST_DIR.")

if problems:
    print(f"\n🛑 {len(problems)} problem(s) above. Fix them and rerun this script.")
    sys.exit(1)

# 6. Spark can really run, write Parquet, write Delta
print("\n⚡ Starting Spark (the first start can take 20-40 seconds)...")
test_dir = u.fresh_dir("setup_check")
spark = u.get_spark("setup check", delta=False)
df = spark.range(1000).withColumnRenamed("id", "n")
df.write.mode("overwrite").parquet(u.uri(test_dir / "pq"))
check("Spark wrote and read Parquet", spark.read.parquet(u.uri(test_dir / "pq")).count() == 1000)
check("Spark UI is reachable at " + str(spark.sparkContext.uiWebUrl), True)
orders = spark.read.csv(str(u.DATA_DIR / "olist_orders_dataset.csv"), header=True)
check(f"Olist orders readable ({orders.count():,} rows)", True)
spark.stop()

if delta_ok:
    print("\n🔺 Testing Delta (the FIRST run downloads the Delta jars from the internet, 1-2 minutes)...")
    try:
        spark = u.get_spark("setup check delta", delta=True)
        spark.range(1000).write.format("delta").mode("overwrite").save(u.uri(test_dir / "delta"))
        n = spark.read.format("delta").load(u.uri(test_dir / "delta")).count()
        log_ok = (test_dir / "delta" / "_delta_log").exists()
        check("Spark wrote and read a Delta table (with a _delta_log folder)", n == 1000 and log_ok)
        spark.stop()
    except Exception as e:
        check("Delta works", False, f"{type(e).__name__}: {str(e)[:300]}\n      Most common causes: no internet on the first run, or delta-spark and pyspark versions do not match (see requirements.txt).")

print()
u.line()
if problems:
    print(f"🛑 {len(problems)} problem(s). Fix them, rerun this script, then start with 01_windows_and_udf.py")
    sys.exit(1)
print("🎉 All checks passed. Next: python 01_windows_and_udf.py")
