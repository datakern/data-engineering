"""
DataKern — Session 4 Demo
Script 03: The Finance Pipeline — Spark SQL API

What you will learn:
  1. How to register DataFrames as temporary views
  2. How to write standard SQL that executes on the Spark engine
  3. That the underlying execution plan is identical to Script 02

The DataFrame API (Script 02) and Spark SQL (Script 03) are fully interoperable.
Data Engineers frequently mix them: SQL for complex joins and aggregations,
DataFrame API for dynamic filtering or ML pipelines.

Run from the demo/ folder:
  python scripts/03_spark_sql_pipeline.py
"""

import os
import time
from pyspark.sql import SparkSession

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")


def pause():
    input("\n🔍 Press ENTER to continue ➔ ")
    print("\n" + "─" * 70)


# ─────────────────────────────────────────────────────────────────
# SECTION 1 — SparkSession and reading data
# ─────────────────────────────────────────────────────────────────

print("\n" + "=" * 70)
print("SECTION 1: Setup and reading data")
print("=" * 70)

spark = SparkSession.builder \
    .appName("DataKern_Session4_SparkSQL") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print("✅ SparkSession ready.")
print("   OBSERVE: Open your Spark UI at http://localhost:4040")

df_orders   = spark.read.csv(os.path.join(DATA_DIR, "olist_orders_dataset.csv"),      header=True, inferSchema=True)
df_items    = spark.read.csv(os.path.join(DATA_DIR, "olist_order_items_dataset.csv"), header=True, inferSchema=True)
df_products = spark.read.csv(os.path.join(DATA_DIR, "olist_products_dataset.csv"),    header=True, inferSchema=True)

print("✅ Source CSVs read into DataFrames (lazy).")

pause()

# ─────────────────────────────────────────────────────────────────
# SECTION 2 — Register Temporary Views
# ─────────────────────────────────────────────────────────────────

print("SECTION 2: Register Temporary Views\n")

print("To query a DataFrame with SQL, you must give it a name that SQL recognizes.")
print("createOrReplaceTempView() links a string name to the DataFrame.")

input("\n🔍 Press ENTER to register the views ➔ ")

df_orders.createOrReplaceTempView("orders")
df_items.createOrReplaceTempView("order_items")
df_products.createOrReplaceTempView("products")

print("\n✅ Views registered: 'orders', 'order_items', 'products'")
print("   These views only exist for the lifetime of this script.")

pause()

# ─────────────────────────────────────────────────────────────────
# SECTION 3 — Write the SQL query
# ─────────────────────────────────────────────────────────────────

print("SECTION 3: The SQL Query\n")

print("We are going to pass a massive SQL string to spark.sql().")
print("Notice how this is EXACTLY the SQL you would write in PostgreSQL.")

sql_query = """
    SELECT
        DATE_TRUNC('month', o.order_purchase_timestamp) AS order_month,
        p.product_category_name,
        SUM(oi.price) AS item_sales_value,
        COUNT(DISTINCT oi.order_id) AS distinct_orders
    FROM order_items oi
    JOIN orders o   ON oi.order_id = o.order_id
    JOIN products p ON oi.product_id = p.product_id
    WHERE o.order_status = 'delivered'
    GROUP BY
        DATE_TRUNC('month', o.order_purchase_timestamp),
        p.product_category_name
    ORDER BY
        order_month,
        item_sales_value DESC
"""

print(sql_query)

print("PREDICT: When we call df_finance = spark.sql(sql_query), will it execute immediately?")

input("\n🔍 Press ENTER to run spark.sql() ➔ ")

# spark.sql() parses the string and returns a DataFrame.
# It is a TRANSFORMATION. It is LAZY.
df_finance = spark.sql(sql_query)

print("\n✅ Query parsed and planned.")
print(f"   Type returned: {type(df_finance)}")
print("   It returned a DataFrame! Spark SQL is just another way to build a plan.")

pause()

# ─────────────────────────────────────────────────────────────────
# SECTION 4 — ACTION: Execute the SQL
# ─────────────────────────────────────────────────────────────────

print("SECTION 4: ACTION — Execute the SQL with .show()\n")

print("Because df_finance is just a DataFrame, we trigger it the exact same way.")

input("\n🔍 Press ENTER to trigger the ACTION ➔ ")

t0 = time.time()
df_finance.show(10, truncate=False)
elapsed = time.time() - t0

print(f"\n⏱️  SQL pipeline executed in {elapsed:.2f} seconds")
print()
print("   OBSERVE: Look at the Spark UI. The Jobs, Stages, and Tasks are")
print("   virtually identical to Script 02. The Catalyst Optimizer takes")
print("   both DataFrame code and SQL code and compiles them into the")
print("   exact same physical execution plan.")

print("\n" + "=" * 70)
print("✅ Script 03 complete.")
print("=" * 70)
print()
print("   Key takeaway:")
print("     Spark SQL and DataFrame API are just two different dialects")
print("     for expressing the same logic.")
print("     Mix and match them based on what is easiest to read and write.")
print()
print("   Next → run python scripts/04_parquet_and_validation.py")

print("\n" + "─" * 70)
print("🛑 Before you leave: The Spark UI is still running at http://localhost:4040.")
print("Take a final look around. When you press ENTER below, the script will call")
print("spark.stop(). The JVM will shut down, and the UI will disappear immediately.")
input("\n🔍 Press ENTER to stop the SparkSession and exit ➔ ")

spark.stop()
print("✅ SparkSession stopped. See you in the next script!")
