"""
DataKern | Assignment 5 | Olist seller scorecard: windows and Parquet

YOU write the bodies marked TODO. Everything else (schemas, driver) is given.
Run the whole thing:   python pipeline.py
Check yourself:        python check_my_work.py
"""
from pathlib import Path

from pyspark.sql import functions as F  # noqa: F401  (you will need it)
from pyspark.sql.window import Window  # noqa: F401

from helpers import OUT_DIR, count_files, csv_path, folder_size_mb, fresh_dir, get_spark, plan_text, timed, uri

# Explicit schemas (given). A CSV has no types, and column ORDER matters: Spark matches by position.
ORDERS_DDL = (
    "order_id STRING, customer_id STRING, order_status STRING, order_purchase_timestamp TIMESTAMP, "
    "order_approved_at TIMESTAMP, order_delivered_carrier_date TIMESTAMP, "
    "order_delivered_customer_date TIMESTAMP, order_estimated_delivery_date TIMESTAMP"
)
ITEMS_DDL = (
    "order_id STRING, order_item_id INT, product_id STRING, seller_id STRING, "
    "shipping_limit_date TIMESTAMP, price DOUBLE, freight_value DOUBLE"
)
CUSTOMERS_DDL = (
    "customer_id STRING, customer_unique_id STRING, customer_zip_code_prefix STRING, "
    "customer_city STRING, customer_state STRING"
)
PRODUCTS_DDL = (
    "product_id STRING, product_category_name STRING, product_name_lenght INT, "
    "product_description_lenght INT, product_photos_qty INT, product_weight_g INT, "
    "product_length_cm INT, product_height_cm INT, product_width_cm INT"
)
TRANSLATION_DDL = "product_category_name STRING, product_category_name_english STRING"

# The columns of the "sales" table you build in Part 1 (one row per order item).
SALES_COLUMNS = [
    "order_id", "order_item_id", "customer_unique_id", "purchase_ts", "seller_id",
    "product_id", "category", "price", "freight_value", "state",
]


# ============================================================================ PART 1
def build_sales(spark):
    """Read the five CSV files and return ONE wide DataFrame with exactly SALES_COLUMNS.

    - read with the explicit schemas above (header=True), using csv_path("<file name>")
    - files: olist_orders_dataset.csv, olist_order_items_dataset.csv, olist_customers_dataset.csv,
             olist_products_dataset.csv, product_category_name_translation.csv
    - one output row per order item: do not lose items, do not duplicate them
    - purchase_ts = order_purchase_timestamp, state = customer_state
    - category = the ENGLISH category name, or the text 'unknown' when there is none
    """
    # TODO
    raise NotImplementedError("Part 1: build_sales")


# ============================================================================ PART 2
def write_silver(sales, silver_path):
    """Write `sales` as Parquet, one folder per month, ONE tidy file per folder.

    - add a column year_month like '2018-03' (text) derived from purchase_ts
    - folders on disk: year_month=2018-03/ ...
    - think about what the in-memory partitioning must look like BEFORE the write
    - re-running must replace the old output
    - use uri(silver_path) when you give the path to Spark
    """
    # TODO
    raise NotImplementedError("Part 2a: write_silver")


def compare_csv_parquet(sales, workdir):
    """Write `sales` once as CSV (with header) and once as plain Parquet (no partitioning)
    under workdir/sales_csv and workdir/sales_parquet. Return a dict:
        {"csv_mb": float, "parquet_mb": float, "csv_files": int, "parquet_files": int}
    Measure on DISK with folder_size_mb() and count_files(folder, suffix).
    """
    # TODO
    raise NotImplementedError("Part 2b: compare_csv_parquet")


# ============================================================================ PART 3
def latest_order_per_customer(sales):
    """One row per customer_unique_id: their MOST RECENT order.
    Columns: customer_unique_id, order_id, purchase_ts.
    Ties on purchase_ts are broken by order_id, larger first (the answer must be repeatable).
    """
    # TODO
    raise NotImplementedError("Part 3a: latest_order_per_customer")


def top3_products_per_category(sales):
    """The 3 best-selling products (by revenue) inside each category.
    Columns: category, product_id, revenue, rank (1, 2, 3).
    revenue = round(sum(price), 2). Ties on revenue are broken by product_id, A to Z.
    A category with fewer than 3 products simply returns fewer rows.
    """
    # TODO
    raise NotImplementedError("Part 3b: top3_products_per_category")


def seller_monthly(sales):
    """Per seller and per month (year_month text like '2018-03'):
        month_revenue       round(sum(price), 2)
        prev_month_revenue  month_revenue of the seller's PREVIOUS ACTIVE month (null for the first)
        mom_growth_pct      round((month - prev) / prev * 100, 1), null for the first month
        running_revenue     round(cumulative month_revenue up to and including this month, 2)
    Columns: seller_id, year_month, month_revenue, prev_month_revenue, mom_growth_pct, running_revenue
    """
    # TODO
    raise NotImplementedError("Part 3c: seller_monthly")


# ============================================================================ PART 4
def read_month(spark, silver_path, year_month):
    """Read ONLY one month from the silver Parquet folder, e.g. year_month='2018-03'.
    Return a DataFrame. The physical plan must show that Spark skips the other folders
    (look for PartitionFilters in plan_text(df)). Do not call an action here.
    """
    # TODO
    raise NotImplementedError("Part 4: read_month")


# ============================================================================ PART 5
def add_price_band_builtin(df):
    """Add column price_band: 'budget' if price < 20, 'mid' if price < 100, else 'premium'.
    Use ONLY built-in Spark functions (no Python function per row)."""
    # TODO
    raise NotImplementedError("Part 5a: add_price_band_builtin")


def add_price_band_udf(df):
    """Same result as above, but written as a Python UDF. (Yes, on purpose: you will measure the cost.)"""
    # TODO
    raise NotImplementedError("Part 5b: add_price_band_udf")


# ============================================================================ PART 6
def write_gold(tables, gold_path):
    """`tables` is a dict {name: DataFrame}. Write each one as Parquet to gold_path/<name>/.
    These tables are small and will be read whole: each folder must hold exactly ONE parquet file
    and no partition sub-folders. Re-running must replace the old output.
    """
    # TODO
    raise NotImplementedError("Part 6: write_gold")


# ============================================================================ driver (given)
def main():
    spark = get_spark()

    sales = build_sales(spark)
    print(f"[1] sales rows: {sales.count():,}")

    silver = OUT_DIR / "silver" / "sales"
    write_silver(sales, silver)
    print(f"[2] silver: {count_files(silver)} parquet files, {folder_size_mb(silver):.1f} MB")
    sizes = compare_csv_parquet(sales, OUT_DIR / "compare")
    print(f"    CSV {sizes['csv_mb']:.1f} MB vs Parquet {sizes['parquet_mb']:.1f} MB")

    silver_df = spark.read.parquet(uri(silver))
    tables = {
        "latest_order": latest_order_per_customer(silver_df),
        "top3_products": top3_products_per_category(silver_df),
        "seller_monthly": seller_monthly(silver_df),
    }
    for name, df in tables.items():
        print(f"[3] {name}: {df.count():,} rows")

    march = read_month(spark, silver, "2018-03")
    print(f"[4] 2018-03 rows: {march.count():,}")

    _, t_builtin = timed(lambda: add_price_band_builtin(silver_df).groupBy("price_band").count().collect())
    _, t_udf = timed(lambda: add_price_band_udf(silver_df).groupBy("price_band").count().collect())
    print(f"[5] built-in {t_builtin:.2f}s | UDF {t_udf:.2f}s")

    gold = OUT_DIR / "gold"
    fresh_dir(gold)
    write_gold(tables, gold)
    print(f"[6] gold written: {count_files(gold)} parquet files")
    spark.stop()


if __name__ == "__main__":
    main()
