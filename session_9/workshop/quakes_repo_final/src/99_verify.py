# Databricks notebook source
# MAGIC %md
# MAGIC # 99 - Verify the pipeline output
# MAGIC
# MAGIC Last task of the job. It checks the tables the pipeline just built. If a check fails the notebook
# MAGIC raises an error, the job run turns red, and nobody trusts the data by accident.
# MAGIC
# MAGIC Parameters: `catalog`, `schema` (same values as the download task).

# COMMAND ----------

dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")

CATALOG = dbutils.widgets.get("catalog").strip()
SCHEMA = dbutils.widgets.get("schema").strip()

if not CATALOG or not SCHEMA:
    raise ValueError("Set the 'catalog' and 'schema' parameters.")


def table(name):
    return spark.table(f"`{CATALOG}`.`{SCHEMA}`.`{name}`")


bronze = table("quakes_bronze")
silver = table("quakes_silver")
daily = table("quakes_daily")
by_region = table("quakes_by_region")

# COMMAND ----------

bronze_rows = bronze.count()
silver_rows = silver.count()
distinct_events = silver.select("event_id").distinct().count()
earthquakes_in_silver = silver.filter("event_type = 'earthquake'").count()
earthquakes_in_daily = daily.agg({"quakes": "sum"}).collect()[0][0]
earthquakes_in_regions = by_region.agg({"quakes": "sum"}).collect()[0][0]

print(f"bronze rows              : {bronze_rows}")
print(f"silver rows              : {silver_rows}   (bronze minus repeated versions and dropped rows)")
print(f"earthquakes in silver    : {earthquakes_in_silver}")
print(f"earthquakes in gold daily: {earthquakes_in_daily}")
print(f"earthquakes in gold regions: {earthquakes_in_regions}")

# COMMAND ----------

# Each check is one plain sentence. The first one that is false stops the job.
assert bronze_rows > 0, "Bronze is empty: did the download task save any files?"
assert silver_rows > 0, "Silver is empty: every row was dropped by a quality rule."
assert silver_rows <= bronze_rows, "Silver has more rows than Bronze."
assert silver_rows == distinct_events, "Silver has the same event more than once."
assert earthquakes_in_daily == earthquakes_in_silver, "Gold daily does not add up to Silver."
assert earthquakes_in_regions == earthquakes_in_silver, "Gold regions does not add up to Silver."

print("\nAll checks passed.")

# COMMAND ----------

# A look at the answer: the ten most active regions.
display(by_region.orderBy("quakes", ascending=False).limit(10))
