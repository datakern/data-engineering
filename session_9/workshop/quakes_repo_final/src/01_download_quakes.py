# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Download earthquakes from USGS into the landing volume
# MAGIC
# MAGIC Source: the USGS earthquake catalog (public, no key). One CSV file per day, saved in the volume.
# MAGIC
# MAGIC Two rules keep it sensible to run again and again:
# MAGIC
# MAGIC * **Old days are downloaded once.** A day that already has a file is skipped.
# MAGIC * **The last 2 days are downloaded every run.** USGS revises recent events (magnitude, status), so we
# MAGIC   save a fresh copy each time. The same event then appears in more than one file. The Silver step
# MAGIC   keeps only the newest version of each event.
# MAGIC
# MAGIC Parameters (job parameters, or widgets when you run it by hand):
# MAGIC
# MAGIC | Parameter | Example |
# MAGIC |---|---|
# MAGIC | `catalog` | the catalog you used in setup |
# MAGIC | `schema` | `<name>_dev` or `<name>_prod` |
# MAGIC | `days_back` | `30` |

# COMMAND ----------

import os
import time
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone

dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")
dbutils.widgets.text("days_back", "30")

CATALOG = dbutils.widgets.get("catalog").strip()
SCHEMA = dbutils.widgets.get("schema").strip()
DAYS_BACK = int(dbutils.widgets.get("days_back"))

if not CATALOG or not SCHEMA:
    raise ValueError("Set the 'catalog' and 'schema' parameters.")

LANDING_FOLDER = f"/Volumes/{CATALOG}/{SCHEMA}/landing/quakes"
USGS_URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"
ALWAYS_REFRESH_LAST_DAYS = 2

# The folder 'quakes' inside the volume is created here. The volume itself comes from setup/00_create_environments.sql.
os.makedirs(LANDING_FOLDER, exist_ok=True)
print("Landing folder:", LANDING_FOLDER)

# COMMAND ----------

# Which days do we want? Today and the DAYS_BACK - 1 days before it, in UTC (USGS works in UTC).
today = datetime.now(timezone.utc).date()
wanted_days = [today - timedelta(days=n) for n in range(DAYS_BACK)]


def day_already_downloaded(day):
    file_prefix = f"quakes_{day.isoformat()}_"
    return any(name.startswith(file_prefix) for name in os.listdir(LANDING_FOLDER))


def is_recent(day):
    return (today - day).days < ALWAYS_REFRESH_LAST_DAYS


days_to_download = [day for day in wanted_days if is_recent(day) or not day_already_downloaded(day)]
print(f"{len(wanted_days)} days wanted, {len(days_to_download)} to download, "
      f"{len(wanted_days) - len(days_to_download)} skipped (already there)")

# COMMAND ----------


def fetch_day_csv(day):
    """Ask USGS for every event between the start of `day` and the start of the next day (UTC)."""
    parameters = {
        "format": "csv",
        "starttime": day.isoformat(),
        "endtime": (day + timedelta(days=1)).isoformat(),
        "orderby": "time",
    }
    url = USGS_URL + "?" + urllib.parse.urlencode(parameters)

    # The internet is not perfectly reliable: try 3 times, waiting a bit longer each time.
    for attempt in range(1, 4):
        try:
            with urllib.request.urlopen(url, timeout=60) as response:
                return response.read()
        except Exception as error:
            print(f"  {day}: attempt {attempt} failed: {error}")
            if attempt == 3:
                raise
            time.sleep(5 * attempt)


# The same stamp goes into every file name of this run, so you can see which files belong together.
run_stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

total_events = 0
for day in sorted(days_to_download):
    csv_bytes = fetch_day_csv(day)
    events_in_file = max(csv_bytes.count(b"\n") - 1, 0)  # minus the header line

    file_path = f"{LANDING_FOLDER}/quakes_{day.isoformat()}_{run_stamp}.csv"
    with open(file_path, "wb") as output_file:
        output_file.write(csv_bytes)

    total_events += events_in_file
    print(f"{day}: {events_in_file:5d} events -> {os.path.basename(file_path)}")

print(f"\nDone. {len(days_to_download)} files, {total_events} events.")
