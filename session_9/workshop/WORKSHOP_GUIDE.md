# Session 9 Workshop: Build a pipeline, put it in a repo, deploy it with an asset bundle

In this workshop you will build a Databricks pipeline and a job on real earthquake data. You will keep every file in a Git repo, deploy to **dev**, then deploy the same files to **prod** straight from the repo, and finally ship one change through the whole path.

You do everything from your laptop with Git, VS Code and the Databricks CLI. You use the Databricks workspace only to look at what got created. You do **not** need a Git folder in Databricks.

**Time:** about 3 hours. Each part ends with a checkpoint. Do not move on until it passes.

```
 your laptop                         Azure DevOps repo                  Databricks workspace
 -----------                         -----------------                  --------------------
 write files  --git push-->          main branch   --git clone-->       prod  (bundle deploy -t prod)
 bundle deploy -t dev  ---------------------------------------------->  dev
```

> Naming note: Databricks has renamed "Asset Bundles" to **Declarative Automation Bundles** in its documentation. The commands are the same: `databricks bundle ...`. This guide says "bundle".

## The data in one minute

**USGS** is the United States Geological Survey, a US government science agency. It records earthquakes worldwide and publishes the list for free through a web address. No account, no key.

Our job asks that address for one day of events at a time. The reply is a CSV (plain text, one line per event, about 300 lines a day). You will see it yourself in Part 1.

Two facts about this data matter for the pipeline:

* **Events get revised.** An earthquake first appears as `automatic`. Hours or days later an analyst reviews it and the magnitude can change. The same event `id` then exists in more than one version. Silver keeps only the newest.
* **Not everything is an earthquake.** The list also holds quarry blasts, explosions, ice quakes. Silver keeps them. Gold counts only earthquakes.

## Before you start

You need:

- [ ] A Databricks workspace with **Unity Catalog** and **serverless** compute, and permission to **create schemas** in one catalog.
- [ ] An **Azure DevOps** project where you can create a repo. (GitHub works the same way. The only differences are the repo screens and the pull request screen.)
- [ ] **Git** (`git --version`) and **VS Code**.
- [ ] The **Databricks CLI**, a **recent** version. Check with `databricks -v`.
  - Mac: `brew tap databricks/tap`, then `brew trust databricks/tap` (newer Homebrew asks for this), then `brew install databricks`.
  - Windows: in Command Prompt, `winget install Databricks.DatabricksCLI`, then close and reopen the window.
  - Pipeline settings in the bundle need a current CLI. If a later command says a field is unknown, update the CLI first.

Choose two values and write them down. You will type them several times.

| Value | Example | Rules |
|---|---|---|
| `catalog` | `workspace` | the Unity Catalog catalog where you can create schemas. See it in **Catalog** in the workspace |
| `name` | `uma` | 2 to 20 characters: lowercase letters, digits, underscore. Starts with a letter |

In the rest of this guide `<catalog>` and `<name>` mean your values.

---

## Part 0: Prepare (20 min)

### 0.1 Log in to your workspace from the terminal

```bash
databricks auth login --host https://<your-workspace-url>
```

The workspace URL looks like `https://adb-1234567890123456.7.azuredatabricks.net`. Copy it from your browser's address bar, up to `.net`. Accept the suggested profile name by pressing Enter. A browser window opens: sign in.

Check:

```bash
databricks current-user me
```

It prints your user. That is the login every `databricks bundle` command will use.

### 0.2 Create the repo

In Azure DevOps: your project > **Repos** > **Files**. Create a new repository named `quakes-pipeline` (the repo drop-down at the top has **New repository**). Tick **Add a README**.

On the repo's **Files** page click **Clone** (top right). Copy the HTTPS address. It looks like:

```
https://dev.azure.com/<organization>/<project>/_git/quakes-pipeline
```

If Git later asks for a password, click **Generate Git Credentials** on that same Clone window and use the username and password it shows. (On Windows, Git for Windows can sign you in through a browser window instead.)

### 0.3 Clone it to your laptop and open it

```bash
git clone https://dev.azure.com/<organization>/<project>/_git/quakes-pipeline
cd quakes-pipeline
code .
```

`code .` opens the folder in VS Code. The folder is nearly empty: just the README. Everything else you create.

### 0.4 Create the two environments

An environment, in this workshop, is a **schema** in Unity Catalog with a **volume** (a folder for files) inside. You need two: `<name>_dev` and `<name>_prod`.

In the workspace, open the **SQL Editor**, paste this with your own `<catalog>` and `<name>`, and **Run all**. Keep a copy of it in your repo too: save it as `setup/00_create_environments.sql`.

*File `setup/00_create_environments.sql`*

```sql
-- Run once, in the Databricks SQL editor, before the first deploy.
-- Replace <catalog> and <name> with your own values (lowercase letters, digits, underscore).
--
-- An environment, in this workshop, is a schema with a volume (a folder for files) inside it.
-- dev and prod are two schemas in the same catalog.

CREATE SCHEMA IF NOT EXISTS <catalog>.<name>_dev;
CREATE VOLUME IF NOT EXISTS <catalog>.<name>_dev.landing;

CREATE SCHEMA IF NOT EXISTS <catalog>.<name>_prod;
CREATE VOLUME IF NOT EXISTS <catalog>.<name>_prod.landing;
```

> **Checkpoint 0.** In **Catalog**, under your catalog, you see the schemas `<name>_dev` and `<name>_prod`, each with a volume named `landing`. In the terminal `databricks current-user me` works.

---

## Part 1: Look at the raw data first (10 min)

Before building anything, see what the source gives you. In the terminal, ask USGS for 3 events:

```bash
curl -s "https://earthquake.usgs.gov/fdsnws/event/1/query?format=csv&starttime=2026-11-01&endtime=2026-11-02&orderby=time&limit=3"
```

(The same address works in a browser. Change `limit=3` and see what happens.)

You get text like this, a header line and then one line per event (shortened here):

```
time,latitude,longitude,depth,mag,magType,...,id,updated,place,type,...,status
2026-11-01T23:58:24.351Z,65,-147.359,0,1.5,ml,...,aka2026vripyo,2026-11-04T22:48:27.941Z,"13 km ENE of Fox, Alaska",quarry blast,...,reviewed
```

| Column | Meaning |
|---|---|
| `time` | when it happened (UTC) |
| `latitude`, `longitude` | where |
| `depth` | how deep underground, km |
| `mag` | magnitude, the strength |
| `place` | a human description of the location |
| `type` | earthquake, quarry blast, explosion, ... |
| `id` | the unique id of the event |
| `updated` | when USGS last revised this record |
| `status` | `automatic` = detected by computer, `reviewed` = checked by an analyst |

> **See 1.** The source is a web address that returns CSV text. No login, no files to find. The job you build in Part 2 does exactly this request, once per day, and saves each reply as a file in the `landing` volume.

---

## Part 2: Build the pipeline and the job (60 min)

You will create the files below, in this order. Each one is a small piece of the picture:

| File | What it is |
|---|---|
| `src/01_download_quakes.py` | job task 1: calls USGS, saves CSV files into the landing volume |
| `src/pipeline/01_bronze.sql`, `02_silver.sql`, `03_gold.sql` | the pipeline: three tables |
| `src/99_verify.py` | job task 3: checks the tables |
| `resources/quakes.pipeline.yml` | the pipeline's settings, as code |
| `resources/quakes.job.yml` | the job: the three tasks in order |
| `databricks.yml` | the bundle: which environments exist and their values |

Create the folders `setup` (done), `src`, `src/pipeline` and `resources` in VS Code. Then create each file by pasting the contents below. Paste the whole block, including the first line.

### 2.1 Task 1: download

*File `src/01_download_quakes.py`*

```python
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
```

The first line `# Databricks notebook source` and the lines `# COMMAND ----------` mean something: they tell Databricks this Python file is a notebook, split into cells. Keep them.

### 2.2 The pipeline: three SQL files

A pipeline is a set of table definitions. You say what each table is. Databricks works out the order, runs it, and counts bad rows.

**Bronze** keeps the files exactly as they arrived. Every column is text on purpose: Bronze never rejects a row.

*File `src/pipeline/01_bronze.sql`*

```sql
-- BRONZE: the files exactly as they arrived.
--
-- Every column is read as text on purpose. Bronze never rejects a row because of a surprise
-- (a missing magnitude, a strange date). Silver is where we decide what is acceptable.
--
-- A STREAMING table only reads files it has not seen before, so each run adds just the new files.
-- ${quakes.landing_path} is a pipeline setting, so dev and prod can read from their own volume.

CREATE OR REFRESH STREAMING TABLE quakes_bronze
COMMENT 'USGS earthquake events, raw text, one row per event per downloaded file'
AS
SELECT
  *,
  _metadata.file_path AS source_file,
  current_timestamp() AS ingested_at
FROM STREAM read_files(
  '${quakes.landing_path}',
  format => 'csv',
  header => true,
  schema => 'time STRING, latitude STRING, longitude STRING, depth STRING, mag STRING, magType STRING, nst STRING, gap STRING, dmin STRING, rms STRING, net STRING, id STRING, updated STRING, place STRING, type STRING, horizontalError STRING, depthError STRING, magError STRING, magNst STRING, status STRING, locationSource STRING, magSource STRING'
);
```

**Silver** gives one clean row per event: real types, bad rows dropped, only the newest version of each event.

*File `src/pipeline/02_silver.sql`*

```sql
-- SILVER: one clean row per earthquake event, with real data types.
--
-- Step 1 (typed):  turn the text into timestamps and numbers, rename columns to plain names.
--                  try_cast gives NULL instead of an error when a value cannot be converted.
-- Step 2 (ranked): the same event can sit in several files, because USGS revises recent events.
--                  Number the versions of each event, newest first.
-- Step 3:          keep version 1 only.
--
-- The CONSTRAINT lines are data quality rules. The pipeline counts, per rule, how many rows broke it
-- (open the pipeline run and look at the data quality tab).
--   * with ON VIOLATION DROP ROW  the broken row is removed.
--   * with nothing after it       the row is kept and the violation is only counted (a warning).
--
-- Note: a magnitude below 0 is NOT an error. Very small quakes really do have negative magnitudes.

CREATE OR REFRESH MATERIALIZED VIEW quakes_silver (
  CONSTRAINT has_event_id     EXPECT (event_id IS NOT NULL)                ON VIOLATION DROP ROW,
  CONSTRAINT has_event_time   EXPECT (event_time IS NOT NULL)              ON VIOLATION DROP ROW,
  CONSTRAINT has_magnitude    EXPECT (magnitude IS NOT NULL)               ON VIOLATION DROP ROW,
  CONSTRAINT valid_latitude   EXPECT (latitude BETWEEN -90 AND 90)         ON VIOLATION DROP ROW,
  CONSTRAINT valid_longitude  EXPECT (longitude BETWEEN -180 AND 180)      ON VIOLATION DROP ROW,
  CONSTRAINT is_reviewed      EXPECT (status = 'reviewed')
)
COMMENT 'One row per earthquake event (latest version), typed and checked'
AS
WITH typed AS (
  SELECT
    id                                   AS event_id,
    try_cast(time AS TIMESTAMP)          AS event_time,
    try_cast(time AS DATE)               AS event_date,
    try_cast(latitude AS DOUBLE)         AS latitude,
    try_cast(longitude AS DOUBLE)        AS longitude,
    try_cast(depth AS DOUBLE)            AS depth_km,
    try_cast(mag AS DOUBLE)              AS magnitude,
    magType                              AS magnitude_type,
    place,
    -- "10 km WNW of The Geysers, CA" -> "CA".  A place without a comma is its own region.
    COALESCE(NULLIF(regexp_extract(place, ',\\s*([^,]+)$', 1), ''), place) AS region,
    type                                 AS event_type,
    status,
    net                                  AS network,
    try_cast(updated AS TIMESTAMP)       AS updated_at,
    ingested_at
  FROM quakes_bronze
),
ranked AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY event_id
      ORDER BY updated_at DESC, ingested_at DESC
    ) AS version_rank
  FROM typed
)
SELECT
  event_id, event_time, event_date, latitude, longitude, depth_km, magnitude, magnitude_type,
  place, region, event_type, status, network, updated_at
FROM ranked
WHERE version_rank = 1;
```

**Gold** answers two questions: how active was each day, and where is it happening.

*File `src/pipeline/03_gold.sql`*

```sql
-- GOLD: two small tables that answer questions. Silver keeps every kind of event
-- (earthquake, quarry blast, explosion ...). Gold decides what counts: earthquakes only.

-- "How active was each day?"
CREATE OR REFRESH MATERIALIZED VIEW quakes_daily
COMMENT 'Earthquakes per day: how many, how strong'
AS
SELECT
  event_date,
  COUNT(*)                     AS quakes,
  ROUND(AVG(magnitude), 2)     AS avg_magnitude,
  MAX(magnitude)               AS strongest_magnitude,
  COUNT_IF(magnitude >= 4)     AS quakes_m4_plus
FROM quakes_silver
WHERE event_type = 'earthquake'
GROUP BY event_date;


-- "Where is it happening?"
CREATE OR REFRESH MATERIALIZED VIEW quakes_by_region
COMMENT 'Earthquakes per region (the last part of the USGS place text)'
AS
SELECT
  region,
  COUNT(*)                     AS quakes,
  MAX(magnitude)               AS strongest_magnitude,
  ROUND(AVG(depth_km), 1)      AS avg_depth_km,
  COUNT_IF(magnitude >= 4)     AS quakes_m4_plus
FROM quakes_silver
WHERE event_type = 'earthquake'
GROUP BY region;
```

### 2.3 Task 3: verify

*File `src/99_verify.py`*

```python
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
```

### 2.4 The pipeline's settings, as code

In a workspace you would click a name, a catalog, a schema, serverless, and a setting for the landing path. Here those clicks are lines in a file.

*File `resources/quakes.pipeline.yml`*

```yaml
# The pipeline: Bronze -> Silver -> Gold, written as three SQL files.
# Every setting you would click in the pipeline page of the workspace is a line in this file.

resources:
  pipelines:
    quakes_pipeline:
      name: quakes-pipeline

      # Where the tables are created.
      catalog: ${var.catalog}
      schema: ${var.schema}

      serverless: true

      # The setting the Bronze file reads as ${quakes.landing_path}.
      configuration:
        quakes.landing_path: /Volumes/${var.catalog}/${var.schema}/landing/quakes

      # The source code of the pipeline: every file in src/pipeline.
      # The pipeline works out the order itself: Silver needs Bronze, Gold needs Silver.
      libraries:
        - glob:
            include: ../src/pipeline/**
```

`${var.catalog}` and `${var.schema}` are placeholders. The target you deploy to fills them in.

### 2.5 The job, as code

*File `resources/quakes.job.yml`*

```yaml
# The job: download -> run the pipeline -> verify.
#
# There is no cluster section on purpose: with none given, the notebook tasks run on serverless compute.

resources:
  jobs:
    quakes_job:
      name: quakes-job

      # Job parameters reach the notebooks as widgets with the same name.
      # The values come from the target in databricks.yml, so dev and prod each get their own.
      parameters:
        - name: catalog
          default: ${var.catalog}
        - name: schema
          default: ${var.schema}
        - name: days_back
          default: "30"

      tasks:
        - task_key: download_quakes
          notebook_task:
            notebook_path: ../src/01_download_quakes.py
          # This task talks to the internet, so one retry is cheap insurance against a flaky connection.
          max_retries: 1
          min_retry_interval_millis: 60000

        - task_key: run_pipeline
          depends_on:
            - task_key: download_quakes
          pipeline_task:
            # ${resources.pipelines.quakes_pipeline.id} is filled in with the real id at deploy time.
            pipeline_id: ${resources.pipelines.quakes_pipeline.id}

        - task_key: verify
          depends_on:
            - task_key: run_pipeline
          notebook_task:
            notebook_path: ../src/99_verify.py

      # Every day at 06:00 UTC. Paused so the workshop does not leave something running.
      # To switch it on: change PAUSED to UNPAUSED and deploy again.
      schedule:
        quartz_cron_expression: "0 0 6 * * ?"
        timezone_id: UTC
        pause_status: PAUSED

      max_concurrent_runs: 1
```

Read it top to bottom: three tasks, each waits for the one before. The middle task runs the pipeline. `${resources.pipelines.quakes_pipeline.id}` is filled in with the real pipeline id at deploy time, which replaces picking the pipeline from a drop-down.

### 2.6 The bundle file, with the dev target

Create `databricks.yml`. **Replace** `<your_catalog>` and `<your_name>` with your values.

*File `databricks.yml`*

```yaml
# The bundle: one file that says WHAT we deploy and WHERE.
#
#   what  = the pipeline and the job in resources/, plus the files in src/ they use
#   where = the targets below: dev and prod
#
# Commands (run them in the folder that holds this file):
#   databricks bundle validate -t dev            check the files
#   databricks bundle deploy   -t dev            create or update the pipeline and job
#   databricks bundle run quakes_job -t dev      start the job
#   databricks bundle deploy   -t prod           same code, next environment
#
# The workspace is not written here: the CLI takes it from the login you made with
# `databricks auth login`.

bundle:
  name: quakes

include:
  - resources/*.yml

# Values that differ per environment. The code never contains them; it receives them as settings.
variables:
  catalog:
    description: Unity Catalog catalog where you created your schemas
  schema:
    description: Schema of this environment, with a landing volume inside it

targets:
  # ---- dev: where you try changes -----------------------------------------------------------
  dev:
    mode: development # names get a [dev <you>] prefix and schedules are paused
    default: true
    variables:
      catalog: <your_catalog> # <-- fill in
      schema: <your_name>_dev # <-- fill in, the schema you created in the SQL editor
```

Also create a file `.gitignore` with this line, so the CLI's working files stay out of Git:

```
.databricks/
```

### 2.7 Check, deploy, run

In the terminal, in the repo folder:

```bash
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run quakes_job -t dev
```

* `validate` reads your files and checks them. It changes nothing in the workspace.
* `deploy` creates the pipeline and the job in the workspace, and uploads your files.
* `run` starts the job and prints its progress in the terminal. The first run takes several minutes.

If `validate` or `deploy` complains, read the message. The usual causes are a `<placeholder>` you did not replace, or a typo in a file name or indentation.

### 2.8 Look at what you created

In the workspace:

1. **Jobs & Pipelines**. There is a job called `[dev <you>] quakes-job` and a pipeline called `[dev <you>] quakes-pipeline`. The prefix comes from `mode: development`.
2. Open the finished job run: three green boxes.
3. Open the pipeline. The graph shows `quakes_bronze` > `quakes_silver` > `quakes_daily` and `quakes_by_region`.
4. **Catalog** > your catalog > `<name>_dev`: the four tables, and in the `landing` volume a folder `quakes` with 30 CSV files, one per day.

> **See 2.** You did not tell the pipeline the order Bronze, Silver, Gold. It read it from the table names in your SQL.

> **See 3.** Click `quakes_silver` in the pipeline, then the **Data quality** tab. The rule `is_reviewed` shows a few hundred violations. Those events are still `automatic`. They were **kept**, only counted, because that rule has no `DROP ROW`. The other rules show zero.

> **See 4.** Open the job's `verify` task output: `All checks passed`, then the ten most active regions.

### 2.9 Run it again

```bash
databricks bundle run quakes_job -t dev
```

The download task now says `2 to download, 28 skipped`: old days are never fetched twice, the last two days are fetched every time because USGS may have revised them. Open `quakes_bronze` and `quakes_silver` again.

> **See 5.** Bronze grew by about 500 rows (the last two days, downloaded again). Silver barely moved: it keeps one version per event. That is the dedup in Silver at work, and the `verify` task checks it.

> **Checkpoint 2.** The dev job run is green and `verify` printed `All checks passed.`

---

## Part 3: Put it in the repo (15 min)

```bash
git status
git add .
git commit -m "Add earthquake pipeline and job as a bundle"
git push
```

`git status` first: read the list. You should see your files, and **not** a `.databricks` folder.

If `git push` is rejected because `main` is protected, create a branch and a pull request instead (the steps are in Part 5).

In Azure DevOps, **Repos** > **Files**: your files are there. Open `resources/quakes.job.yml` and `resources/quakes.pipeline.yml` in the browser.

> **See 6.** The repo holds the code **and** the definition of the pipeline and the job. If you hand this repo to someone, they can create everything with one command. With a plain Git folder in Databricks, the repo would hold only notebooks, and the pipeline settings and the job would exist only as clicks in one workspace.

> **Checkpoint 3.** All files are visible in the repo in Azure DevOps, and there is no `.databricks` folder in it.

---

## Part 4: Deploy to the next environment from the repo (30 min)

"Prod" here is the schema `<name>_prod`. The rule of prod: **nobody edits files in the prod folder. Prod only changes when `main` changes.**

### 4.1 Add the prod target

In `databricks.yml` add the prod target at the end of the file, under `targets:`, at the same indent as `dev:`. Replace the placeholders.

*File `databricks.yml (add at the end)`*

```yaml
  # ---- prod: the next environment -----------------------------------------------------------
  # No `mode: production` here on purpose. That mode demands that the job runs as a service
  # principal (a machine identity) and that permissions are listed, which a workshop does not have.
  # A real prod target would add:  mode: production, run_as: <service principal>, permissions: [...]
  # To use a second workspace, add under prod:
  #   workspace:
  #     host: https://<prod-workspace>.azuredatabricks.net
  prod:
    variables:
      catalog: <your_catalog> # <-- fill in
      schema: <your_name>_prod # <-- fill in, the schema you created in the SQL editor
```

Check, then push:

```bash
databricks bundle validate -t prod
git add databricks.yml
git commit -m "Add prod target"
git push
```

A target is a name and a few values. The pipeline and job files did not change at all.

### 4.2 Deploy from a fresh copy of the repo

Do not deploy prod from the folder where you edit. Clone the repo again, into a second folder next to it:

```bash
cd ..
git clone https://dev.azure.com/<organization>/<project>/_git/quakes-pipeline quakes-deploy
cd quakes-deploy
```

This second folder is your "build server": it only ever receives what is in `main`. It has no `.databricks` folder and no edits.

```bash
databricks bundle validate -t prod
databricks bundle deploy -t prod
databricks bundle run quakes_job -t prod
```

### 4.3 Compare dev and prod

In **Jobs & Pipelines** there are now two of each:

| | dev | prod |
|---|---|---|
| Job | `[dev <you>] quakes-job` | `quakes-job` |
| Pipeline | `[dev <you>] quakes-pipeline` | `quakes-pipeline` |
| Tables in | `<name>_dev` | `<name>_prod` |
| Schedule | paused by `mode: development` | paused in `quakes.job.yml` (`PAUSED`) |

> **See 7.** One set of files, two environments. What differs is only the target: the schema, and the name prefix. Nothing was typed twice and nothing was clicked.

> **Checkpoint 4.** The prod job ran green from the second clone, and `<name>_prod` holds the same four tables.

---

## Part 5: Ship a change through the whole path (30 min)

The change: the daily table should also count magnitude 5 and above. You will move it from your laptop to prod the way a team does: branch, test in dev, pull request, merge, deploy prod.

Go back to the folder where you edit:

```bash
cd ../quakes-pipeline
git checkout -b add-m5-column
```

Open `src/pipeline/03_gold.sql`. In `quakes_daily` change the last column line, adding a comma and one new line, so that

```sql
  COUNT_IF(magnitude >= 4)     AS quakes_m4_plus
FROM quakes_silver
```

becomes

```sql
  COUNT_IF(magnitude >= 4)     AS quakes_m4_plus,
  COUNT_IF(magnitude >= 5)     AS quakes_m5_plus
FROM quakes_silver
```

Test it in dev:

```bash
databricks bundle deploy -t dev
databricks bundle run quakes_job -t dev
```

Open `quakes_daily` in `<name>_dev`: it has a new column `quakes_m5_plus`. In `<name>_prod` it does not. Prod has not changed.

Push the branch:

```bash
git add src/pipeline/03_gold.sql
git commit -m "Count magnitude 5+ per day"
git push -u origin add-m5-column
```

In Azure DevOps: **Repos** > **Pull requests** > **New pull request** (Azure DevOps often shows a banner offering this). Source `add-m5-column`, target `main`. Look at the diff: two lines. **Create**, then **Complete** (merge).

Now deploy prod from the clean copy:

```bash
cd ../quakes-deploy
git pull
databricks bundle deploy -t prod
databricks bundle run quakes_job -t prod
```

Open `quakes_daily` in `<name>_prod`: the new column is there.

> **See 8.** The only way the change reached prod was through `main`. Your edit in `quakes-pipeline` could not go to prod by itself. In `quakes-deploy` nobody edits; it only pulls and deploys.

### Optional: drift

1. In the workspace, open the **prod** job `quakes-job`. Change the job parameter `days_back` from `30` to `5`. Save.
2. In `quakes-deploy`, run `databricks bundle deploy -t prod`.
3. Open the job again. `days_back` is `30` again.

> **See 9.** Someone changed prod by hand, and the next deploy put it back to what the repo says. The repo is the truth.

> **Checkpoint 5.** `quakes_daily` in `<name>_prod` has the column `quakes_m5_plus`, and it got there through a pull request.

---

## Part 6: Clean up (10 min)

From each of the two folders, delete what the bundle deployed. The command asks you to confirm:

```bash
cd ../quakes-pipeline
databricks bundle destroy -t dev
cd ../quakes-deploy
databricks bundle destroy -t prod
```

This permanently deletes the deployed job and pipeline. The tables the pipeline owns go with it. Then in the SQL editor:

```sql
DROP SCHEMA IF EXISTS <catalog>.<name>_dev CASCADE;
DROP SCHEMA IF EXISTS <catalog>.<name>_prod CASCADE;
```

Keep the repo. It is a complete small example for an interview: a pipeline, a job, two environments, deployed from Git.

### What a real team adds next

* **A check before the merge.** An Azure Pipeline that runs `databricks bundle validate` and your tests on every pull request, and blocks the merge if they fail.
* **Deploy on merge.** A second pipeline that runs `databricks bundle deploy -t prod` when `main` changes, instead of a person in a terminal.
* **A service principal for prod.** The prod job runs as a machine identity, not as a person, with `mode: production` and `run_as` in the target.

---

## Appendix A: If you want a Git folder in Databricks (optional)

A **Git folder** is a copy of a repo inside the workspace, so you can edit and commit from the browser. The workshop does not need it. If you want one:

1. In the sidebar open **Workspace**. Browse to a folder under **Users** > your own user folder.
2. Click **Create** > **Git folder**. (If you only see **Folder**, see the table below.)
3. Paste the repo address and choose the provider **Azure DevOps Services**. Name the folder. Click **Create Git folder**.

| Problem | Cause and fix |
|---|---|
| Only **Folder** is offered, no **Git folder** | You may not have **CAN MANAGE** on the folder you are in. Go to your own user folder and try there. If it is still missing, a workspace admin may have switched the Git folders feature off. It is on by default. Ask the admin. |
| The repo address does not work | Use `https://dev.azure.com/<organization>/<project>/_git/<repo>`. If the address you copied has `<organization>@` in front of `dev.azure.com`, remove that part. |
| It asks for credentials, or fails to clone | Profile (top right) > **Settings** > **Linked accounts** > **Add Git credential**, provider Azure DevOps Services. If Azure DevOps and Databricks use the same Microsoft Entra ID tenant, sign-in can happen automatically. Otherwise create a **personal access token** in Azure DevOps (user settings > Personal access tokens) and paste it there with your Azure DevOps email. |
| Pull or push buttons do nothing | Running them from the Git folder screen needs serverless compute and network access from the workspace to Azure DevOps. |

Databricks can also create a pipeline project inside a Git folder from the workspace UI (**New** > **ETL pipeline** > **Set up as source-controlled**). It produces the same kind of bundle files you wrote by hand in Part 2.

## Appendix B: If something goes wrong

| Symptom | Likely cause and fix |
|---|---|
| `databricks: command not found` | The CLI is not installed, or the terminal was not reopened after installing. |
| `bundle validate` says a field is unknown | The CLI is too old. Update it. |
| `bundle validate` mentions a variable | A `<your_catalog>` or `<your_name>` placeholder is still in `databricks.yml`. |
| `bundle deploy` says the schema or volume cannot be found, or the download task cannot find the landing folder | The SQL in step 0.4 did not run, or the `schema` in `databricks.yml` has a typo. |
| The download task fails with a network error | Your workspace may block outbound internet. Ask the admin whether serverless compute can reach `earthquake.usgs.gov`. |
| Pipeline: Bronze is empty | `quakes.landing_path` does not match where the download task saved files. Check the schema in the target. |
| `bundle deploy -t prod` fails with a message about `mode: production` | You added `mode: production` to prod. Remove it for the workshop (see the comment in `databricks.yml`). |
| `git push` is rejected | `main` is protected. Use a branch and a pull request. |
| `git push` asks for a password | Use **Generate Git Credentials** on the Clone window of the repo (step 0.2). |
