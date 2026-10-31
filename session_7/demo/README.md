# DataKern: Session 7 demos (Spark, level up, Part 2)

Five hands-on demos that follow the slides of "Spark, level up, Part 2" in the same order.
You can **follow along live** with your instructor, or **run them later** on your own. Each script pauses so you can look at the Spark UI and at the files it creates.

**One story.** The demos build one small lakehouse on the Olist e-commerce data. Each demo uses what the one before it left behind:

```
01 partition & prune  →  02 BRONZE (tidy Parquet)  →  03 SILVER (Delta table)  →  04 MERGE new orders into it  →  05 keep the table healthy
```

If you skip a demo, the next one rebuilds the missing layer for you and tells you so.

| Run this | Slides | What you will see for yourself |
|---|---|---|
| `01_partitioning_and_pruning.py` | 13 | partition folders, pruning, the trap that defeats pruning, choosing a partition key |
| `02_repartition_and_tidy_write.py` | 11, 12 | repartition vs coalesce in the plan and in partition sizes, why `partitionBy` writes too many files, the fix, and the BRONZE layer |
| `03_delta_log_and_acid.py` | 14 to 16 | the `_delta_log` JSON, a crashed write that destroys a Parquet table but not a Delta table, the SILVER layer |
| `04_merge_time_travel_and_schema.py` | 17 to 19 | MERGE (update + insert in one commit), the duplicate-key trap, time travel, schema enforcement, DELETE by accident and RESTORE |
| `05_optimize_zorder_vacuum.py` | 22 to 25 | 40 small files, OPTIMIZE, Z-ORDER, VACUUM, and what VACUUM costs you |

Run them in order. Together they take about 55 to 70 minutes. Each demo has at most 5 steps and 4 OBSERVE cards.

## One-time setup

1. Install **Java 17 or newer** (for example Temurin 21 from https://adoptium.net). Close and reopen your terminal.
2. Use **Python 3.10 to 3.13**. Create a virtual environment and install the packages (exact versions are pinned in `requirements.txt`):
   ```
   python -m venv .venv
   # Windows PowerShell:  .venv\Scripts\Activate.ps1        Mac/Linux:  source .venv/bin/activate
   pip install -r requirements.txt
   ```
3. Put the Olist CSV files in `data/olist/` (next to these scripts), or point to your copy:
   - Windows PowerShell: `$env:OLIST_DIR="C:\path\to\olist"`
   - Mac/Linux: `export OLIST_DIR=/path/to/olist`

   Files used: orders, order items, customers. All three come from the Olist dataset you already have from the earlier sessions.
4. **Windows only:** Spark needs the Hadoop helper files. Your instructor shares a `hadoop` folder. Set `HADOOP_HOME` to it and add `%HADOOP_HOME%\bin` to your PATH.
5. **Internet is needed once.** The first demo downloads the Delta Lake jars (1 to 2 minutes). After that everything runs offline.

## How to run a demo

```
python 01_partitioning_and_pruning.py
```

- When Spark starts it prints the **Spark UI address** (usually http://127.0.0.1:4040). Open it and keep it open.
- Every step follows the same rhythm: `🤔 PREDICT` (answer out loud or on paper **before** you press ENTER) ➜ the code runs ➜ `👀 OBSERVE` (where to look, what to look for, what it means).
- Each script ends with a **clean reference** block: the whole demo as plain code, without pauses and prints. Copy from there.
- Everything the demos write goes to `demo_output/`. The lakehouse you build is in `demo_output/lakehouse/`. It is safe to delete the whole folder.
- You can re-run any demo. Demo 4 rolls the silver table back to version 0 first, so it always starts from the same place.
- Quick run without pauses (a smoke test, not for learning): `DEMO_AUTO=1 python 01_partitioning_and_pruning.py` (PowerShell: `$env:DEMO_AUTO=1`).

## What is real, and what is arranged

Everything runs on the real Olist data with the real Spark and Delta engines. Nothing is simulated. A few things are **arranged** so that a laptop can show what a cluster shows, and the scripts say so on screen:

| Where | What we arranged |
|---|---|
| Demo 3 | The crash is injected: one value raises an error at a moment we choose. The failure and its effect on the files are real |
| Demo 4 | The 6,532 new orders are real. The 300 corrections (`shipped` → `delivered`) are **constructed**, because the dataset is a frozen snapshot with no real update feed |
| Demo 5 | 1 MB target file size instead of 1 GB. The 7-day VACUUM safety check is switched off on a scratch table only |

## If something goes wrong

| Symptom | Likely cause |
|---|---|
| `java: command not found` | Java is not installed or the terminal was not reopened |
| `Service 'sparkDriver' failed ... Can't assign requested address` | your machine cannot resolve its own name. The scripts already set `SPARK_LOCAL_IP=127.0.0.1`; if you still see it, connect to any network or turn off your VPN and retry |
| `UnsatisfiedLinkError ... NativeIO` on Windows | `HADOOP_HOME` / `winutils.exe` missing |
| The first demo hangs for minutes at the start | it is downloading the Delta jars; wait, or check your internet |
| Delta fails to start | `pyspark` and `delta-spark` versions do not match: install from `requirements.txt` |
| `Cannot find ...olist_orders_dataset.csv` | wrong `data/olist` folder or `OLIST_DIR` |
| Spark UI port is not 4040 | another Spark is running; use the address the script prints |
| A scan line says "could not read the scan numbers" | the Spark UI port is blocked; open the **SQL** tab instead and read the same numbers from the `Scan parquet` box |
