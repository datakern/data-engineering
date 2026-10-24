# DataKern: Spark session demos (windows, Parquet, Delta, speed)

Four hands-on demos that follow the slides of the session "Spark, level up", in the same order.
You run them **after** the session, on your own laptop, at your own pace. Each script pauses
so you can look at the Spark UI and at the files it creates.

| Run this | Slides | What you will see for yourself |
|---|---|---|
| `00_setup_check.py` | | Java, Spark, Delta and your Olist files all work |
| `01_windows_and_udf.py` | 9 to 12 | latest order per customer, top 3 per category, running totals, the one-partition window trap, UDF cost |
| `02_parquet_and_partitioning.py` | 13 to 16 | CSV vs Parquet size and speed, row-group statistics, folders per month, repartition vs coalesce |
| `03_delta_acid.py` | 17 to 19 | a crash that destroys a Parquet table but not a Delta table, MERGE, time travel, schema enforcement, RESTORE |
| `04_skew_and_table_health.py` | 20 to 23 | a straggler task in the Spark UI and three fixes, then OPTIMIZE, Z-ORDER and VACUUM |

## One-time setup

1. Install **Java 17 or newer** (for example Temurin 21 from https://adoptium.net). Close and reopen your terminal.
2. Create a virtual environment and install the packages (exact versions are pinned in `requirements.txt`):
   ```
   python -m venv .venv
   # Windows PowerShell:  .venv\Scripts\Activate.ps1        Mac/Linux:  source .venv/bin/activate
   pip install -r requirements.txt
   ```
3. **Download the Olist data** (about 45 MB zipped, not stored in this repo):
   1. Open https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce ("Brazilian E-Commerce Public Dataset by Olist"). A free Kaggle account is needed to download.
   2. Click **Download** and unzip the file.
   3. Create the folder `data/olist/` next to these scripts and copy these **6 files** into it (ignore the other CSVs in the zip, the demos do not use them):
      ```
      data/olist/olist_orders_dataset.csv
      data/olist/olist_order_items_dataset.csv
      data/olist/olist_customers_dataset.csv
      data/olist/olist_products_dataset.csv
      data/olist/olist_sellers_dataset.csv
      data/olist/product_category_name_translation.csv
      ```
   4. Or keep the files anywhere you like and point to that folder:
      - Windows PowerShell: `$env:OLIST_DIR="C:\path\to\olist"`
      - Mac/Linux: `export OLIST_DIR=/path/to/olist`

   `python 00_setup_check.py` (step 5) tells you if a file is missing.
4. **Windows only:** Spark needs the Hadoop helper files. Your instructor shares a `hadoop` folder. Set `HADOOP_HOME` to it and add `%HADOOP_HOME%\bin` to your PATH.
5. Run the check: `python 00_setup_check.py`. Fix anything it reports before the demos. The first Delta run needs internet (it downloads the Delta jars, 1 to 2 minutes).

## How to run a demo

```
python 01_windows_and_udf.py
```

- When Spark starts it prints the **Spark UI address** (usually http://localhost:4040). Open it and keep it open.
- Every step follows the same rhythm: `📥 INPUT DATA` (the table the query works on, press ENTER when you have understood the query) ➜ `🤔 PREDICT` (answer out loud or on paper **before** you press ENTER) ➜ `📤 OUTPUT` (what the query produced) ➜ ENTER ➜ `👀 OBSERVE` (what to look at).
- Every `👀 OBSERVE` has three lines: **where** to look (a Spark UI tab, a folder, or the terminal), **what to look for**, and **what it means**.
- Each script ends with a **clean reference** block: the same code without the pauses and prints.
- Everything the demos write goes to `demo_output/`. It is safe to delete.
- Quick run without pauses (for a smoke test): `DEMO_AUTO=1 python 01_windows_and_udf.py` (PowerShell: `$env:DEMO_AUTO=1`).

## If something goes wrong

| Symptom | Likely cause |
|---|---|
| `java: command not found` | Java is not installed or the terminal was not reopened |
| `UnsatisfiedLinkError ... NativeIO` on Windows | `HADOOP_HOME` / `winutils.exe` missing |
| Delta fails on the first run | no internet to download the Delta jars, or `pyspark` and `delta-spark` versions do not match (use `requirements.txt`) |
| `PySpark does not yet fully support pandas >= 3` | install `pandas<3` (it is in `requirements.txt`) |
| `Cannot find ...olist_orders_dataset.csv` | wrong `data/olist` folder or `OLIST_DIR` |
| Spark UI port is not 4040 | another Spark is running; use the address the script prints |
| Out of memory | set `DEMO_DRIVER_MEM=4g` (and `DEMO_AMPLIFY=5` for demo 2) |
