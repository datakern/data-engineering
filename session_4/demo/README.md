# DataKern — Session 4 Demo
## Hands-On Spark: From a Single Machine to Distributed Data

**Session 4 · Data Engineering · DataKern**

This is a hands-on demo where you run five Python scripts, each one building on the last. By the time you finish, you will have built and validated a distributed Finance pipeline using Apache Spark — the same pipeline you built in PostgreSQL in Assignment 3.

---

## What You Will Build

The Finance question is the same as Assignment 3:

> **"How is item sales value changing across product categories over time?"**

You will answer it five different ways, each time learning something new:

| Script | What you build | What you learn |
|---|---|---|
| `01_hello_spark.py` | A SparkSession, read a CSV, inspect data | Entry point, lazy vs eager, schema |
| `02_dataframe_pipeline.py` | The full Finance pipeline with the DataFrame API | Transformations, actions, grain |
| `03_spark_sql_pipeline.py` | The same pipeline using Spark SQL | SQL equivalence, API choice |
| `04_parquet_and_validation.py` | Write to Parquet, read back, validate | Output format, Delta Lake preview |
| `05_spark_ui_tour.py` | Navigate the Spark UI live | Jobs, Stages, Tasks, Catalyst DAG |

Run them in order. Each script ends by telling you what to run next.

---

## Setup — Do This First

### 1. Copy the Olist CSV files into the data/ folder

This demo uses the same five CSV files from Assignment 1. Copy them to `demo/data/`:

```bash
# From your Assignment 1 processed data directory:
cp path/to/your/data/olist_orders_dataset.csv         demo/data/
cp path/to/your/data/olist_order_items_dataset.csv    demo/data/
cp path/to/your/data/olist_products_dataset.csv       demo/data/
cp path/to/your/data/olist_customers_dataset.csv      demo/data/
cp path/to/your/data/olist_order_reviews_dataset.csv  demo/data/
```

You can use either the raw CSVs or your processed output from Assignment 1 — the scripts work with both.

### 2. Create a virtual environment

```bash
cd demo
python -m venv .venv
source .venv/bin/activate        # On Windows: .venv\Scripts\activate
```

### 3. Install PySpark

```bash
pip install -r requirements.txt
```

### 4. Check Java and verify PySpark works

PySpark runs on the JVM, so Java must be installed on your machine.
Do these two checks in the **same terminal** where you activated the venv:

```bash
java -version
```

Expected output (any Java 8, 11, or 17 is fine):
```
openjdk version "11.0.22" 2024-01-16
OpenJDK Runtime Environment ...
```

If Java is missing:
- **macOS:**
  ```bash
  brew install openjdk@11
  sudo ln -sfn /opt/homebrew/opt/openjdk@11/libexec/openjdk.jdk /Library/Java/JavaVirtualMachines/openjdk-11.jdk
  ```
  *(Then restart your terminal)*
- **Ubuntu/Debian:** `sudo apt install openjdk-11-jdk`
- **Windows:** 
  1. Go to [https://adoptium.net/temurin/releases/](https://adoptium.net/temurin/releases/)
  2. Select **Java 11** for **Windows** (Architecture: x64) and download the `.msi` installer.
  3. **Important during installation:** When you reach the "Custom Setup" screen, you MUST enable the option to **"Set JAVA_HOME variable"** (click the red X and change it to "Will be installed on local hard drive").
  4. After installation finishes, restart your terminal.

Then verify PySpark can import:

```bash
python -c "import pyspark; print(pyspark.__version__)"
```

Expected output — a version number on its own line:
```
3.5.1
```

If you see a version number, everything is ready.

> If `python` is not found, try `python3` instead.
> If you see `dquote>` in your terminal after running the command, press **Ctrl+C** to cancel.
> That usually means the command was copied with a broken character — type it manually or copy it again carefully.

---

## Running the Scripts

Run each script from the `demo/` folder:

```bash
cd demo

python scripts/01_hello_spark.py

If you see errors, first try setting the local IP address: using 

bash
export SPARK_LOCAL_IP="127.0.0.1"
then run the script again:

python scripts/01_hello_spark.py

python scripts/02_dataframe_pipeline.py
python scripts/03_spark_sql_pipeline.py
python scripts/04_parquet_and_validation.py
python scripts/05_spark_ui_tour.py
```
```

Each script runs in **Observe Mode**. 
Instead of instantly dumping a wall of text, the scripts will pause at each major step.

You will see prompts like this:
```
PREDICT: Before running the next line, think about this...
🔍 Press ENTER to continue ➔
OBSERVE: Check the Spark UI 'Jobs' tab and refresh.
```

**Keep the Spark UI (`http://localhost:4040`) open side-by-side with your terminal.** 
The scripts will explicitly tell you when to look at the UI and what to observe as you press Enter to execute the actions.

---

## The Spark UI (http://localhost:4040)

The Spark UI is a browser-based dashboard that shows you what Spark is doing while a script is running.

**Open it before starting script 05.** It is available at `http://localhost:4040` whenever a SparkSession is active.

### What each tab shows

| Tab | What you see | Concept from the session |
|---|---|---|
| **Jobs** | One entry per action you called (`.show()`, `.count()`, `.write`) | Lazy evaluation — nothing appears until an action |
| **Stages** | Each job broken into stages at shuffle boundaries | The DAG — joins and groupBys create stage boundaries |
| **Tasks** | One row per data partition within each stage | RDD partitions — each task = one executor processing one slice |
| **SQL / DataFrame** | Visual diagram of the Catalyst query plan | How Spark optimised your code before running it |
| **Executors** | Driver and Executor(s) with resource usage | The three-box architecture: Driver coordinates, Executors work |
| **Environment** | `spark.master = local[*]` and all configuration | What changes when you move to a production cluster |

### What to look for

**Jobs tab** — before any action, this tab is empty. The moment you call `.show()` or `.count()`, one job appears. That is lazy evaluation: nothing runs until you ask for a result.

**Stages tab** — click into any job to see its stages. The Finance pipeline (with joins and groupBy) will have more stages than a simple `.count()` — because each join and groupBy causes a shuffle, which creates a stage boundary.

**Tasks tab** — click into any stage to see its tasks. Each row is one task — one CPU core processing one partition. This is the RDD in action: your data was split into partitions and each partition was processed independently.

**SQL / DataFrame tab** — click on the most recent query to see the Catalyst plan. Look for how Spark may have reordered your steps. If you wrote the join before the filter, Catalyst may have moved the filter earlier — running it first reduces the number of rows entering the join. This is called predicate pushdown, and Spark did it automatically.

---

## Folder Structure

```text
demo/
├── README.md                              ← This file
├── requirements.txt                       ← pip install -r requirements.txt
│
├── data/                                  ← Olist CSV files (you copy these in)
│   ├── olist_orders_dataset.csv
│   ├── olist_order_items_dataset.csv
│   ├── olist_products_dataset.csv
│   ├── olist_customers_dataset.csv
│   └── olist_order_reviews_dataset.csv
│
├── scripts/                               ← Run these in order
│   ├── 01_hello_spark.py                  ← SparkSession, reading, inspecting
│   ├── 02_dataframe_pipeline.py           ← Finance pipeline, DataFrame API
│   ├── 03_spark_sql_pipeline.py           ← Finance pipeline, Spark SQL
│   ├── 04_parquet_and_validation.py       ← Write Parquet, validate, Delta Lake
│   └── 05_spark_ui_tour.py               ← Spark UI: Jobs, Stages, Tasks, DAG
│
└── output/                                ← Generated by script 04 (auto-created)
    └── curated/
        └── finance_monthly_category_sales/
```

---

## Troubleshooting

| Problem | What to try |
|---|---|
| `java.lang.RuntimeException: Java not found` | Install Java 8 or 11. Run `java -version` to confirm. |
| `BindException: Can't assign requested address` | Your Mac is struggling to resolve its own hostname. Run `export SPARK_LOCAL_IP="127.0.0.1"` in your terminal before running the scripts. |
| `AnalysisException: path does not exist` | Check that your CSV files are in `demo/data/`. |
| `Py4JJavaError` with type mismatch | Try `inferSchema=True` on the read, or check the CSV format. |
| First script takes 30–40 seconds to start | Normal — the JVM takes a moment to start. Subsequent scripts are faster. |
| `FileAlreadyExistsException` on write | The scripts use `.mode("overwrite")` — re-run the script and it will overwrite. |
| WARN messages in the terminal | Normal. The scripts suppress most Spark logs, but some warnings appear. |
| Spark UI not loading at 4040 | The SparkSession must be running — start one of the scripts first. |
| Port 4040 already in use | Spark will try 4041, 4042, etc. Check the terminal output for the correct port. |

---

*DataKern Data Engineering · Session 4*
*www.data-kern.com*
