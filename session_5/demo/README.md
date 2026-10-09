# DataKern — Session 5 Interactive Demo & Masterclass
## Spark Internals, Advanced Transformations, Parquet & Delta Lake

**Session 5 · Data Engineering · DataKern**  
*Prerequisites: Session 4 Spark Fundamentals & Assignment 4*

---

## 🎯 Executive Summary & Objectives

Welcome to the hands-on interactive demo for **Session 5**. In Session 4, you learned how to start a SparkSession and build basic DataFrame pipelines. In Session 5, we open the hood and master **how Apache Spark actually behaves in a distributed cluster**, how to optimize pipelines for production, and how modern Lakehouses leverage **Parquet** and **Delta Lake**.

This demo is designed in **"Observe Mode"**: every script breaks execution into digestible steps, prompts you to **PREDICT** behavior before it happens, and tells you exactly what metrics to **OBSERVE** in the terminal and live **Spark UI**.

---

## 🗺️ Demo Roadmap

| Script | Core Focus | What You Will Observe & Learn |
|---|---|---|
| [`01_the_shuffle_and_data_skew.py`](file:///Users/ramadugu/Documents/vscode_git/datakern/data_engineering_regular_sessions/session_5/demo/scripts/01_the_shuffle_and_data_skew.py) | **The Shuffle & Data Skew** | Narrow vs Wide transformations, Exchange boundaries, Shuffle Read/Write bytes, and how São Paulo's 41.7% customer concentration creates straggler tasks. |
| [`02_join_strategies_and_catalyst_optimizer.py`](file:///Users/ramadugu/Documents/vscode_git/datakern/data_engineering_regular_sessions/session_5/demo/scripts/02_join_strategies_and_catalyst_optimizer.py) | **Joins, Catalyst & Caching** | Why SortMergeJoin shuffles both tables, how BroadcastHashJoin eliminates fact-table shuffles, reading `.explain()` for Predicate Pushdown and Column Pruning, and memory hygiene with `.cache()` and `.unpersist()`. |
| [`03_production_schemas_nulls_and_udfs.py`](file:///Users/ramadugu/Documents/vscode_git/datakern/data_engineering_regular_sessions/session_5/demo/scripts/03_production_schemas_nulls_and_udfs.py) | **Production Data Hygiene** | Why `inferSchema=True` fails in production, explicit `StructType` definitions, treating Nulls as business design decisions (in-flight orders vs anomalies), and the severe IPC serialization penalty of Python UDFs. |
| [`04_window_functions_intro.py`](file:///Users/ramadugu/Documents/vscode_git/datakern/data_engineering_regular_sessions/session_5/demo/scripts/04_window_functions_intro.py) | **Window Functions Intro** | Mental model: `groupBy` collapses row grain vs `Window` preserves every row. Calculating cumulative running category revenue and ranking top items (`row_number` vs `rank`). |
| [`05_parquet_anatomy_and_partitioning.py`](file:///Users/ramadugu/Documents/vscode_git/datakern/data_engineering_regular_sessions/session_5/demo/scripts/05_parquet_anatomy_and_partitioning.py) | **Parquet & Partitioning** | Columnar storage vs CSV row-orientation, ~70% disk savings with Snappy, Column Pruning query speedup, directory trees with `partitionBy`, and Partition Pruning. |
| [`06_delta_lake_and_medallion_architecture.py`](file:///Users/ramadugu/Documents/vscode_git/datakern/data_engineering_regular_sessions/session_5/demo/scripts/06_delta_lake_and_medallion_architecture.py) | **Delta Lake & Medallion** | ACID transactions, inspecting `_delta_log/000000.json` commits, Time Travel with `versionAsOf`, audit trail via `DeltaTable.history()`, Schema Evolution (`mergeSchema`), and Medallion Architecture (Bronze ➔ Silver ➔ Gold). |

*(Prefer Jupyter Notebooks? We also provide [`session_5_interactive_masterclass.ipynb`](file:///Users/ramadugu/Documents/vscode_git/datakern/data_engineering_regular_sessions/session_5/demo/session_5_interactive_masterclass.ipynb) containing all 6 modules in an interactive notebook!)*

---

## ⚙️ Environment Setup

### 1. Verify Java Installation
Apache Spark runs on the Java Virtual Machine (JVM). Ensure Java 11 (or Java 8/17) is available:
```bash
java -version
```

### 2. Set Up Virtual Environment & Dependencies
From the `session_5/demo/` directory:
```bash
# Navigate to the demo directory
cd data_engineering_regular_sessions/session_5/demo

# Create virtual environment
python3 -m venv .venv

# Activate virtual environment
source .venv/bin/activate        # On Windows: .venv\Scripts\activate

# Install PySpark and Delta Lake
pip install -r requirements.txt
```

*Requirements include:*
- `pyspark==3.5.1`
- `delta-spark==3.2.0`

### 3. Verify Data Files
The demo uses the Brazilian Olist e-commerce dataset:
- `olist_orders_dataset.csv`
- `olist_order_items_dataset.csv`
- `olist_products_dataset.csv`
- `olist_customers_dataset.csv`
- `olist_order_reviews_dataset.csv`

The `demo/data/` folder is pre-linked to the dataset. If running independently on another machine, copy the CSV files from Assignment 1 into `demo/data/`.

---

## 🖥️ Live Spark UI Companion (http://localhost:4040)

Whenever a demo script is running, Spark starts a web server at **`http://localhost:4040`**.

> [!IMPORTANT]
> Keep your browser open side-by-side with your terminal. Here is what to inspect on each tab:

| Tab | What to Look For | Script Reference |
|---|---|---|
| **Jobs** | See lazy evaluation materialized. Notice how one action triggers one Job. | Scripts 01, 02 |
| **Stages** | Look at Stage boundaries. Note **Shuffle Read** and **Shuffle Write** metrics. Expand **Summary Metrics for Completed Tasks** (Min vs Median vs Max duration) to detect **Data Skew**. | Script 01 |
| **SQL / DataFrame** | Visual DAG representation. Compare `SortMergeJoin` (Exchange on both tables) vs `BroadcastHashJoin` (zero shuffle on fact table). Check `WholeStageCodeGen` blocks. | Script 02 |
| **Storage** | Check cached DataFrames when calling `.cache()`. Observe **Size in Memory**, **Fraction Cached (100%)**, and see it disappear after `.unpersist()`. | Script 02 |
| **Executors** | Single driver process utilizing local laptop CPU threads (or multiple worker nodes in cloud). Active tasks, GC time, and memory usage. | Scripts 01, 05 |

---

## 🔬 Step-by-Step Script Walkthrough

### Script 01: The Shuffle & Data Skew
```bash
python scripts/01_the_shuffle_and_data_skew.py
```
- **Concept**:
  - Narrow transformation (`filter`, `select`): Workers process local memory chunks independently without exchanging raw rows.
  - DataFrame Aggregations (`count()`): Catalyst computes counts via a 2-phase aggregation (local partial counts per partition, followed by a tiny ~few-hundred-byte subtotal shuffle to 1 final task).
  - Wide transformation (`groupBy`): Workers must redistribute full records across partitions by key (`Exchange` boundary, shuffling megabytes of actual data).
- **The Skew Demo**:
  - In Olist, São Paulo (`SP`) contains **41.7% of all customers in Brazil** (~41,746 rows). Other states like Roraima (`RR`) have only 46 rows.
  - In the Spark UI Stages tab, observe the task duration distribution across your CPU cores: tasks handling tiny states finish in milliseconds, while the task processing `SP` takes significantly longer. **Your cluster is only as fast as its slowest straggler!**

---

### Script 02: Joins, Catalyst & Caching
```bash
python scripts/02_join_strategies_and_catalyst_optimizer.py
```
- **SortMergeJoin vs BroadcastHashJoin**:
  - Joining 112,000 order items with 32,000 products.
  - With default join: Spark shuffles and sorts *both* tables.
  - With `F.broadcast(df_products)`: Spark sends products to all workers once; items stream through with **zero shuffle**.
- **Catalyst Optimizer**:
  - Run `.explain(mode="formatted")`.
  - See **Predicate Pushdown** (`PushedFilters`) pushing `WHERE price > 150` directly down into the file scan!
  - See **Column Pruning** discarding unselected columns at the scan layer.
- **Caching Hygiene**:
  - Measure execution time before and after `.cache()`.
  - Inspect the **Storage Tab** in Spark UI.
  - Call `.unpersist()` to protect against OutOfMemory errors.

---

### Script 03: Production Schemas, Nulls & UDFs
```bash
python scripts/03_production_schemas_nulls_and_udfs.py
```
- **Schema Enforcement**:
  - Why `inferSchema=True` is dangerous (two file passes, string type inferences, schema drift risks).
  - Explicit `StructType` definitions enable fast single-pass reads and guaranteed contracts.
- **Null Handling as a Design Decision**:
  - In Olist, orders with status `shipped` or `processing` have `NULL` delivery dates because they are **currently on the delivery truck**!
  - Blindly calling `.dropna()` destroys valid in-transit business orders.
  - Practice 3 distinct strategies: drop corrupt primary keys, fill dimension fallbacks, and flag operational anomalies.
- **The UDF Trap**:
  - Benchmark Python `@F.udf` against native `F.when()`.
  - See the `BatchEvalPython` IPC socket penalty. Native Spark SQL executes in the JVM at whole-stage compiled speed!

---

### Script 04: Window Functions Intro
```bash
python scripts/04_window_functions_intro.py
```
- **Mental Model**:
  - `groupBy`: Collapses multiple rows into 1 row per group (loses row grain).
  - `Window`: Computes group metrics *while preserving every single row*.
- **Practical Patterns**:
  - **Running Cumulative Revenue**: Calculating cumulative category revenue chronologically using `Window.partitionBy().orderBy().rowsBetween(unboundedPreceding, currentRow)`.
  - **Ranking**: Isolating the Top 3 most expensive items per product category using `F.row_number().over(w_rank)`.
- **Bridge to Assignment 5**:
  - Preview how Window functions power Month-over-Month growth calculations (`F.lag()`) and customer cohort analysis.

---

### Script 05: Parquet Anatomy & Partitioning
```bash
python scripts/05_parquet_anatomy_and_partitioning.py
```
- **CSV vs Parquet**:
  - Compare file sizes on disk: Parquet Snappy compression achieves **~70% space reduction** over raw CSV.
  - Benchmark Column Pruning: Selecting 2 columns from Parquet is drastically faster than CSV.
- **Directory Partitioning**:
  - Write with `.partitionBy("order_status")`.
  - Inspect the generated directory hierarchy (`order_status=delivered/`, `order_status=shipped/`).
  - Demonstrate **Partition Pruning**: Spark reads only the requested folder and skips all other directories entirely.
  - The **Goldilocks Rule**: Avoid high cardinality columns (like `order_id` or timestamp) to prevent the Small Files Problem.

---

### Script 06: Delta Lake & Medallion Architecture
```bash
python scripts/06_delta_lake_and_medallion_architecture.py
```
- **The Lakehouse Missing Piece**:
  - Why plain Parquet is not enough (no transactions, no rollback on crashed jobs, corrupted concurrent writes).
  - Formula: **Delta Lake = Parquet + ACID Transaction Log (`_delta_log/`)**.
- **Transaction Log Inspection**:
  - Open and read `_delta_log/00000000000000000000.json` live! Inspect commit metadata, added files, and column min/max statistics.
- **Time Travel & History**:
  - Overwrite data and query both current (Version 1) and historical (Version 0 via `.option("versionAsOf", 0)`).
  - Audit table evolution with `DeltaTable.forPath(...).history().show()`.
- **Schema Evolution**:
  - Test Delta's schema enforcement blocking unauthorized columns, then safely evolve schema with `.option("mergeSchema", "true")`.
- **The Medallion Architecture**:
  - Build Bronze (raw ingest) ➔ Silver (cleaned, joined) ➔ Gold (curated Finance summary with Window ranking).

---

## 💡 Key Takeaways & Data Engineering Rules

1. **Narrow over Wide**: Filter early, filter often. Every Wide transformation (`groupBy`, `join`) crosses the network shuffle bridge.
2. **Broadcast Small Dimensions**: When joining fact tables (> 100k rows) with dimensions (< 100MB), always hint `F.broadcast()`.
3. **Never Cache Linearly**: Cache only for branching dataflows or iterative loops. Always pair `.cache()` with `.unpersist()`.
4. **Enforce Schemas at Ingestion**: Production pipelines must use `StructType`. Never rely on `inferSchema=True`.
5. **Nulls are Business States**: Missing delivery dates on shipped orders represent parcels on trucks. Always distinguish missing data from invalid data.
6. **Prefer JVM Native Functions**: Avoid Python UDFs unless absolutely necessary to avoid Py4J IPC serialization bottlenecks.
7. **Window Functions Preserve Grain**: Use `Window` when you need group metrics or ranks attached to individual transactions.
8. **Parquet for Analytics, Delta for Reliability**: Parquet provides columnar speed; Delta Lake adds ACID transactions, Time Travel, and Schema Evolution.

---

## 🎓 Next Steps: Assignment 5

With these 6 modules completed, you are fully prepared for **Assignment 5 (Spark Advanced & Delta Lake)**:
- Implementing Month-over-Month growth with `F.lag()` and customer cohort analysis.
- Structuring Medallion pipelines with Delta Lake outputs.
- Reconciling business revenue totals against your Assignment 4 milestones!

---

*Don't focus on getting the answer. Focus on learning how to engineer the solution.*  
— **Uma Kiran | DataKern | [www.data-kern.com](https://www.data-kern.com)**
