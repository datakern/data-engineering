# 🐘 Data Engineering Session 3: Exercises & Testing Guide

Welcome to the hands-on exercises for **Session 3: From Validated Files to Analytics-Ready Data**!

In your previous assignment, you used Pandas to explore, process, and validate CSV files. In this session, you are stepping into the database world to build durable, relational, and governed systems.

These exercises guide you step-by-step through the core skills of a Data Engineer:
1. **Mental Modeling & Grain** (Design before you build)
2. **Table Contracts & DDL** (The database bouncer)
3. **Safe Loading & Dependency Order** (Parent-child hierarchy)
4. **Gate-4 Validation Diagnostics** (Prove before you trust)
5. **Analytical Queries & Grain Traps** (Avoiding double-counting traps)

---

## 📂 Exercise Files Overview

| Exercise File | Focus Topic | Format |
| :--- | :--- | :--- |
| [`exercise_01_modeling_and_grain.md`](./exercise_01_modeling_and_grain.md) | Entity Grains, Cardinality & Key Selection | Markdown & Modeling Prompts |
| [`exercise_02_ddl_contracts.sql`](./exercise_02_ddl_contracts.sql) | Schemas, Data Types, Constraints & DDL | PostgreSQL SQL script |
| [`exercise_03_safe_loading_order.sql`](./exercise_03_safe_loading_order.sql) | Dependency Order, Transactions & Rollback | PostgreSQL SQL script |
| [`exercise_04_validation_diagnostics.sql`](./exercise_04_validation_diagnostics.sql) | Gate 4 Quality Checks (Nulls, Duplicates, Orphans) | Diagnostic SQL queries |
| [`exercise_05_sql_grain_and_analytics.sql`](./exercise_05_sql_grain_and_analytics.sql) | Execution Order, Diagnostic Joins & Gold Reporting | Analytical SQL queries |

*Complete solutions with detailed engineering rationales can be found in the [`solutions/`](./solutions/) folder.*

---

## 🛠️ Environment Setup

You can run these SQL exercises using **PostgreSQL** (via `psql`, **DBeaver**, **pgAdmin**, or the **VS Code PostgreSQL extension**).

### Quick Setup with Docker (Recommended)
If you don't have PostgreSQL installed locally, launch one in seconds with Docker:

```bash
docker run --name datakern-postgres \
  -e POSTGRES_USER=datakern \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=olist_warehouse \
  -p 5432:5432 \
  -d postgres:16
```

To connect via terminal:
```bash
docker exec -it datakern-postgres psql -U datakern -d olist_warehouse
```

Or connect using any GUI tool (DBeaver / pgAdmin / VS Code):
- **Host:** `localhost`
- **Port:** `5432`
- **Database:** `olist_warehouse`
- **Username:** `datakern`
- **Password:** `postgres`

---

## 🧭 Step-by-Step Exercise Walkthrough

### Step 1: Modeling & Grain (`exercise_01_modeling_and_grain.md`)
* Open `exercise_01_modeling_and_grain.md`.
* Before writing a line of SQL, define the **grain** ("One row represents...") for each Olist entity.
* Identify the **Primary Keys**, **Foreign Keys**, and the **Composite Key** using the Bus Seat analogy.

### Step 2: Write Table Contracts (`exercise_02_ddl_contracts.sql`)
* Open `exercise_02_ddl_contracts.sql`.
* Fill in the `TODO` sections to create the `silver` schema and declare tables with explicit data types (`VARCHAR`, `NUMERIC(10,2)`, `TIMESTAMP`), `NOT NULL` constraints, and key relationships.
* Run the script in your database to verify there are no syntax errors.

### Step 3: Master Safe Loading Order (`exercise_03_safe_loading_order.sql`)
* Foreign key constraints mean you cannot load tables in random order!
* Trace which tables are **parents** (no foreign keys) and which are **children** (dependent on parents).
* Understand how `BEGIN`, `COMMIT`, and `ROLLBACK` protect the database from corrupt partial loads.

### Step 4: Run Gate-4 Diagnostics (`exercise_04_validation_diagnostics.sql`)
* *"A load that finishes without crashing is not automatically correct."*
* Fill in the 4 verification queries:
  1. Row count comparison against source file.
  2. Primary key NULL check.
  3. Duplicate key check using `GROUP BY ... HAVING COUNT(*) > 1`.
  4. Orphan foreign key check using `LEFT JOIN ... WHERE parent.id IS NULL`.

### Step 5: Solve Grain Traps & Build Gold Analytics (`exercise_05_sql_grain_and_analytics.sql`)
* Practice identifying the silent killer in SQL: **double-counting when joining 1-to-many tables**.
* Fix a broken query using `COUNT(DISTINCT ...)`.
* Write a diagnostic `LEFT JOIN` to find unreviewed orders.
* Write a Gold-layer business query computing monthly revenue by product category.

---

## 🏆 Definition of Done
You are ready for the PostgreSQL assignment when you can:
1. Explain the grain of any table in plain English without hesitating.
2. Defend every data type and constraint choice in your DDL.
3. Name all 4 validation checks and explain what failure in each check signals.
4. Explain why joining `orders` to `order_items` changes the row count and how to write aggregations safely.

*Don’t focus on getting the answer. Focus on learning how to engineer the solution.*  
**— DataKern • www.data-kern.com**
