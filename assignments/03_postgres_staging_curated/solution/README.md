# Assignment 3: From Validated Files to Business Data Products

## Project Overview
This repository contains the solution for DataKern Assignment 3. The goal is to design and implement a Data Warehouse using PostgreSQL with a four-layer architecture (`raw`, `clean`, `staging`, `curated`). The pipeline processes data from raw CSV files into trustworthy business entities and ultimately answers specific business requirements for Finance and Operations teams.

## 📁 Repository Structure
```text
solution/
├── data/
│   ├── raw/                  # Source CSV evidence
│   └── processed/            # Assignment 1 processed data (if needed)
├── notebooks/
│   └── assignment_3_warehouse.ipynb  # Main pipeline and engineering notebook
├── docs/
│   ├── data_model.md         # Schema architecture, constraints, and relationships
│   ├── business_requirements.md # Definitions, metric logic, and grains
│   └── validation_evidence.md   # Data quality checks and validations
├── sql/
│   └── optional_sql_notes.sql   # Scratchpad for query ideas
├── README.md
├── requirements.txt
└── .gitignore
```

## 🛠️ Setup Instructions

### 1. Database Setup
Create the PostgreSQL database and schemas:
```sql
CREATE DATABASE olist_warehouse;
\c olist_warehouse

CREATE SCHEMA raw;
CREATE SCHEMA clean;
CREATE SCHEMA staging;
CREATE SCHEMA curated;
```

### 2. Python Environment
Install the required dependencies:
```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 3. Environment Variables
Create a `.env` file in the root directory and add your database connection string:
```env
DATABASE_URL=postgresql://<username>:<password>@localhost:5432/olist_warehouse
```

---

## 👨‍🏫 Instructor Explanations

1. **Why Four Schemas?**
   - `raw`: Preserves evidence. If a number looks wrong later, you can check what the source actually said. It's immutable.
   - `clean`: Trustworthy entities (Customer, Order, Product). Not built for one specific question, but built to represent the real world accurately.
   - `staging`: The "showing your work" layer. It avoids writing 200-line monolithic SQL queries. It breaks complex logic (like deciding if an order is late) into testable steps.
   - `curated`: The final product. Designed specifically for the business user. It should be simple enough that an analyst can `SELECT *` and get immediate value without needing to JOIN.

2. **Python vs. SQL**
   - We use Python (Pandas) to move files into the database (`raw`) because Python is excellent at I/O operations and inferring rough file structures.
   - We use SQL for transformations because relational joins and aggregations belong in a relational engine. The database is heavily optimized for this.

3. **Validation Strategy**
   - You must validate row counts at every step. If you JOIN `orders` to `order_items` and the row count explodes unexpectedly, you have a Cartesian product or a duplicated grain. Stop and fix it.

---

## 🎯 Things to Remember for Interviews

- **Data Modeling Mindset:** Interviewers care more about *how you think* about the data than your syntax. Always define the **grain** (what one row represents) before writing any SQL.
- **Traceability:** Be prepared to trace a metric backwards. If a stakeholder asks "Why is sales down?", you need to prove your pipeline accurately reflects the source data from `curated` -> `staging` -> `clean` -> `raw` -> `source file`.
- **Handling Nulls/Missing Data:** Never silently drop or impute data in the `raw` layer. If a category is missing, let it be missing. Handle it explicitly in `staging` with business logic (e.g., categorizing as 'Unknown').
- **Incremental Builds:** Explain that in production, you wouldn't drop and recreate tables every time. You would load incrementally (UPSERTS/MERGES) based on timestamps, but the logical layers remain the same.
- **Security:** Mentioning `.env` files and not committing credentials to GitHub shows maturity and adherence to basic security best practices.
