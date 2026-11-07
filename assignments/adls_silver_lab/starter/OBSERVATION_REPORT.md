![DataKern Watermark](https://img.shields.io/badge/DATAKERN-Assignment-F26522?style=for-the-badge)
_www.data-kern.com_

# Observation Report: ADLS → Bronze → Silver

> **How to fill this in:** copy every number from **your own run**. Do not round and do not guess. The source files are identical for every student, so row counts should match across the class; a different number is a signal to investigate your pipeline, not to adjust the report.
> Short answers are better than long ones. One or two sentences in your own words, every time you see **Why?**

**Name:** ______________________  **Date:** ______________

---

## Scoreboard (fill this in last)

| What | Your value |
|---|---|
| Storage account | |
| Catalog used | |
| Bronze rows: Aug / Sep / Oct / total | |
| Silver rows (`silver.taxi_trips`) | |
| Rejected rows (`silver.taxi_trips_rejected`) | |
| Exact duplicates removed | |
| Weather rows (Bronze / Silver) | |
| Check cells: notebook 1 passed / total | |
| Check cells: notebook 2 passed / total | |
| Trips per day, rainy vs dry (§9) | |
| Total time spent (hours) | |

---

## §1 Raw landing (Notebook 1, Step 1)

| Path in ADLS | File size (MB) |
|---|---|
| `raw/taxi/yellow_tripdata_2025-08.parquet` | |
| `raw/taxi/yellow_tripdata_2025-09.parquet` | |
| `raw/taxi/yellow_tripdata_2025-10.parquet` | |
| `raw/weather/nyc_daily_2025-08-01_2025-10-31.json` | |

**Why** keep an untouched copy in `raw/` instead of writing Delta straight from the download?

> _your answer_

**How** did you move the file from the internet into ADLS on serverless? (the 2–3 calls you used, and what each does)

> _your answer_

---

## §2 Bronze (Notebook 1, Steps 2–3)

| Month | Rows in raw Parquet | Rows in `bronze.taxi_trips` |
|---|---|---|
| 2025-08 | | |
| 2025-09 | | |
| 2025-10 | | |

**Three schema surprises** in the Bronze taxi table (column, type you saw, why it surprised you):

| Column | Type in Bronze | Why odd or surprising |
|---|---|---|
| | | |
| | | |
| | | |

**Weather:** rows in Bronze = ______. Why is "one JSON object with parallel arrays" an awkward shape for analytics, and what does one row per date make easy?

> _your answer_

**Partitioning:** list the folders you see under `bronze/taxi_trips/` (paste `dbutils.fs.ls` output). What is each folder, and why did we partition by `source_month`?

> _your answer_

---

## §3 Re-run test (Notebook 1, Step 5)

| | Bronze rows (all months) |
|---|---|
| Before re-running Step 2 | |
| After re-running Step 2 | |

Number of Delta versions in `DESCRIBE HISTORY` of `bronze.taxi_trips` after the re-run: ______

**Why** did the row count stay the same? What would have happened with plain `append`? Explain `replaceWhere` in your own words.

> _your answer_

---

## §4 Registration evidence (Notebook 1 Step 4 and Notebook 2 Step 5)

Fill one row per table (from `DESCRIBE EXTENDED`):

| Table | Type | Provider | Location (path after `.dfs.core.windows.net/`) |
|---|---|---|---|
| `bronze.taxi_trips` | | | |
| `bronze.weather_daily` | | | |
| `silver.taxi_trips` | | | |
| `silver.taxi_trips_rejected` | | | |
| `silver.weather_daily` | | | |

List the files you find in `bronze/taxi_trips/_delta_log/` (first 3 names) and say what the `.json` files in that folder are for:

> _your answer_

**Screenshots** (save in an `evidence/` folder next to this report and link them):

1. ADLS container `lakehouse` showing the folders `raw`, `bronze`, `silver`: `![adls](evidence/adls.png)`
2. Catalog Explorer showing the `bronze` and `silver` schemas with all 5 tables: `![catalog](evidence/catalog.png)`
3. Output of the check cells in both notebooks: `![checks](evidence/checks.png)`

**Why** is it safe to register these as *external* tables over the same container? What happens to the files in ADLS if someone runs `DROP TABLE` on an external table? (Do not test it.)

> _your answer_

---

## §5 What I found when I profiled Bronze (Notebook 2, Step 1)

| Question | Your number |
|---|---|
| Total Bronze rows | |
| Earliest pickup timestamp | |
| Latest pickup timestamp | |
| Pickups outside the month of their own file | |
| Rows with dropoff ≤ pickup | |
| Rows with distance ≤ 0 | |
| Rows with fare ≤ 0 | |
| Null count of `passenger_count` | |

Which columns share exactly the same large null count? ______________________________________
Which `payment_type` value has that same row count? ______ What does that payment type mean (see Step 3 table)?

> **Why** do those columns all go null together? What does it tell you about those rows?
> _your answer_

---

## §6 Rejected rows and my decisions (Notebook 2, Step 2)

| `reject_reason` | Rows rejected | One example row (pickup, dropoff, distance, fare) | Why this row is not a real, usable trip |
|---|---|---|---|
| `pickup_outside_source_month` | | | |
| `dropoff_not_after_pickup` | | | |
| `duration_over_6h` | | | |
| `distance_invalid` | | | |
| `fare_invalid` | | | |
| **Total rejected** | | | |

**Decision 1:** Millions of rows have a null `passenger_count`. We did **not** reject them. Why not? What would the business lose if we had?

> _your answer_

**Decision 2:** A row can break several rules at once, but gets one `reject_reason`. Take a row that has both `distance ≤ 0` and `fare ≤ 0`: which reason does it get, and why does the **order** of the rules change your rejected counts per reason but not the **total**?

> _your answer_

**Decision 3:** Why does the rejected table keep the *original* (uncleaned) values and Bronze column names?

> _your answer_

**Your own judgment:** pick **one** of the five rules and say if you would loosen or tighten it (for example the 6-hour limit) and what evidence you would want first.

> _your answer_

---

## §7 Silver design (Notebook 2, Steps 3–4)

Fill the before/after table for **seven** columns of your choice from `silver.taxi_trips` (must include `passenger_count`, a money column and `store_and_fwd_flag`):

| Bronze column and type | Silver column and type | What changed and why |
|---|---|---|
| | | |
| | | |
| | | |
| | | |
| | | |
| | | |
| | | |

Silver rows per month: Aug ______ Sep ______ Oct ______  
Silver rows with a NULL `passenger_count`: ______ (compare with the Bronze null count in §5 and explain the difference)

**Why** decimal for money and not double? Give one concrete example of what can go wrong with double.

> _your answer_

**Why** `passenger_count` outside 1–6 becomes NULL instead of rejecting the trip, while a fare ≤ 0 rejects it?

> _your answer_

**Why** units in the weather column names (`temp_max_c`, `precip_mm`, …)? What is `is_rainy` and which threshold did we use?

> _your answer_

Weather in Silver: rows ______, rainy days ______ (`is_rainy = true`), dry days ______.

---

## §8 Reconciliation: nothing disappears silently

| Bronze rows | Silver rows | Rejected rows | Duplicates removed | Bronze − (Silver + Rejected + Duplicates) |
|---|---|---|---|---|
| | | | | **must be 0** |

Percentage of Bronze rows that were rejected: ______ %

**Why** is "rows in = rows out + rows rejected" the first thing a data engineer checks after building a layer? Describe one real situation where it would have caught a bug.

> _your answer_

---

## §9 Does Silver join? (Notebook 2, Step 5)

Paste the SQL you wrote (rainy vs dry days, joined on date):

```sql
-- your query
```

| is_rainy | Days | Trips | Trips per day |
|---|---|---|---|
| false | | | |
| true | | | |

In one sentence: what does this number *not* prove? (Hint: how many rainy days are there, and what else changes on those days?)

> _your answer_

---

## §10 Problems I hit

At least **two** real errors or surprises from your own run (not the ones in the hints).

| What I saw (paste the error line) | Why it happened | How I fixed it | Time lost |
|---|---|---|---|
| | | | |
| | | | |

---

## §11 Explain it back (no AI, from memory, one or two sentences each)

1. Bronze, Silver, Gold: what is the single job of **Silver**, in your words?
2. Why did Bronze keep the original column names and types, when it would have been easy to clean them there?
3. What is the difference between **rejecting** a row, **fixing** a value, and **keeping** a null? Give one example of each from this assignment.
4. If your Silver notebook crashes halfway through the write, what state is the Delta table in, and why is re-running it safe?
5. Your colleague drops the table `silver.taxi_trips`. Is the data gone? How do you bring the table back?
6. A new month (2025-11) arrives. List exactly what you would change or run to add it, in order.

> **1.**
> **2.**
> **3.**
> **4.**
> **5.**
> **6.**

---

## §12 How I used AI

| Question | Answer |
|---|---|
| Which AI tool(s) did you use? | |
| For which steps (list the notebook and step numbers)? | |
| One thing the AI got **wrong or that you had to change** (paste the original line and your fix) | |
| One line of AI-written code that you did not understand at first, and what it does | |
| One step you wrote yourself without AI, and why | |

> **Self-check before you submit:** close the AI tool. Pick any cell in your notebook. Can you explain every line and what breaks if you delete it? If not, go back to that cell.

---

## Submit

1. Both notebooks with output visible (run top to bottom).
2. This report, completely filled in, with the `evidence/` screenshots.
3. Commit everything: `git commit -m "assignment: adls bronze silver"` and share the link.

---

*Don’t focus on getting the answer. Focus on learning how to engineer the solution.*
**— Uma Kiran | DataKern | www.data-kern.com**
