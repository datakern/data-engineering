-- ==============================================================================
-- 🐘 DataKern Data Engineering — Session 3
-- EXERCISE 4: Gate-4 Validation Diagnostics (Prove Before You Trust)
--
-- Goal: Write the 4 diagnostic SQL queries that every Data Engineer must run
-- after loading data into PostgreSQL before signing off on table quality.
--
-- The 4 Checks:
--   1. Row Count Reconciliation (Did all source rows arrive?)
--   2. Primary Key Null Check (Are any identity values missing?)
--   3. Uniqueness Check (Are there duplicate keys at the grain?)
--   4. Referential Integrity / Orphan Check (Do foreign keys match parents?)
-- ==============================================================================

-- -----------------------------------------------------------------------------
-- CHECK 1: Row Count Reconciliation
--
-- Your Python processing step reported that `order_items.csv` has 112,650 rows.
-- Write a query to count the total rows currently in `silver.order_items`.
-- -----------------------------------------------------------------------------

-- -> Write your query below:




-- -----------------------------------------------------------------------------
-- CHECK 2: Null Check on Critical Key Columns
--
-- In `silver.order_items`, neither `order_id` nor `order_item_id` can EVER be NULL.
-- Write a diagnostic query that counts how many rows have a NULL in either
-- `order_id` OR `order_item_id`.
-- (Expected result for a healthy table: 0 rows).
-- -----------------------------------------------------------------------------

-- -> Write your query below:




-- -----------------------------------------------------------------------------
-- CHECK 3: Duplicate Key Check at Table Grain
--
-- Remember: The grain of `silver.order_items` is (order_id, order_item_id).
-- Write a query using `GROUP BY` and `HAVING COUNT(*) > 1` to find any 
-- duplicate composite keys in `silver.order_items`.
-- (Expected result for a healthy table: 0 rows returned).
-- -----------------------------------------------------------------------------

-- -> Write your query below:




-- -----------------------------------------------------------------------------
-- CHECK 4: Referential Integrity / Orphan Foreign Key Check
--
-- An "orphan" order item is a row in `silver.order_items` whose `order_id` 
-- does not exist in `silver.orders`.
--
-- Write a query using a `LEFT JOIN` between `silver.order_items` (as oi)
-- and `silver.orders` (as o) on `order_id` that returns all order items 
-- that have no matching parent in `silver.orders`.
-- (Hint: Look for rows where the parent's primary key `IS NULL`).
-- -----------------------------------------------------------------------------

-- -> Write your query below:




-- -----------------------------------------------------------------------------
-- BONUS CHECK 5: Multi-Parent Orphan Check
--
-- `silver.order_items` has TWO parents: `silver.orders` and `silver.products`.
-- Write a query to find any order items that reference a `product_id` that 
-- does NOT exist in `silver.products`.
-- -----------------------------------------------------------------------------

-- -> Write your query below:


