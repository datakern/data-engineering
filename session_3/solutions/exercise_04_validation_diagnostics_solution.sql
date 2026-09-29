-- ==============================================================================
-- 🐘 DataKern Data Engineering — Session 3
-- SOLUTION: EXERCISE 4 — Gate-4 Validation Diagnostics
-- ==============================================================================

-- -----------------------------------------------------------------------------
-- CHECK 1: Row Count Reconciliation
-- -----------------------------------------------------------------------------
SELECT COUNT(*) AS total_rows_loaded
FROM silver.order_items;

-- Engineering Insight:
-- If your Python processing script logged 112,650 rows, this query MUST return 
-- exactly 112,650. If the number is smaller, rows were dropped or rejected.
-- If larger, duplicate ingestion happened.


-- -----------------------------------------------------------------------------
-- CHECK 2: Null Check on Critical Key Columns
-- -----------------------------------------------------------------------------
SELECT 
    COUNT(*) AS total_null_keys
FROM silver.order_items
WHERE order_id IS NULL 
   OR order_item_id IS NULL;

-- Engineering Insight:
-- For any primary key column, the only acceptable result is 0.
-- Even though DDL enforces NOT NULL, running this query validates that the constraint
-- was actually applied and active during load.


-- -----------------------------------------------------------------------------
-- CHECK 3: Duplicate Key Check at Table Grain
-- -----------------------------------------------------------------------------
SELECT 
    order_id,
    order_item_id,
    COUNT(*) AS occurrence_count
FROM silver.order_items
GROUP BY 
    order_id, 
    order_item_id
HAVING COUNT(*) > 1;

-- Engineering Insight:
-- A table that respects its grain will return ZERO rows.
-- If rows appear here, your primary key definition failed or the data contains
-- un-deduplicated duplicates that will corrupt downstream aggregations.


-- -----------------------------------------------------------------------------
-- CHECK 4: Referential Integrity / Orphan Foreign Key Check (vs silver.orders)
-- -----------------------------------------------------------------------------
SELECT 
    oi.order_id,
    oi.order_item_id,
    oi.product_id,
    oi.price
FROM silver.order_items AS oi
LEFT JOIN silver.orders AS o 
    ON oi.order_id = o.order_id
WHERE o.order_id IS NULL;

-- Engineering Insight:
-- How this works: A LEFT JOIN keeps ALL rows from the left table (`order_items`).
-- If there is a matching row in `orders`, `o.order_id` is populated.
-- If there is NO match in the parent table, `o.order_id` is filled with NULL.
-- Therefore, filtering by `WHERE o.order_id IS NULL` isolates the orphans!


-- -----------------------------------------------------------------------------
-- BONUS CHECK 5: Multi-Parent Orphan Check (vs silver.products)
-- -----------------------------------------------------------------------------
SELECT 
    oi.order_id,
    oi.order_item_id,
    oi.product_id
FROM silver.order_items AS oi
LEFT JOIN silver.products AS p 
    ON oi.product_id = p.product_id
WHERE p.product_id IS NULL;

-- Engineering Insight:
-- In real e-commerce datasets, products are often deleted or retired in the web app
-- while historical order lines still reference them. This diagnostic detects 
-- catalog discrepancies immediately upon load.
