-- ==============================================================================
-- 🐘 DataKern Data Engineering — Session 3
-- SOLUTION: EXERCISE 5 — SQL Grain Traps & Analytics Engineering
-- ==============================================================================

-- -----------------------------------------------------------------------------
-- PUZZLE 1: The Double-Counting Grain Trap (Debugging)
-- -----------------------------------------------------------------------------
-- 1. What is the bug?
-- Answer: 
-- The table `silver.order_items` is at the item grain, not the order grain.
-- When you JOIN `orders` to `order_items`, an order with 4 items is expanded into 
-- 4 distinct rows.
-- Running `COUNT(*)` counts the physical rows in the joined result set — which means 
-- it was counting TOTAL ITEMS SOLD, not total orders placed!
--
-- 2. Corrected Query:
SELECT 
    c.customer_state,
    COUNT(DISTINCT o.order_id) AS total_orders
FROM silver.customers AS c
JOIN silver.orders AS o 
    ON c.customer_id = o.customer_id
-- Note: You don't even need to join silver.order_items if you only need order counts!
-- But if other item columns are needed, COUNT(DISTINCT o.order_id) ensures correctness:
GROUP BY c.customer_state
ORDER BY total_orders DESC;


-- -----------------------------------------------------------------------------
-- PUZZLE 2: The Logical Execution Order Error
-- -----------------------------------------------------------------------------
-- 1. Why does PostgreSQL throw "column total_item_cost does not exist"?
-- Answer:
-- SQL queries are NOT executed from top to bottom. The logical execution order is:
--   Step 1: FROM (and JOINs)
--   Step 2: WHERE (filters rows before aggregation)
--   Step 3: GROUP BY
--   Step 4: HAVING
--   Step 5: SELECT (expressions evaluated and ALIASES created here!)
--   Step 6: ORDER BY
--   Step 7: LIMIT
--
-- Because the WHERE clause (Step 2) is evaluated long before the SELECT clause 
-- (Step 5), the column alias `total_item_cost` does not exist yet when PostgreSQL
-- is evaluating the filter.
--
-- 2. Corrected Query:
SELECT 
    order_id,
    order_item_id,
    (price + freight_value) AS total_item_cost
FROM silver.order_items
WHERE (price + freight_value) > 200.00;


-- -----------------------------------------------------------------------------
-- PUZZLE 3: Diagnostic Query (Unreviewed Orders)
-- -----------------------------------------------------------------------------
SELECT 
    o.order_id,
    o.order_status,
    o.order_purchase_timestamp
FROM silver.orders AS o
LEFT JOIN silver.order_reviews AS r 
    ON o.order_id = r.order_id
WHERE r.review_id IS NULL
ORDER BY o.order_purchase_timestamp DESC
LIMIT 10;

-- Engineering Insight:
-- A LEFT JOIN preserves all orders. If an order never received a review,
-- all columns coming from `silver.order_reviews` will be NULL.
-- Filtering on `r.review_id IS NULL` gives the Operations/Product teams the exact
-- list of orders where customer feedback is missing.


-- -----------------------------------------------------------------------------
-- PUZZLE 4: Building Your First Gold Business Model
-- -----------------------------------------------------------------------------
SELECT 
    p.product_category_name,
    COUNT(DISTINCT oi.order_id) AS order_count,
    ROUND(SUM(oi.price + oi.freight_value), 2) AS total_revenue
FROM silver.order_items AS oi
JOIN silver.products AS p 
    ON oi.product_id = p.product_id
WHERE p.product_category_name IS NOT NULL
GROUP BY p.product_category_name
ORDER BY total_revenue DESC
LIMIT 10;

-- Engineering Insight:
-- This is what Gold represents! Silver stores the clean, normalized individual entities
-- (`orders`, `products`, `items`). Gold creates a queryable, aggregated data model
-- designed specifically to answer executive business questions like:
-- "Which product categories generate the most revenue?"
