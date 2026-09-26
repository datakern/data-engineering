-- ==============================================================================
-- 🐘 DataKern Data Engineering — Session 3
-- SOLUTION: EXERCISE 3 — Safe Loading & Dependency Hierarchy
-- ==============================================================================

-- -----------------------------------------------------------------------------
-- TODO 1: Sequence the Loading Order
-- -----------------------------------------------------------------------------
-- Correct Safe Loading Order:
--   Step 1: silver.customers (Parent — no FK dependencies)
--   Step 2: silver.products  (Parent — no FK dependencies)
--   Step 3: silver.orders    (Child of customers — requires customer_id to exist)
--   Step 4: silver.order_items (Child of orders & products — requires both to exist)
--   Step 5: silver.order_reviews (Child of orders — requires order_id to exist)
--
-- Question: Could Step 1 and Step 2 be swapped?
-- Answer: YES! Neither `silver.customers` nor `silver.products` has any foreign 
-- keys pointing to other tables. They are independent root entities and can be 
-- loaded in either order or in parallel.
--
-- However, `silver.orders` MUST be loaded after `customers`, and `silver.order_items`
-- MUST be loaded after BOTH `orders` and `products`.


-- -----------------------------------------------------------------------------
-- TODO 2: The Foreign Key Bouncer (Live Test)
-- -----------------------------------------------------------------------------
-- When you attempt to insert an order with an unrecorded `customer_id`, 
-- PostgreSQL raises:
--
-- ERROR: insert or update on table "orders" violates foreign key constraint "fk_orders_customer"
-- DETAIL: Key (customer_id)=(cust_phantom_999) is not present in table "customers".
--
-- Key Insight: The database proactively protects itself from "orphan" rows!


-- -----------------------------------------------------------------------------
-- TODO 3: Safe Transaction Pattern (BEGIN -> COMMIT / ROLLBACK)
-- -----------------------------------------------------------------------------

-- Step A: Start the transaction
BEGIN;

-- Step B: Insert the parent record
INSERT INTO silver.customers (
    customer_id, 
    customer_unique_id, 
    customer_zip_code_prefix, 
    customer_city, 
    customer_state
) VALUES (
    'cust_valid_101',
    'uniq_101',
    '01310',
    'sao paulo',
    'SP'
);

-- Step C: Insert the dependent child record
INSERT INTO silver.orders (
    order_id, 
    customer_id, 
    order_status, 
    order_purchase_timestamp, 
    order_estimated_delivery_date
) VALUES (
    'ord_valid_501',
    'cust_valid_101',
    'shipped',
    NOW(),
    NOW() + INTERVAL '3 days'
);

-- Step D: Finalize and commit
COMMIT;


-- -----------------------------------------------------------------------------
-- TODO 4: Rollback Understanding Check
-- -----------------------------------------------------------------------------
-- 1. Does `cust_valid_101` remain in `silver.customers` if ROLLBACK is issued?
--    Answer: NO. An atomic transaction is "all or nothing" (Atomicity in ACID).
--    When ROLLBACK executes, PostgreSQL undoes EVERY modification made since 
--    the BEGIN statement, as if none of it ever happened.
--
-- 2. Why is this behavior essential for data pipeline reliability?
--    Answer: It completely prevents "half-loaded" or corrupt states. If a pipeline
--    crashes halfway through an ingestion batch, we can safely re-run the entire
--    batch from scratch without risking duplicate parent rows or dangling foreign keys.
