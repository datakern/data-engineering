-- ==============================================================================
-- 🐘 DataKern Data Engineering — Session 3
-- EXERCISE 3: Safe Loading & Dependency Hierarchy
--
-- Goal: Understand relational dependencies, transaction safety, and 
-- the 4-Gate loading pattern: Define -> Load -> Commit -> Validate.
--
-- Rule: Never load children before parents!
-- ==============================================================================

-- -----------------------------------------------------------------------------
-- TODO 1: Sequence the Loading Order
--
-- You have 5 tables to load from your processed CSV files:
--   - silver.orders
--   - silver.customers
--   - silver.order_items
--   - silver.products
--   - silver.order_reviews
--
-- Due to FOREIGN KEY constraints, PostgreSQL will reject rows if the parent
-- row does not exist yet.
--
-- In what exact order MUST you load these 5 tables? Fill in below:
--
-- Step 1 (Parent): _________________________
-- Step 2 (Parent): _________________________
-- Step 3 (Child) : _________________________
-- Step 4 (Child) : _________________________
-- Step 5 (Child) : _________________________
--
-- Question: Could Step 1 and Step 2 be swapped? Why or why not?
-- Answer:
-- -----------------------------------------------------------------------------


-- -----------------------------------------------------------------------------
-- TODO 2: The Foreign Key Bouncer (Live Test)
--
-- Below is a simulation. Try running this block in your database.
-- Notice what happens when we try to insert an order for a customer who DOES NOT exist.
-- -----------------------------------------------------------------------------

-- Attempting to insert an order without a customer in silver.customers:
/*
INSERT INTO silver.orders (
    order_id, 
    customer_id, 
    order_status, 
    order_purchase_timestamp, 
    order_estimated_delivery_date
) VALUES (
    'ord_test_001',
    'cust_phantom_999', -- This customer DOES NOT exist in silver.customers!
    'delivered',
    NOW(),
    NOW() + INTERVAL '5 days'
);
*/

-- Question: What exact error does PostgreSQL throw when you run the query above?
-- Answer: 


-- -----------------------------------------------------------------------------
-- TODO 3: Safe Transaction Pattern (BEGIN -> COMMIT / ROLLBACK)
--
-- In production data engineering, we wrap batch ingestion in a transaction block.
-- If anything fails (like a bad row or constraint violation), the entire batch 
-- is rolled back so the database is never left in a corrupt, half-loaded state.
--
-- Complete the transaction block below to safely insert a customer and their order:
-- -----------------------------------------------------------------------------

-- Step A: Start the transaction
-- -> Write the keyword to start a transaction:


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
    'cust_valid_101', -- Matches the parent above!
    'shipped',
    NOW(),
    NOW() + INTERVAL '3 days'
);

-- Step D: Make the changes permanent if no errors occurred
-- -> Write the keyword to finalize and commit the transaction:



-- -----------------------------------------------------------------------------
-- TODO 4: Rollback Understanding Check
--
-- Suppose Step B succeeded, but Step C failed because of a broken date format.
-- If you run `ROLLBACK;` instead of `COMMIT;`:
--
-- 1. Does `cust_valid_101` remain in `silver.customers`?
-- 2. Why is this behavior essential for data pipeline reliability?
-- -----------------------------------------------------------------------------
-- Your Answer:
-- 1. 
-- 2. 
