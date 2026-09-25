-- ==============================================================================
-- 🐘 DataKern Data Engineering — Session 3
-- EXERCISE 2: The Database Bouncer (DDL Contracts & Schemas)
--
-- Goal: Translate the logical ERD into physical PostgreSQL table definitions (DDL).
-- Enforce data types, nullability, primary keys, and foreign keys.
-- 
-- The database is your system's bouncer: corrupt data must never enter Silver!
-- ==============================================================================

-- -----------------------------------------------------------------------------
-- TODO 1: Create the Medallion Schemas
-- In PostgreSQL, schemas act like directory folders inside a database.
-- Create three schemas if they do not already exist:
--   1. bronze
--   2. silver
--   3. gold
-- -----------------------------------------------------------------------------

-- -> Write your CREATE SCHEMA statements below:




-- -----------------------------------------------------------------------------
-- TODO 2: Create Parent Table: `silver.customers`
-- 
-- Columns required:
-- - `customer_id`              : Text identifier (e.g. VARCHAR(32)), NOT NULL, PRIMARY KEY
-- - `customer_unique_id`       : Text identifier (e.g. VARCHAR(32)), NOT NULL
-- - `customer_zip_code_prefix` : Postal code (e.g. VARCHAR(10)), NOT NULL
-- - `customer_city`            : City name (e.g. VARCHAR(100)), NOT NULL
-- - `customer_state`           : 2-character state code (e.g. CHAR(2) or VARCHAR(10)), NOT NULL
-- -----------------------------------------------------------------------------

-- -> Write your CREATE TABLE silver.customers statement below:




-- -----------------------------------------------------------------------------
-- TODO 3: Create Parent Table: `silver.products`
--
-- Columns required:
-- - `product_id`               : Text identifier (e.g. VARCHAR(32)), NOT NULL, PRIMARY KEY
-- - `product_category_name`    : Category text (e.g. VARCHAR(100)), CAN be NULL (some products have no category)
-- - `product_weight_g`         : Weight in grams (e.g. NUMERIC or INTEGER), CAN be NULL
-- - `product_length_cm`        : Length (e.g. NUMERIC or INTEGER), CAN be NULL
-- - `product_height_cm`        : Height (e.g. NUMERIC or INTEGER), CAN be NULL
-- - `product_width_cm`         : Width (e.g. NUMERIC or INTEGER), CAN be NULL
-- -----------------------------------------------------------------------------

-- -> Write your CREATE TABLE silver.products statement below:




-- -----------------------------------------------------------------------------
-- TODO 4: Create Child Table: `silver.orders`
--
-- Columns required:
-- - `order_id`                       : Text identifier (e.g. VARCHAR(32)), NOT NULL, PRIMARY KEY
-- - `customer_id`                    : Foreign key pointing to silver.customers(customer_id), NOT NULL
-- - `order_status`                   : Status text (e.g. VARCHAR(20)), NOT NULL
-- - `order_purchase_timestamp`       : Purchase timestamp (TIMESTAMP), NOT NULL
-- - `order_approved_at`              : Approval timestamp (TIMESTAMP), CAN be NULL
-- - `order_delivered_carrier_date`   : Carrier handover timestamp (TIMESTAMP), CAN be NULL
-- - `order_delivered_customer_date`  : Delivery timestamp (TIMESTAMP), CAN be NULL
-- - `order_estimated_delivery_date`  : Promised delivery timestamp (TIMESTAMP), NOT NULL
--
-- IMPORTANT: Add a FOREIGN KEY constraint referencing silver.customers(customer_id)!
-- -----------------------------------------------------------------------------

-- -> Write your CREATE TABLE silver.orders statement below:




-- -----------------------------------------------------------------------------
-- TODO 5: Create Child Table: `silver.order_items`
--
-- Remember the Bus Seat Analogy!
-- Primary key MUST be composite: PRIMARY KEY (order_id, order_item_id)
--
-- Columns required:
-- - `order_id`             : Text identifier (e.g. VARCHAR(32)), NOT NULL
-- - `order_item_id`        : Sequential item number in order (INTEGER), NOT NULL
-- - `product_id`           : Foreign key to silver.products(product_id), NOT NULL
-- - `seller_id`            : Seller identifier (e.g. VARCHAR(32)), NOT NULL
-- - `shipping_limit_date`  : Shipping deadline timestamp (TIMESTAMP), NOT NULL
-- - `price`                : Monetary price (NUMERIC(10, 2)), NOT NULL
-- - `freight_value`        : Shipping cost (NUMERIC(10, 2)), NOT NULL
--
-- IMPORTANT:
-- - Declare PRIMARY KEY (order_id, order_item_id)
-- - Add FOREIGN KEY referencing silver.orders(order_id)
-- - Add FOREIGN KEY referencing silver.products(product_id)
-- -----------------------------------------------------------------------------

-- -> Write your CREATE TABLE silver.order_items statement below:




-- -----------------------------------------------------------------------------
-- TODO 6: Create Child Table: `silver.order_reviews`
--
-- Columns required:
-- - `review_id`               : Review identifier (e.g. VARCHAR(32)), NOT NULL, PRIMARY KEY
-- - `order_id`                : Foreign key to silver.orders(order_id), NOT NULL
-- - `review_score`            : Rating score from 1 to 5 (SMALLINT or INTEGER), NOT NULL
-- - `review_comment_title`    : Optional comment title (TEXT or VARCHAR(100)), CAN be NULL
-- - `review_comment_message`  : Optional message body (TEXT), CAN be NULL
-- - `review_creation_date`    : Timestamp review survey sent (TIMESTAMP), NOT NULL
-- - `review_answer_timestamp` : Timestamp customer submitted (TIMESTAMP), NOT NULL
--
-- IMPORTANT:
-- - Add FOREIGN KEY referencing silver.orders(order_id)
-- -----------------------------------------------------------------------------

-- -> Write your CREATE TABLE silver.order_reviews statement below:




-- -----------------------------------------------------------------------------
-- VERIFICATION QUERY
-- Once you have executed your DDL, run this query to verify all 5 tables exist
-- in the `silver` schema:
-- -----------------------------------------------------------------------------
SELECT table_schema, table_name
FROM information_schema.tables
WHERE table_schema = 'silver'
ORDER BY table_name;
