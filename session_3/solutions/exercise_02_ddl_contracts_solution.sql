-- ==============================================================================
-- 🐘 DataKern Data Engineering — Session 3
-- SOLUTION: EXERCISE 2 — DDL Contracts & Schemas
-- ==============================================================================

-- -----------------------------------------------------------------------------
-- TODO 1: Create the Medallion Schemas
-- -----------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;


-- -----------------------------------------------------------------------------
-- TODO 2: Create Parent Table: `silver.customers`
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS silver.customers (
    customer_id              VARCHAR(32)     NOT NULL,
    customer_unique_id       VARCHAR(32)     NOT NULL,
    customer_zip_code_prefix VARCHAR(10)     NOT NULL,
    customer_city            VARCHAR(100)    NOT NULL,
    customer_state           VARCHAR(10)     NOT NULL,
    CONSTRAINT pk_silver_customers PRIMARY KEY (customer_id)
);


-- -----------------------------------------------------------------------------
-- TODO 3: Create Parent Table: `silver.products`
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS silver.products (
    product_id               VARCHAR(32)     NOT NULL,
    product_category_name    VARCHAR(100)    NULL,
    product_weight_g         NUMERIC(10, 2)  NULL,
    product_length_cm        NUMERIC(10, 2)  NULL,
    product_height_cm        NUMERIC(10, 2)  NULL,
    product_width_cm         NUMERIC(10, 2)  NULL,
    CONSTRAINT pk_silver_products PRIMARY KEY (product_id)
);


-- -----------------------------------------------------------------------------
-- TODO 4: Create Child Table: `silver.orders`
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS silver.orders (
    order_id                      VARCHAR(32)  NOT NULL,
    customer_id                   VARCHAR(32)  NOT NULL,
    order_status                  VARCHAR(20)  NOT NULL,
    order_purchase_timestamp      TIMESTAMP    NOT NULL,
    order_approved_at             TIMESTAMP    NULL,
    order_delivered_carrier_date  TIMESTAMP    NULL,
    order_delivered_customer_date TIMESTAMP    NULL,
    order_estimated_delivery_date TIMESTAMP    NOT NULL,
    CONSTRAINT pk_silver_orders PRIMARY KEY (order_id),
    CONSTRAINT fk_orders_customer FOREIGN KEY (customer_id) 
        REFERENCES silver.customers (customer_id)
);


-- -----------------------------------------------------------------------------
-- TODO 5: Create Child Table: `silver.order_items`
-- Notice: Composite Primary Key (order_id, order_item_id)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS silver.order_items (
    order_id            VARCHAR(32)     NOT NULL,
    order_item_id       INTEGER         NOT NULL,
    product_id          VARCHAR(32)     NOT NULL,
    seller_id           VARCHAR(32)     NOT NULL,
    shipping_limit_date TIMESTAMP       NOT NULL,
    price               NUMERIC(10, 2)  NOT NULL,
    freight_value       NUMERIC(10, 2)  NOT NULL,
    CONSTRAINT pk_silver_order_items PRIMARY KEY (order_id, order_item_id),
    CONSTRAINT fk_order_items_order FOREIGN KEY (order_id) 
        REFERENCES silver.orders (order_id),
    CONSTRAINT fk_order_items_product FOREIGN KEY (product_id) 
        REFERENCES silver.products (product_id)
);


-- -----------------------------------------------------------------------------
-- TODO 6: Create Child Table: `silver.order_reviews`
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS silver.order_reviews (
    review_id               VARCHAR(32)  NOT NULL,
    order_id                VARCHAR(32)  NOT NULL,
    review_score            SMALLINT     NOT NULL CHECK (review_score BETWEEN 1 AND 5),
    review_comment_title    VARCHAR(100) NULL,
    review_comment_message  TEXT         NULL,
    review_creation_date    TIMESTAMP    NOT NULL,
    review_answer_timestamp TIMESTAMP    NOT NULL,
    CONSTRAINT pk_silver_order_reviews PRIMARY KEY (review_id),
    CONSTRAINT fk_reviews_order FOREIGN KEY (order_id) 
        REFERENCES silver.orders (order_id)
);


-- -----------------------------------------------------------------------------
-- Verification Check
-- -----------------------------------------------------------------------------
SELECT table_schema, table_name
FROM information_schema.tables
WHERE table_schema = 'silver'
ORDER BY table_name;
