-- Scratchpad for SQL ideas before finalizing in the notebook

-- 1. Operations: Check delivery logic boundary cases
/*
SELECT 
    order_id,
    order_status,
    order_purchase_timestamp,
    order_estimated_delivery_date,
    order_delivered_customer_date,
    CASE 
        WHEN order_delivered_customer_date IS NULL THEN 'unknown/not_delivered'
        WHEN order_delivered_customer_date > order_estimated_delivery_date THEN 'late'
        ELSE 'on_time'
    END as delivery_classification
FROM clean.orders
LIMIT 50;
*/

-- 2. Finance: Joining order items to products to check grain
/*
SELECT 
    oi.order_id,
    oi.order_item_id,
    oi.price,
    COALESCE(p.product_category_name, 'Unknown') as category
FROM clean.order_items oi
LEFT JOIN clean.products p ON oi.product_id = p.product_id
LIMIT 50;
*/
