# Validation Evidence

This document tracks the sanity checks performed at each layer of the pipeline.

## 1. RAW Load Validation
- **Goal**: Ensure CSV rows == Database rows.
- **Evidence**:
  - `olist_orders_dataset.csv` row count matched `raw.orders` `COUNT(*)`.
  - No data truncated during Python `to_sql` loading.

## 2. CLEAN Validation
- **Goal**: Ensure data types cast correctly and keys are unique.
- **Evidence**:
  - `SELECT COUNT(*)` on `clean.orders` equals `raw.orders`.
  - Checked for null Primary Keys: `SELECT COUNT(*) FROM clean.orders WHERE order_id IS NULL` -> 0.
  - Foreign key joins (`clean.order_items` JOIN `clean.orders`) did not drop expected records.

## 3. STAGING Validation
### Finance
- **Staging 1 (Detailed Sales)**: Grain is one order item. Join between `order_items` and `products` maintained the exact row count of `order_items`. No duplication.
- **Staging 2 (Monthly Aggregation prep)**: Date truncation worked correctly, verified a sample of 10 rows to ensure timestamps mapped to the correct YYYY-MM.

### Operations
- **Staging 1 (Delivery Classification)**: Checked boundary conditions.
  - Where `actual_delivery_date` == `estimated_delivery_date`, classified as 'On Time'.
  - Where `actual_delivery_date` > `estimated_delivery_date`, classified as 'Late'.
  - Where `actual_delivery_date` is NULL, classified as 'Unknown/Not Delivered'.

## 4. CURATED Validation (Reconciliation)
- **Goal**: Ensure final numbers tie back to clean entities.
- **Finance**: `SELECT SUM(sales_value) FROM curated.finance_monthly_category_sales` == `SELECT SUM(price) FROM clean.order_items WHERE <valid statuses>`.
- **Operations**: Sum of `late_orders` + `delivered_orders` matches total shipped orders in the given timeframe in `clean.orders`.
