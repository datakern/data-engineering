# Data Model Document

## Schema Responsibilities

- **`raw`**: The immutable landing zone. Tables here perfectly mirror the source CSV files in structure and content (e.g., `raw.olist_orders`). All columns are typically loaded as strings or raw inferred types.
- **`clean`**: The trusted business entity layer. Tables here have enforced data types, primary keys, and foreign keys. (e.g., `clean.customers`, `clean.orders`, `clean.products`).
- **`staging`**: The intermediate transformation layer. Used to break down complex logic. (e.g., `staging.finance_sales_detailed`, `staging.ops_delivery_classification`).
- **`curated`**: The final data products consumed by business teams. Grain is explicitly defined by the business question. (e.g., `curated.finance_monthly_category_sales`, `curated.ops_monthly_delivery_performance`).

## CLEAN Layer Relationships (ERD Concepts)

### `clean.customers`
- **Purpose**: Unique customer definitions.
- **Grain**: One row = one unique customer ID.
- **Primary Key**: `customer_id`

### `clean.orders`
- **Purpose**: Order lifecycle events.
- **Grain**: One row = one unique order.
- **Primary Key**: `order_id`
- **Foreign Keys**: `customer_id` -> `clean.customers.customer_id`

### `clean.order_items`
- **Purpose**: Individual items purchased in an order.
- **Grain**: One row = one item in one order (`order_id`, `order_item_id`).
- **Primary Key**: (`order_id`, `order_item_id`)
- **Foreign Keys**: 
  - `order_id` -> `clean.orders.order_id`
  - `product_id` -> `clean.products.product_id`

### `clean.products`
- **Purpose**: Product catalog attributes.
- **Grain**: One row = one unique product.
- **Primary Key**: `product_id`

## Constraints Explained
- **Primary Keys** ensure we don't duplicate entities, avoiding massive fan-outs when joining.
- **Foreign Keys** ensure referential integrity (e.g., we cannot have an order item that belongs to a non-existent order).
