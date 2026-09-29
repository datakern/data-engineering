# Data Observations Log

*Instructor Note: Students should fill this file based on their interactive exploration in part A. Below is a reference for expected observations.*

### File: olist_orders_dataset.csv
- **Observation:** 99,441 rows.
- **Evidence (operation/output):** `orders.shape`
- **Why it may matter:** Knowing the baseline size helps us confirm processing doesn't accidentally drop rows.
- **Proposed action (or no action):** No action needed. Keep all rows.
- **Reason / assumption to verify:** We need all orders for complete analysis.

### File: olist_orders_dataset.csv
- **Observation:** `order_purchase_timestamp` and other date columns are loaded as object (strings).
- **Evidence (operation/output):** `orders.info()`
- **Why it may matter:** We can't do date arithmetic or sorting easily if they are strings.
- **Proposed action:** Convert date columns to datetime objects in our processing script.
- **Reason:** Better downstream analysis.

### File: olist_orders_dataset.csv
- **Observation:** Missing values in `order_approved_at`, `order_delivered_carrier_date`, `order_delivered_customer_date`.
- **Evidence (operation/output):** `orders.isna().sum()` shows missing values.
- **Why it may matter:** Missing delivery dates might correspond to cancelled or unavailable orders.
- **Proposed action:** Do NOT drop them. Leave as missing (NaT).
- **Reason:** Missingness here has business meaning (e.g., order was cancelled before approval/delivery).

### File: olist_products_dataset.csv
- **Observation:** `product_category_name` has missing values (610 missing).
- **Evidence:** `products.isna().sum()`
- **Why it may matter:** Affects category-based analysis.
- **Proposed action:** Fill missing values with 'Unknown'.
- **Reason:** Keeps the product records intact without losing data for product-level analysis.

### File: olist_order_reviews_dataset.csv
- **Observation:** `review_comment_title` and `review_comment_message` have many missing values.
- **Evidence:** `reviews.isna().sum()`
- **Why it may matter:** Missing text means the user just left a score, no comment.
- **Proposed action:** Fill with 'No comment'.
- **Reason:** Standardizes the text field and makes it explicit that no comment was provided.
