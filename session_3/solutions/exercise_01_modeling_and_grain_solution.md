# Solution: Exercise 1 — Data Modeling, Grain & Key Selection

![DataKern Watermark](https://img.shields.io/badge/DATAKERN-Solution-2A9D8F?style=for-the-badge)  
*Data Engineering Session 3: Official Answer Key*

---

## 📝 Task 1: Complete the Grain Statements

* **`silver.customers`**  
  **Grain Statement:** One row represents one **customer purchase account** (or customer session profile).  
  *Context:* In Olist, `customer_id` is assigned per order session, while `customer_unique_id` identifies the physical person across multiple orders. At the `customers` table level, each row is a unique `customer_id`.

* **`silver.products`**  
  **Grain Statement:** One row represents one **unique product listing / catalog item**.

* **`silver.orders`**  
  **Grain Statement:** One row represents one **order placed by a customer**.

* **`silver.order_items`**  
  **Grain Statement:** One row represents one **individual physical item line in an order**.  
  *⚠️ Critical Trap:* Never say "one row represents one order". If an order has 4 items, there are 4 rows in this table with the same `order_id`!

* **`silver.order_reviews`**  
  **Grain Statement:** One row represents one **customer review submission for an order**.

---

## 📝 Task 2: The Key Identification Matrix

| Table Name | Primary Key Column(s) | Is it a Composite Key? | Foreign Key(s) & Target Parent Table |
| :--- | :--- | :--- | :--- |
| `silver.customers` | `customer_id` | No | None (Root entity) |
| `silver.products` | `product_id` | No | None (Root entity) |
| `silver.orders` | `order_id` | No | `customer_id` $\rightarrow$ points to `silver.customers(customer_id)` |
| `silver.order_items`| `(order_id, order_item_id)` | **Yes** | 1. `order_id` $\rightarrow$ `silver.orders(order_id)`<br>2. `product_id` $\rightarrow$ `silver.products(product_id)` |
| `silver.order_reviews`| `review_id` | No | `order_id` $\rightarrow$ points to `silver.orders(order_id)` |

---

## 📝 Task 3: The Bus Seat Analogy Challenge

1. **Why does setting `PRIMARY KEY (order_id)` fail in PostgreSQL as soon as a customer orders 2 or more items?**  
   *Answer:* A Primary Key strictly enforces uniqueness. If customer `Alice` orders 3 books in `order_001`, `silver.order_items` must store 3 rows. If `order_id` is the primary key, PostgreSQL will throw a duplicate key violation on the 2nd row and abort the transaction.

2. **Why does setting `PRIMARY KEY (order_item_id)` also fail if `order_item_id` is just a sequence number (`1, 2, 3...`)?**  
   *Answer:* `order_item_id` restarts at 1 for every single order. In the entire table, there will be tens of thousands of rows where `order_item_id = 1`. A sequence number alone is not globally unique across orders.

3. **How does the combination `PRIMARY KEY (order_id, order_item_id)` solve this?**  
   *Answer:* Just like the bus ticket (`Bus 42, Seat 12A`), neither number is unique on its own, but the **composite pair** is guaranteed to be 100% unique across the entire database. There can only ever be one Item #1 on Order #42.

---

## 📝 Task 4: The Mystery of the Dashed Line

1. **What does a solid line in an ERD communicate?**  
   *Answer:* A solid line indicates an established, high-certainty relational dependency. In our model, every `order_item` must belong to an `order` and a `product`.

2. **Why did we draw the reviews line as dashed? What real-world business evidence from the raw data justifies this hesitation?**  
   *Answer:* A dashed line indicates an **uncertain or unverified business relationship**. 
   * Many orders are never reviewed by customers (0 reviews).
   * A single order can sometimes receive multiple review survey attempts or updates (>1 review).
   * Some review records in raw data might arrive before an order is formally marked delivered.  
   A professional data engineer uses a dashed line to say: *"Do not build rigid downstream business logic until we query the raw data and prove the exact cardinality."*
