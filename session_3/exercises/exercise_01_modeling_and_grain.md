# Exercise 1: The Architect — Data Modeling, Grain & Key Selection

![DataKern Watermark](https://img.shields.io/badge/DATAKERN-Exercise-F26522?style=for-the-badge)  
*Session 3: From Validated Files to Analytics-Ready Data*

---

## 🎯 Goal
Before writing a single line of SQL, a data engineer designs the system. In this exercise, you will master the foundational rule of relational modeling:
> **"Grain before schema. Schema before SQL."**

If you don't know the grain of a table, every `COUNT(*)`, `SUM()`, and `JOIN` you write later will produce deceptive, unreliable numbers.

---

## 🧠 Core Mental Models to Recall

1. **Grain:** The physical reality of a single row. The universal sentence frame:
   > *"One row represents one [singular noun] [context/scope]."*
2. **Primary Key (PK):** A column (or set of columns) that uniquely identifies each row. Must pass **3 Tests**:
   - Test 1: Can it ever be `NULL`? (Must be NO)
   - Test 2: Is it unique at the table's grain? (Must be YES)
   - Test 3: Does it represent the entity itself, not a child? (Must be YES)
3. **Composite Key (The Bus Seat Analogy):** 
   - A bus ticket specifies: `Bus 42, Seat 12A`. 
   - Can "Seat 12A" exist on other buses? Yes.
   - Can "Bus 42" have other seats? Yes.
   - Only the **combination** `(Bus, Seat)` identifies a single, unique physical passenger spot.
4. **Foreign Key (FK):** A column in a child table that points back to the Primary Key of a parent table.

---

## 📝 Task 1: Complete the Grain Statements
Write out the precise grain statement for each of the five Olist entities. Be specific about what a single row represents.

*(Replace the blanks below with your answers)*

* **`silver.customers`**  
  *Your grain statement:* One row represents one ________________________________________.

* **`silver.products`**  
  *Your grain statement:* One row represents one ________________________________________.

* **`silver.orders`**  
  *Your grain statement:* One row represents one ________________________________________.

* **`silver.order_items`**  
  *Your grain statement:* One row represents one ________________________________________.

* **`silver.order_reviews`**  
  *Your grain statement:* One row represents one ________________________________________.

---

## 📝 Task 2: The Key Identification Matrix
Fill in the table below to document the keys for each entity in the Silver layer.

| Table Name | Primary Key Column(s) | Is it a Composite Key? (Yes/No) | Foreign Key(s) (Points to Which Parent?) |
| :--- | :--- | :--- | :--- |
| `silver.customers` | `customer_id` | No | None (Root entity) |
| `silver.products` | ____________________ | ______ | ____________________ |
| `silver.orders` | ____________________ | ______ | Points to: ____________________ |
| `silver.order_items`| ____________________ | ______ | Points to: 1. ____________, 2. ____________ |
| `silver.order_reviews`| ____________________ | ______ | Points to: ____________________ |

---

## 📝 Task 3: The Bus Seat Analogy Challenge
In `silver.order_items`, junior developers often try to set `order_id` as the Primary Key.

1. **Why does setting `PRIMARY KEY (order_id)` fail in PostgreSQL as soon as a customer orders 2 or more items?**  
   *Your answer:*

2. **Why does setting `PRIMARY KEY (order_item_id)` also fail if `order_item_id` is just a sequence number (`1, 2, 3...`)?**  
   *Your answer:*

3. **How does the combination `PRIMARY KEY (order_id, order_item_id)` solve this?**  
   *Your answer:*

---

## 📝 Task 4: The Mystery of the Dashed Line
In our Session 3 ERD, the relationship between `silver.orders` and `silver.order_reviews` was drawn with a **dashed line**, unlike the solid line between `orders` and `order_items`.

```
silver.orders  - - - - <  silver.order_reviews
```

1. **What does a solid line in an ERD communicate?**
2. **Why did we draw the reviews line as dashed? What real-world business evidence from the raw data justifies this hesitation?**
   *(Hint: Does every order get reviewed? Can an order have more than one review?)*

---

## ✅ Self-Check Checkpoint
Before checking the solution in `solutions/exercise_01_modeling_and_grain_solution.md`, verify:
- [ ] You did not define the grain of `order_items` as "one order".
- [ ] You identified both components of the composite key in `order_items`.
- [ ] You stated the parent table for every foreign key.
- [ ] You can explain why an engineer never assumes cardinality without verifying data first.
