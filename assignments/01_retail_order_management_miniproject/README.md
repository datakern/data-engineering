# Retail Order Management — Mini Project 1
**DataKern Data Engineering Programme**

---

## Problem Statement

A retail company needs a system to manage customers, products and orders.
The system must support three types of customers (Regular, Premium, Corporate), maintain product stock levels, build orders by adding products with quantities, and calculate totals with type-specific discounts.

---

## How to Set Up and Run

```bash
# Clone the repo and navigate to the folder
cd data_engineering_regular_sessions/assignments/retail_order_management_miniproject_1

# (Optional) Create and activate a virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Run the application (runs all built-in test scenarios)
python3 retail_order.py
```

No external packages required. Only the Python standard library is used.

---

## Class Design and Responsibilities

| Class               | Responsibility                                                          |
|---------------------|-------------------------------------------------------------------------|
| `Product`           | Stores name, price, stock. Controls stock changes through `reduce_stock()`. |
| `Customer`          | Base class. Stores id, name, email. Defines `get_discount_rate()` → 0%  |
| `PremiumCustomer`   | Inherits from Customer. Overrides `get_discount_rate()` → 10%           |
| `CorporateCustomer` | Inherits from Customer. Overrides `get_discount_rate()` → 20%           |
| `OrderItem`         | One line on the receipt. Stores a Product reference + quantity.         |
| `Order`             | Belongs to a Customer. Holds a list of OrderItems. Calculates totals.   |

---

## Inheritance Design

```
Customer (base)
├── PremiumCustomer     # 10% discount
└── CorporateCustomer   # 20% discount
```

All three customer types share `customer_id`, `customer_name` and `customer_email`. 
I put those in the base `Customer` class so I don't repeat them. Each subclass then **only** adds what is different: the discount rate. 

Using a separate class for each type also means I can add a new customer type later (e.g., `VIPCustomer`) by just creating a new class — I don't need to modify `Order` or any existing class.

---

## Where Polymorphism Is Used

**Method:** `get_discount_rate()` defined on `Customer`, overridden in `PremiumCustomer` and `CorporateCustomer`.

**Location in `Order.calculate_total()`:**

```python
discount_rate = self.customer.get_discount_rate()   # <-- Polymorphism
discount_amount = subtotal * discount_rate
final_total = subtotal - discount_amount
```

The `Order` class does not contain a single `if/elif` that checks the customer type. It simply calls `get_discount_rate()` on whatever customer object it has. Each object responds with its own correct value. Adding a new customer type in the future requires zero changes to `Order`.

---

## Where Encapsulation Is Used

**In `Product`:** `_price` and `_stock` are private attributes. Access goes through `@property` getters and setters.

```python
@stock.setter
def stock(self, new_stock):
    if new_stock < 0:
        raise ValueError("Stock cannot be negative.")
    self._stock = new_stock
```

This means no other part of the code can accidentally set the stock to a negative value. The only correct way to reduce stock is through `reduce_stock(quantity)`, which validates both the quantity and the available stock before making any change.

**In `OrderItem`:** The constructor rejects a quantity of 0 or less immediately, so an `OrderItem` can never exist in an invalid state.

---

## Business / Discount Rules

| Customer Type   | Discount |
|-----------------|----------|
| Regular         | 0%       |
| Premium         | 10%      |
| Corporate       | 20%      |

- Stock is deducted from the product the moment `add_item()` is called.
- If requested quantity > available stock → `ValueError` is raised, item is NOT added.
- Quantity of 0 or less → `ValueError` is raised.
- Price cannot be set to a negative value → `ValueError` is raised.

---

## How I Tested the Application

I planned 6 test scenarios in `tests/test_data.md` before running any code.
Then I ran `python3 retail_order.py` and recorded actual output vs. expected in `tests/test_results.md`.

All 6 tests passed on the first run after fixing the Order/OrderItem relationship bug (see Problems section below).

---

## Assumptions

- Stock is reduced immediately at `add_item()`, not at checkout. This is simpler and prevents overselling.
- The system runs in memory only. There is no database or file persistence.
- Customer email is stored as a plain string; no format validation is applied.
- The `CorporateCustomer` always gets 20% regardless of order size. A volume-based rule could be added later.

---

## Problems I Encountered and How I Solved Them

**Bug: Order and OrderItem were not connected correctly.**  
When I first wrote the `Order` class, `add_item()` was appending `Product` objects directly to `self.items`. Then `get_order_details()` was calling `item.product_name` and `item.price`, which only exist on `Product`, not on `OrderItem`. This also meant quantity was completely ignored.

**Fix:** I rewrote `add_item()` to create an `OrderItem` instance and append that instead. All loops in `Order` now access `item.product.product_name` and call `item.get_subtotal()`.

**Lesson:** I should have designed on paper (or pseudocode) before coding. Once I drew out the relationship — Order has a list of OrderItems, each OrderItem holds a Product + quantity — the fix was obvious.

---

## Project Structure

```
retail_order_management_miniproject_1/
├── retail_order.py          # Main application file
├── docs/
│   └── solution_design.md   # Design decisions and reasoning
├── tests/
│   ├── test_data.md         # Planned test scenarios
│   └── test_results.md      # Actual test output and pass/fail status
└── README.md
```
