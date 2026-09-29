# Solution Design Document
# Retail Order Management - Mini Project 1
# DataKern Data Engineering Programme

---

## 1. Problem Statement

A retail company needs a software system to manage customers, products and orders.
The system must:

- Create and store different types of customers (Regular, Premium, Corporate).
- Create and store products with a price and a stock level.
- Create orders that belong to a customer.
- Add products (with a quantity) to an order.
- Calculate the order total and apply a discount based on the customer type.
- Prevent invalid states (negative stock, negative price, impossible quantities).

---

## 2. Identifying Nouns and Actions

**Nouns → Classes:**

| Noun           | Becomes a Class? | Reason                                                              |
|----------------|------------------|---------------------------------------------------------------------|
| Customer       | Yes              | Has an identity, name, email, and a type-specific discount rule.   |
| Product        | Yes              | Has an identity, price, and stock level that changes over time.    |
| Order          | Yes              | A transaction linking a customer to a set of products.             |
| OrderItem      | Yes              | The receipt line. Links one product to a quantity.                 |
| Price          | No               | It is just a number. It belongs *to* a Product.                    |
| Discount       | No               | It is a behaviour of the customer, not an entity on its own.       |

**Actions → Methods:**

| Action              | Lives in Class | Method Name          |
|---------------------|----------------|----------------------|
| Check/reduce stock  | Product        | `reduce_stock()`     |
| Get discount rate   | Customer       | `get_discount_rate()`|
| Add product to order| Order          | `add_item()`         |
| Calculate total     | Order          | `calculate_total()`  |
| Get line total      | OrderItem      | `get_subtotal()`     |

---

## 3. Class Responsibilities

### `Product`
- Knows its own name, price and stock level.
- Protects price and stock from being set to invalid values.
- Provides a controlled method to reduce its own stock.

### `Customer` (base) / `PremiumCustomer` / `CorporateCustomer`
- Each customer knows its own id, name and email.
- Each **type** of customer knows its own discount rate via `get_discount_rate()`.
- The Order class never checks the customer type directly.

### `OrderItem`
- Represents one line on the receipt: one product + a quantity.
- Calculates its own subtotal.
- Validates that quantity > 0.

### `Order`
- Belongs to one customer.
- Holds a list of `OrderItem` objects.
- Manages the `add_item()` flow: validate, reduce stock, create `OrderItem`.
- Calculates the final total using the customer's discount rate (Polymorphism).

---

## 4. Encapsulation Decisions

**Problem:** Python allows any code to do `product.stock = -10`, which would corrupt business logic.

**Solution:** I used Python `@property` decorators to make `_price` and `_stock` private.

```python
@stock.setter
def stock(self, new_stock):
    if new_stock < 0:
        raise ValueError("Stock cannot be negative.")
    self._stock = new_stock
```

I applied the same pattern to `price`. The `reduce_stock(quantity)` method is the only correct way to decrease stock — it validates the quantity is positive AND that there is enough stock before changing anything.

---

## 5. Inheritance Design

```
Customer (base)
├── PremiumCustomer     # 10% discount
└── CorporateCustomer   # 20% discount
```

**Why inheritance?** All three customer types share `customer_id`, `customer_name` and `customer_email`. Rather than repeating those attributes three times, the base `Customer` class holds them and the subclasses inherit them automatically.

**Why not just a `customer_type` string?**  
If I had used `self.type = "Premium"`, the `Order` class would need a growing `if/elif/else` block to decide which discount to apply. Every time the business added a new customer type, someone would have to open `Order` and edit it. Inheritance lets me add a new customer type by simply creating a new class — no existing code needs to change.

---

## 6. Polymorphism Design

**The key method:** `get_discount_rate()`

Each class provides its own version:

| Class               | `get_discount_rate()` returns |
|---------------------|-------------------------------|
| `Customer`          | `0.0`   (no discount)         |
| `PremiumCustomer`   | `0.10`  (10% discount)        |
| `CorporateCustomer` | `0.20`  (20% discount)        |

**Inside `Order.calculate_total()`:**

```python
discount_rate = self.customer.get_discount_rate()   # Polymorphism here
discount_amount = subtotal * discount_rate
final_total = subtotal - discount_amount
```

The `Order` class does not know or care whether `self.customer` is a `Customer`, `PremiumCustomer` or `CorporateCustomer`. It just asks for the discount rate. Each object responds correctly for its own type. This is Polymorphism.

---

## 7. Business Rules Chosen

| Customer Type   | Discount |
|-----------------|----------|
| Regular         | 0%       |
| Premium         | 10%      |
| Corporate       | 20%      |

- Stock is reduced immediately when `add_item()` is called.
- If requested quantity > available stock, a `ValueError` is raised and the item is NOT added.
- A quantity of 0 or negative always raises a `ValueError`.

---

## 8. Design Decisions and Trade-offs

| Decision | Alternative Considered | Why I chose this approach |
|---|---|---|
| Use `@property` for stock/price | Public attribute | `@property` enforces validation without changing how callers access the value |
| `OrderItem` holds `product` object | Store just `product_id` | Storing the full object makes it easy to call `product.price` without a lookup |
| `Order` reduces stock in `add_item()` | Reduce stock only at checkout | Simpler for now; prevents the same stock being sold to two orders simultaneously |
| `CorporateCustomer` as a separate class | Attribute on base class | Allows adding new customer types without touching existing classes |
