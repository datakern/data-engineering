# Retail Order Management - Detailed Student Thought Process & Reasoning

## 1. Initial Shock and Deconstruction
When I first opened this assignment, I felt a wave of imposter syndrome. Our previous assignment was mostly writing scripts and running SQL queries line by line. Now, I'm being asked to "design a solution" and make "engineering decisions" using Object-Oriented Programming (OOP), without any AI help. 

The instructor explicitly stated: **"Define the solution before you code."** 

Okay, let's take a step back and read the problem statement.
> *"The application should allow the company to create customers, maintain products, create orders, add products to orders, calculate order amounts and apply different business rules depending on the customer type (Regular, Premium, Corporate)."*

### The "Nouns and Actions" Exercise
The assignment tells me to start by identifying nouns and actions to bridge business requirements to software design.

**Nouns I found:**
- Customer (and types: Regular, Premium, Corporate)
- Product
- Order
- Order Amount / Total
- Business Rules / Discount

**Actions I found:**
- Create (Customer, Product, Order)
- Add (Product to Order)
- Calculate (Order Amount)
- Apply (Business Rules / Discount)

This exercise helped me realize that classes aren't just random containers for code. They represent actual things in the business. 

---

## 2. Deciding on the Classes (The "Why")

I can't just make everything a class. I need to justify it.

### Candidate 1: `Product`
- **Why?** A product in a retail store has an identity. It has a name, a price, and critically, a stock level. If stock reaches zero, people can't buy it.
- **Attributes:** `id`, `name`, `price`, `stock`
- **Behaviors:** `check_stock()`, `reduce_stock()`

### Candidate 2: `Customer` (and Subclasses)
- **Why?** An order must belong to someone. But the tricky part is the "three customer types".
- **Reasoning:** I initially thought about just adding a `customer_type` string attribute to a single `Customer` class (e.g., `self.type = "Premium"`). But wait, the assignment mentions *Polymorphism*. If I just use a string attribute, I'll end up with a massive `if/elif/else` block inside my Order class to calculate discounts. That's procedural programming, not OOP.
- **Decision:** I will use **Inheritance**. I'll create a base `Customer` class with common attributes (`id`, `name`). Then I'll create `RegularCustomer`, `PremiumCustomer`, and `CorporateCustomer` that inherit from it. 

### Candidate 3: `Order`
- **Why?** This is the core transaction. It links a Customer to Products.
- **Attributes:** `order_id`, `customer` (this will hold a Customer object, not just a string ID!), `items` (a list).
- **Behaviors:** `add_item()`, `calculate_total()`

### Candidate 4: `OrderItem`
- **Why?** I struggled with this. Why not just put `Product` objects directly into a list in the `Order`? 
- **Reasoning:** Because of *quantity*. If someone buys 5 laptops, I don't want to add the Laptop object to the list 5 times. I need a link between the Product and the Quantity. 
- **Decision:** An `OrderItem` class is necessary. It will have `product` and `quantity` attributes.

---

## 3. Designing Encapsulation (Protecting the Data)

The assignment asks: *"What should happen if someone tries to set a negative price or invalid quantity?"*

If I just write:
```python
laptop = Product(name="Laptop", price=1000, stock=5)
laptop.stock = -10
```
Python will allow it. But my business logic is ruined.

**My Encapsulation Strategy:**
1.  I will make `price` and `stock` private by prefixing them with an underscore (`_price`, `_stock`).
2.  I will write getter and setter methods. In the setter for `stock`, I will add logic:
    ```python
    if new_stock < 0:
        raise ValueError("Stock cannot be negative")
    ```
3.  I will do the same for the quantity in `OrderItem`. You can't order 0 or negative items.

---

## 4. Designing Polymorphism (The "Aha!" Moment)

The requirement: *Apply different business rules depending on the customer type.*

If I put the logic in the `Order` class, it looks like this:
```python
# Bad OOP
if customer.type == "Premium":
    total = total * 0.90
elif customer.type == "Corporate":
    total = total * 0.80
```
This violates OOP principles because if the business adds a "VIP Customer" later, I have to modify the `Order` class. The `Order` class shouldn't care about discount rules.

**My Polymorphic Strategy:**
I will define a method `get_discount_rate()` on every customer class.
- `Customer` (Base): Returns `0.0`
- `PremiumCustomer`: Returns `0.10`
- `CorporateCustomer`: Returns `0.20`

Inside the `Order` class, when I calculate the total, I will just write:
```python
discount = self.customer.get_discount_rate() * subtotal
final_total = subtotal - discount
```
The `Order` class just asks the customer object for its discount rate. It doesn't know or care *which* subclass it is talking to. That is Polymorphism!

---

## 5. Development Strategy & Anticipated Struggles

### Step-by-Step Build
1.  **Environment:** Set up the `.venv` and Git branch (`git switch -c retail-order-management`).
2.  **Product Class:** Write it, encapsulate the stock, and test it in a scratch file.
3.  **Customer Hierarchy:** Write the base and subclasses. Test the polymorphic `get_discount_rate()` method.
4.  **Order Classes:** Write `OrderItem` and `Order`. 
5.  **Integration:** This is where I expect to struggle. Passing objects into other objects is confusing. I have to remember that `order = Order(customer=my_premium_customer)` is passing a *reference in memory*, not a copy of the data.
6.  **Stock Reduction Logic:** When a product is added to an order, does the stock drop immediately? What if the order is cancelled? For simplicity, I'll reduce the stock the moment `add_item()` is called, and raise a `ValueError` if `requested_quantity > product.stock`.
7.  **Testing:** I will manually write out my scenarios in `tests/test_data.md` and run a python script to simulate them, logging the outputs.

### Where I'll probably get stuck:
- **Circular Imports:** If `Order` imports `Product` and `Product` imports `Order`, Python will crash. I need to structure my files carefully (maybe put all models in one `models.py` file for now).
- **The CLI:** Making a text-based menu (`1. Add Customer`, `2. Add Product`, `3. Checkout`) requires an infinite `while True` loop. I need to make sure I manage the list of available products and customers in memory while the loop runs.

By writing all of this down first, the actual coding feels much less scary. I now have a blueprint to follow.
