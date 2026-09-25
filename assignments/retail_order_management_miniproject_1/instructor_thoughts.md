# Instructor Guide: Teaching the Retail Order Management Mini-Project

**Goal of this Session:** Guide students from reading a business requirement to designing an Object-Oriented system. We will not write the final Python code for them, but we will write pseudocode and map out the architecture together.

---

## 1. Setting the Stage: Why are we doing this?
**Instructor Script:** 
"Welcome everyone. Today we are looking at your first mini-project. Up until now, you've written scripts that run from top to bottom. Today, we are building a *system*. A system has parts that talk to each other. 
Before you open VS Code, close your laptops. We are going to design this system on the whiteboard first. In the real world, coding is the *last* thing you do. Designing is the first."

## 2. Nouns and Actions (Whiteboard Exercise)
**Activity:** Read the problem statement out loud. Ask the class to shout out the nouns (entities) and verbs (actions). 

*Write on board:*
*   **Nouns:** Customer, Order, Product, Price, Stock, Premium Customer.
*   **Verbs:** Add to cart, Calculate total, Apply discount.

**Instructor Script:**
"Not every noun becomes a class. Does a 'Price' need its own class? No, it's just a number. It belongs *to* a Product. But does a 'Product' need a class? Yes! It has an identity, a price, and a stock level that changes."

## 3. Designing the `Product` (Teaching Encapsulation)

**Instructor Script:**
"Let's design the Product class. What does a product need to know about itself?"
*(Elicit: name, price, stock)*

"Let's write some pseudocode."

```python
# Pseudocode - Product Class
Class Product:
    Function __init__(name, price, stock):
        self.name = name
        self.price = price
        self.stock = stock
```

"Now, what happens if another part of our code does this?"
```python
my_laptop = Product("Laptop", 1000, 5)
my_laptop.stock = -10  # Oh no!
```

**Teaching Point (Encapsulation):**
"This is why we need Encapsulation. We must protect the object from being put into an invalid state. We do this by making the attribute private and controlling how it changes."

```python
# Pseudocode - Better Product Class
Class Product:
    Function __init__(name, price, stock):
        self.name = name
        self.price = price
        self._stock = stock  # Private!

    Function reduce_stock(quantity):
        If quantity > self._stock:
            Throw Error("Not enough stock!")
        Else:
            self._stock = self._stock - quantity
```
"Now, the only way to change stock is through our controlled method."

## 4. Designing the `Customer` (Teaching Inheritance & Polymorphism)

**Instructor Script:**
"We have three types of customers: Regular, Premium, Corporate. They all get different discounts. How do we design this?"

*(Let students suggest ideas. Someone will likely suggest a giant IF statement).*

"Let's look at the IF statement approach:"
```python
# The Procedural Way (Avoid)
If customer_type == "Premium":
    discount = 0.10
Elif customer_type == "Corporate":
    discount = 0.20
Else:
    discount = 0.0
```
"Why is this bad? Because if our marketing team introduces a 'VIP Customer' tomorrow, we have to find this code and modify it. It breaks the Open/Closed Principle."

**Teaching Point (Inheritance & Polymorphism):**
"Instead, let's use Inheritance. We create a parent class `Customer`, and child classes that *inherit* from it. We use Polymorphism to let each child calculate its *own* discount."

```python
# Pseudocode - Polymorphic Customers
Class Customer (Base):
    Function __init__(id, name):
        self.id = id
        self.name = name
    
    Function get_discount_rate():
        Return 0.0  # Default no discount

Class PremiumCustomer(Inherits from Customer):
    Function get_discount_rate():
        Return 0.10  # 10% off

Class CorporateCustomer(Inherits from Customer):
    Function get_discount_rate():
        Return 0.20  # 20% off
```
"Notice how clean this is? If we add a VIP customer tomorrow, we just create a new class. We don't touch the existing code!"

## 5. Designing the `Order` and `OrderItem` (The Supermarket Analogy)

**Instructor Script:**
"Now for the hardest part. The Order. An order isn't just data. It links a Customer to Products. But how exactly? Let's use a real-world analogy: **A Supermarket Receipt.**"

"When you buy 10 Apples, the physical receipt doesn't print the word 'Apple' 10 times. It prints one line: 'Apple x10 = $10'. This is exactly how our 4 main classes relate to each other:"

1. **The Physical Apple on the Shelf:** This is our `Product` class. It only knows its name and price. It doesn't know how many you are buying.
2. **The Shopper:** This is our `Customer` class. 
3. **The Single Line on the Receipt ('Apple x10'):** This is our `OrderItem` class. It glues a specific `Product` to a `Quantity`.
4. **The Entire Piece of Paper (The Receipt):** This is our `Order` class. It holds a list of `OrderItem` lines and belongs to the `Customer`.

"Let's look at the pseudocode for the Receipt Line (`OrderItem`):"

```python
# Pseudocode - Order Item
Class OrderItem:
    Function __init__(product_object, quantity):
        self.product = product_object
        self.quantity = quantity
    
    Function get_subtotal():
        Return self.product.price * self.quantity
```

"Finally, let's build the `Order`. Watch how Polymorphism makes the total calculation incredibly simple."

```python
# Pseudocode - Order
Class Order:
    Function __init__(customer_object):
        self.customer = customer_object
        self.items = []  # List of OrderItems

    Function add_item(product_object, quantity):
        # 1. Ask the product to reduce its stock
        product_object.reduce_stock(quantity)
        
        # 2. Create the item and add to list
        new_item = OrderItem(product_object, quantity)
        self.items.append(new_item)

    Function calculate_total():
        subtotal = 0
        For item in self.items:
            subtotal = subtotal + item.get_subtotal()
        
        # MAGIC HAPPENS HERE (Polymorphism)
        # We don't know what type of customer this is, and we don't care!
        discount_rate = self.customer.get_discount_rate() 
        discount_amount = subtotal * discount_rate
        
        Return subtotal - discount_amount
```

## 6. Closing the Session
**Instructor Script:**
"Do you see how the Order class doesn't have a single `if` statement checking if the customer is Premium or Corporate? That is the power of Object-Oriented Design. The objects talk to each other through defined interfaces. 

Your assignment is to take these pseudocode concepts, turn them into real Python code, handle errors gracefully (what if stock goes below zero?), write test cases, and document your decisions. 

Go build it, make mistakes, read the errors, and learn!"
