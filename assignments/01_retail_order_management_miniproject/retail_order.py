# retail_order.py
# Student: DataKern - Retail Order Management Mini Project
# Following the design documented in docs/solution_design.md

# -----------------------------------------------------------------------
# Product Class
# Encapsulation: _price and _stock are private to prevent invalid states.
# -----------------------------------------------------------------------

class Product:
    def __init__(self, product_id, product_name, price, stock):
        self.product_id = product_id
        self.product_name = product_name
        self._price = price
        self._stock = stock

    # -- Getters --

    @property
    def price(self):
        return self._price

    @property
    def stock(self):
        return self._stock

    # -- Setters with validation --

    @price.setter
    def price(self, new_price):
        if new_price < 0:
            raise ValueError("Price cannot be negative.")
        self._price = new_price

    @stock.setter
    def stock(self, new_stock):
        if new_stock < 0:
            raise ValueError("Stock cannot be negative.")
        self._stock = new_stock

    def reduce_stock(self, quantity):
        """Reduce stock by quantity. Raises ValueError if not enough stock."""
        if quantity <= 0:
            raise ValueError("Quantity must be a positive number.")
        if quantity > self._stock:
            raise ValueError(
                f"Not enough stock for '{self.product_name}'. "
                f"Requested: {quantity}, Available: {self._stock}"
            )
        self._stock -= quantity

    def display_product_info(self):
        return (
            f"Product ID   : {self.product_id}\n"
            f"Product Name : {self.product_name}\n"
            f"Price        : ${self._price}\n"
            f"Stock        : {self._stock}"
        )


# -----------------------------------------------------------------------
# Customer Hierarchy
# Inheritance: Customer is the base class.
# Polymorphism: each subclass overrides get_discount_rate().
# The Order class never needs to know which subclass it is dealing with.
# -----------------------------------------------------------------------

class Customer:
    """Base customer - no discount by default."""

    def __init__(self, customer_id, customer_name, customer_email):
        self.customer_id = customer_id
        self.customer_name = customer_name
        self.customer_email = customer_email

    def get_discount_rate(self):
        # Regular customers receive no discount
        return 0.0

    def display_customer_info(self):
        return (
            f"Customer ID    : {self.customer_id}\n"
            f"Customer Name  : {self.customer_name}\n"
            f"Customer Email : {self.customer_email}\n"
            f"Customer Type  : Regular\n"
            f"Discount Rate  : {int(self.get_discount_rate() * 100)}%"
        )


class PremiumCustomer(Customer):
    """Premium customers receive a 10% discount."""

    def get_discount_rate(self):
        return 0.10

    def display_customer_info(self):
        return (
            f"Customer ID    : {self.customer_id}\n"
            f"Customer Name  : {self.customer_name}\n"
            f"Customer Email : {self.customer_email}\n"
            f"Customer Type  : Premium\n"
            f"Discount Rate  : {int(self.get_discount_rate() * 100)}%"
        )


class CorporateCustomer(Customer):
    """Corporate customers receive a 20% discount."""

    def get_discount_rate(self):
        return 0.20

    def display_customer_info(self):
        return (
            f"Customer ID    : {self.customer_id}\n"
            f"Customer Name  : {self.customer_name}\n"
            f"Customer Email : {self.customer_email}\n"
            f"Customer Type  : Corporate\n"
            f"Discount Rate  : {int(self.get_discount_rate() * 100)}%"
        )


# -----------------------------------------------------------------------
# OrderItem Class
# Represents one line on the "receipt" - a Product and a Quantity.
# The Order class holds a list of OrderItem objects, not Products directly.
# -----------------------------------------------------------------------

class OrderItem:
    def __init__(self, product, quantity):
        if quantity <= 0:
            raise ValueError("Quantity must be greater than zero.")
        self.product = product
        self.quantity = quantity

    def get_subtotal(self):
        """Return the total cost for this line item."""
        return self.product.price * self.quantity

    def get_order_item_details(self):
        return (
            f"  Product  : {self.product.product_name}\n"
            f"  Price    : ${self.product.price}\n"
            f"  Quantity : {self.quantity}\n"
            f"  Subtotal : ${self.get_subtotal()}"
        )


# -----------------------------------------------------------------------
# Order Class
# Represents the full receipt. Belongs to one Customer.
# Polymorphism is used here: Order asks the customer for its discount rate
# without caring whether the customer is Regular, Premium, or Corporate.
# -----------------------------------------------------------------------

class Order:
    def __init__(self, order_id, customer):
        self.order_id = order_id
        self.customer = customer
        self.items = []  # List of OrderItem objects

    def add_item(self, product, quantity=1):
        """
        Adds an item to the order.
        Also reduces the product's stock immediately.
        Raises ValueError if there is not enough stock or quantity is invalid.
        """
        # Reduce stock first - this also validates quantity and availability
        product.reduce_stock(quantity)

        # Create the OrderItem and add it to the list
        order_item = OrderItem(product, quantity)
        self.items.append(order_item)
        print(f"  Added: {product.product_name} x{quantity} to Order #{self.order_id}")

    def calculate_total(self):
        """
        Calculate the final total after applying the customer's discount.
        Polymorphism: we call get_discount_rate() on the customer object.
        The Order class does not use if/elif to check customer types.
        """
        subtotal = sum(item.get_subtotal() for item in self.items)

        # This line is where Polymorphism happens
        discount_rate = self.customer.get_discount_rate()
        discount_amount = subtotal * discount_rate
        final_total = subtotal - discount_amount

        return subtotal, discount_amount, final_total

    def get_order_details(self):
        print(f"\n{'=' * 45}")
        print(f"  Order ID : {self.order_id}")
        print(f"  Customer : {self.customer.customer_name} ({self.customer.__class__.__name__})")
        print(f"{'=' * 45}")
        if not self.items:
            print("  No items in this order.")
        else:
            for item in self.items:
                print(item.get_order_item_details())
                print(f"  {'-' * 40}")

    def checkout(self):
        """Print the final receipt including subtotal, discount, and final total."""
        if not self.items:
            print("Cannot checkout an empty order.")
            return

        subtotal, discount_amount, final_total = self.calculate_total()
        discount_pct = int(self.customer.get_discount_rate() * 100)

        self.get_order_details()
        print(f"  Subtotal         : ${subtotal:.2f}")
        print(f"  Discount ({discount_pct:>2}%)   : -${discount_amount:.2f}")
        print(f"  Final Total      : ${final_total:.2f}")
        print(f"{'=' * 45}\n")


# -----------------------------------------------------------------------
# Quick Demo / Manual Test Run
# -----------------------------------------------------------------------

if __name__ == "__main__":

    print("\n--- Setting up Products ---\n")
    laptop = Product(product_id="P001", product_name="Laptop", price=1200, stock=10)
    mouse = Product(product_id="P002", product_name="Wireless Mouse", price=45, stock=50)
    keyboard = Product(product_id="P003", product_name="Mechanical Keyboard", price=90, stock=5)
    print(laptop.display_product_info())
    print()

    print("\n--- Setting up Customers ---\n")
    regular = Customer("C001", "Alice Johnson", "alice@email.com")
    premium = PremiumCustomer("C002", "Bob Smith", "bob@email.com")
    corporate = CorporateCustomer("C003", "Acme Corp", "orders@acme.com")

    print(regular.display_customer_info())
    print()
    print(premium.display_customer_info())
    print()
    print(corporate.display_customer_info())

    # --- T001: Regular customer places a normal order ---
    print("\n--- T001: Regular customer order ---")
    order1 = Order(order_id="ORD001", customer=regular)
    order1.add_item(laptop, quantity=1)
    order1.add_item(mouse, quantity=2)
    order1.checkout()

    # --- T002: Premium customer order (10% off) ---
    print("--- T002: Premium customer order ---")
    order2 = Order(order_id="ORD002", customer=premium)
    order2.add_item(laptop, quantity=1)
    order2.add_item(keyboard, quantity=2)
    order2.checkout()

    # --- T003: Corporate customer order (20% off) ---
    print("--- T003: Corporate customer order ---")
    order3 = Order(order_id="ORD003", customer=corporate)
    order3.add_item(mouse, quantity=10)
    order3.checkout()

    # --- T004: Quantity exceeds stock ---
    print("--- T004: Quantity exceeds stock (should raise error) ---")
    try:
        order4 = Order(order_id="ORD004", customer=regular)
        order4.add_item(keyboard, quantity=100)  # Only 1 left in stock after T002
        order4.checkout()
    except ValueError as e:
        print(f"  Caught expected error: {e}\n")

    # --- T005: Invalid quantity (zero) ---
    print("--- T005: Invalid quantity = 0 (should raise error) ---")
    try:
        order5 = Order(order_id="ORD005", customer=regular)
        order5.add_item(mouse, quantity=0)
    except ValueError as e:
        print(f"  Caught expected error: {e}\n")

    # --- T006: Encapsulation - setting negative price ---
    print("--- T006: Set negative price (should raise error) ---")
    try:
        laptop.price = -500
    except ValueError as e:
        print(f"  Caught expected error: {e}\n")