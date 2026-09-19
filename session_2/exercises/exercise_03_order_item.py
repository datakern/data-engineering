"""
╔══════════════════════════════════════════════════════════════╗
║  Exercise 03 — The Receipt Line                             ║
║  Topic: Object Composition · line_total() · __repr__        ║
╚══════════════════════════════════════════════════════════════╝

Real-world story
────────────────
Think about a supermarket receipt:

  Apple         €1.00
  ×10
  ─────────────────────
  Line total:   €10.00

The apple on the shelf only knows its name and price.
It does NOT know how many you bought.

You need a "receipt line" object — something that glues
a Product to a quantity. That is your OrderItem.

────────────────────────────────────────────────────────────────
What to build
────────────────────────────────────────────────────────────────
An OrderItem class that:
  ✦ Takes a product (a Product object) and a quantity (int)
  ✦ Has a method  line_total()  → price × quantity
  ✦ Has a  __repr__  so it prints like:
        OrderItem(Laptop x3 = €2999.97)

The Product class is already written for you. Don't change it.

────────────────────────────────────────────────────────────────
Thinking hints  💡
────────────────────────────────────────────────────────────────
  1. To access the product's price inside OrderItem:
     → self.product.price   (one object living inside another)

  2. line_total is just multiplication:
     → self.product.price * self.quantity

  3. __repr__ controls what gets printed when you  print(item)
     → Use an f-string:
        f"OrderItem({self.product.name} x{self.quantity} = €{...:.2f})"

────────────────────────────────────────────────────────────────
Expected output when you run this file
────────────────────────────────────────────────────────────────
  2999.97
  OrderItem(Laptop x3 = €2999.97)
  OrderItem(Mouse x5 = €149.95)
"""

# ── Provided — do NOT change ──────────────────────────────────────────────────

class Product:
    def __init__(self, name: str, price: float):
        self.name  = name
        self.price = price


# ── Write your code here ──────────────────────────────────────────────────────


class OrderItem:
    def __init__(self, product, quantity):
        self.product = product # replace this
        self.quantity = quantity
    def line_total(self):
        return self.product.price * self.quantity
    def __repr__(self):
        return f"OrderItem({self.product.name} x{self.quantity} = €{self.line_total():.2f})"

# ── Test it — do not change anything below this line ─────────────────────────

if __name__ == "__main__":
    laptop = Product("Laptop", 999.99)
    item   = OrderItem(laptop, 3)

    print(item.line_total())   # → 2999.97
    print(item)                # → OrderItem(Laptop x3 = €2999.97)

    mouse = Product("Mouse", 29.99)
    item2 = OrderItem(mouse, 5)
    print(item2)               # → OrderItem(Mouse x5 = €149.95)
