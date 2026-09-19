"""
╔══════════════════════════════════════════════════════════════╗
║  Exercise 01 — The Product Blueprint                        ║
║  Topic: Classes · Private Attributes · Validation           ║
╚══════════════════════════════════════════════════════════════╝

Real-world story
────────────────
Imagine you're building software for an online store.
Every product on the shelf has a name, a price, and a stock count.

The problem: if any part of your code can freely set stock to -50,
your store will happily sell laptops you don't have. 💥

Your job: build a Product class that *protects itself*.

────────────────────────────────────────────────────────────────
What to build
────────────────────────────────────────────────────────────────
A Product class with:
  ✦ name        — a string  (e.g. "Laptop")
  ✦ price       — a float   (e.g. 999.99)
  ✦ __stock     — a PRIVATE int (the double underscore makes it private)

Two methods:
  ✦ get_stock()           → returns the current stock number
  ✦ reduce_stock(qty)     → subtracts qty from stock,
                            but ONLY if qty is valid

────────────────────────────────────────────────────────────────
Thinking hints  💡
────────────────────────────────────────────────────────────────
  1. How do you make an attribute private in Python?
     → Start the name with two underscores:  self.__stock

  2. reduce_stock should refuse bad inputs. Ask yourself:
     → What if someone passes -5?   (qty cannot be negative)
     → What if someone asks for 100 but only 10 are left?

  3. To raise an error in Python:
     → raise ValueError("your message here")

────────────────────────────────────────────────────────────────
Expected output when you run this file
────────────────────────────────────────────────────────────────
  7
  Quantity cannot be negative.
  Not enough stock. Available: 7
"""

# ── Write your code here ──────────────────────────────────────────────────────

class Product:
    pass  # replace this


# ── Test it — do not change anything below this line ─────────────────────────

if __name__ == "__main__":
    laptop = Product("Laptop", 999.99, 10)

    laptop.reduce_stock(3)
    print(laptop.get_stock())        # → 7

    try:
        laptop.reduce_stock(-1)
    except ValueError as e:
        print(e)                     # → Quantity cannot be negative.

    try:
        laptop.reduce_stock(50)
    except ValueError as e:
        print(e)                     # → Not enough stock. Available: 7
