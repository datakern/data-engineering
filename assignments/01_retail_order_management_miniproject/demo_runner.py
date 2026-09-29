"""
demo_runner.py
--------------
Instructor demo script for the Retail Order Management Mini Project.
Run this file in class: python3 demo_runner.py

Each scenario pauses and waits for you to press Enter.
This gives you time to explain before moving to the next step.
"""

import sys
import os

# Make sure we can import from retail_order.py in the same folder
sys.path.insert(0, os.path.dirname(__file__))

from retail_order import Product, Customer, PremiumCustomer, CorporateCustomer, Order


# ───────────────────────────────────────────────
# Helper utilities for clean terminal output
# ───────────────────────────────────────────────

def clear():
    os.system("clear" if os.name == "posix" else "cls")

def pause(label=""):
    if label:
        print(f"\n  ⏸  {label}")
    input("\n  [ Press Enter to continue... ]\n")

def banner(title, concept=""):
    print("\n" + "═" * 55)
    print(f"  🎯  {title}")
    if concept:
        print(f"  📘  Concept: {concept}")
    print("═" * 55)

def section(text):
    print(f"\n  ── {text}")


# ───────────────────────────────────────────────
# Shared test data — set up once, used across demos
# ───────────────────────────────────────────────

def setup():
    laptop   = Product("P001", "Laptop",               price=1200, stock=10)
    mouse    = Product("P002", "Wireless Mouse",        price=45,   stock=50)
    keyboard = Product("P003", "Mechanical Keyboard",   price=90,   stock=5)

    regular   = Customer("C001",  "Alice Johnson", "alice@email.com")
    premium   = PremiumCustomer("C002", "Bob Smith",   "bob@email.com")
    corporate = CorporateCustomer("C003", "Acme Corp",  "orders@acme.com")

    return laptop, mouse, keyboard, regular, premium, corporate


# ═══════════════════════════════════════════════
# SCENARIO 1 — Objects and Classes
# ═══════════════════════════════════════════════

def demo_01_objects(laptop, mouse, keyboard, regular, premium, corporate):
    clear()
    banner("DEMO 1 — Creating Objects from Classes", "Classes & Objects")

    section("These are our Product objects:")
    print()
    print(laptop.display_product_info())
    print()
    print(mouse.display_product_info())
    print()
    print(keyboard.display_product_info())

    pause("A class is a blueprint. Each product above is a separate OBJECT created from the same Product class.")

    section("These are our Customer objects (three different types):")
    print()
    print(regular.display_customer_info())
    print()
    print(premium.display_customer_info())
    print()
    print(corporate.display_customer_info())

    pause("Notice: all three have customer_id, customer_name, customer_email. But the TYPE is different. This leads us to Inheritance...")


# ═══════════════════════════════════════════════
# SCENARIO 2 — Inheritance
# ═══════════════════════════════════════════════

def demo_02_inheritance(regular, premium, corporate):
    clear()
    banner("DEMO 2 — Inheritance", "Inheritance")

    section("What class is each customer?")
    print()
    print(f"  regular   → {type(regular).__name__}")
    print(f"  premium   → {type(premium).__name__}")
    print(f"  corporate → {type(corporate).__name__}")

    pause("PremiumCustomer and CorporateCustomer INHERIT from Customer.\nThey automatically get customer_id, customer_name, customer_email — no code duplication.")

    section("Checking if PremiumCustomer IS a Customer (isinstance):")
    print()
    print(f"  isinstance(premium, Customer)    → {isinstance(premium, Customer)}")
    print(f"  isinstance(regular, PremiumCustomer) → {isinstance(regular, PremiumCustomer)}")

    pause("PremiumCustomer IS a Customer (it inherits from it).\nBut a regular Customer is NOT a PremiumCustomer.")


# ═══════════════════════════════════════════════
# SCENARIO 3 — Polymorphism
# ═══════════════════════════════════════════════

def demo_03_polymorphism(regular, premium, corporate):
    clear()
    banner("DEMO 3 — Polymorphism", "Polymorphism")

    section("Calling get_discount_rate() on each customer type:")
    print()
    print(f"  regular.get_discount_rate()    → {regular.get_discount_rate()}")
    print(f"  premium.get_discount_rate()    → {premium.get_discount_rate()}")
    print(f"  corporate.get_discount_rate()  → {corporate.get_discount_rate()}")

    pause("Same method name. Three different objects. Three different results.\nThis is Polymorphism — 'many forms'.")

    section("How Order uses this (no if/elif anywhere):")
    print()
    print("  discount_rate = self.customer.get_discount_rate()  # <── Polymorphism")
    print("  discount      = subtotal * discount_rate")
    print("  final_total   = subtotal - discount")
    print()
    print("  The Order class does NOT know or care if the customer is Regular, Premium or Corporate.")
    print("  It just asks for the discount rate. Each object answers correctly for its own type.")

    pause("If we add a VIPCustomer tomorrow, we just create a new class.\nWe do NOT change Order at all. That's the power of Polymorphism.")


# ═══════════════════════════════════════════════
# SCENARIO 4 — Encapsulation
# ═══════════════════════════════════════════════

def demo_04_encapsulation(laptop):
    clear()
    banner("DEMO 4 — Encapsulation", "Encapsulation")

    section("Without encapsulation, Python lets anyone do this:")
    print()
    print("  laptop._stock = -999   ← Python won't stop you")
    print("  laptop._price = -500   ← Python won't stop you")
    print()
    print("  This is a BIG problem. The object is now in an INVALID state.")

    pause("With @property setters, we control access. Let's try setting an invalid price...")

    section("Trying to set a negative price:")
    print()
    try:
        laptop.price = -500
    except ValueError as e:
        print(f"  ❌  ValueError caught → {e}")

    pause("The setter blocked the invalid value. self._price was NOT changed.")

    section(f"  Price is still: ${laptop.price}")
    print()
    print("  Only valid values can get through the setter.")

    pause()


# ═══════════════════════════════════════════════
# SCENARIO 5 — Regular customer order (happy path)
# ═══════════════════════════════════════════════

def demo_05_regular_order(laptop, mouse, regular):
    clear()
    banner("DEMO 5 — Regular Customer Order (Happy Path)", "Order + OrderItem")

    section("Creating an Order for Alice (Regular customer)...")
    print()
    order = Order("ORD001", regular)

    pause("Order object created. self.items = [] (empty list of OrderItems)")

    section("Adding Laptop x1 and Mouse x2...")
    print()
    order.add_item(laptop, quantity=1)
    order.add_item(mouse, quantity=2)

    pause(f"Stock updated. Laptop stock is now {laptop.stock}. Mouse stock is now {mouse.stock}.")

    section("Checkout:")
    print()
    order.checkout()

    pause("Regular customer: 0% discount. Final total = Subtotal.")


# ═══════════════════════════════════════════════
# SCENARIO 6 — Premium customer (10% discount)
# ═══════════════════════════════════════════════

def demo_06_premium_order(laptop, keyboard, premium):
    clear()
    banner("DEMO 6 — Premium Customer Order (10% Discount)", "Polymorphism in action")

    section("Creating an Order for Bob (Premium customer)...")
    print()
    order = Order("ORD002", premium)
    order.add_item(laptop, quantity=1)
    order.add_item(keyboard, quantity=2)
    print()
    order.checkout()

    pause("See the 10% discount? Same Order code, different customer type → different result.\nThat's Polymorphism working live.")


# ═══════════════════════════════════════════════
# SCENARIO 7 — Corporate customer (20% discount)
# ═══════════════════════════════════════════════

def demo_07_corporate_order(mouse, corporate):
    clear()
    banner("DEMO 7 — Corporate Customer Order (20% Discount)", "Polymorphism in action")

    section("Creating an Order for Acme Corp (Corporate customer)...")
    print()
    order = Order("ORD003", corporate)
    order.add_item(mouse, quantity=10)
    print()
    order.checkout()

    pause("20% discount applied. Same checkout() method, same Order class.")


# ═══════════════════════════════════════════════
# SCENARIO 8 — Stock validation (failure case)
# ═══════════════════════════════════════════════

def demo_08_stock_error(keyboard, regular):
    clear()
    banner("DEMO 8 — Stock Validation (Failure Scenario)", "Error Handling + Encapsulation")

    section(f"Current Keyboard stock: {keyboard.stock}")
    print()
    section("Now trying to order 100 Keyboards...")
    print()
    try:
        order = Order("ORD004", regular)
        order.add_item(keyboard, quantity=100)
    except ValueError as e:
        print(f"  ❌  ValueError caught → {e}")

    pause(f"Stock is still {keyboard.stock}. The error was raised BEFORE any state change.")


# ═══════════════════════════════════════════════
# SCENARIO 9 — Invalid quantity
# ═══════════════════════════════════════════════

def demo_09_invalid_quantity(mouse, regular):
    clear()
    banner("DEMO 9 — Invalid Quantity = 0 (Failure Scenario)", "Error Handling")

    section("Trying to add Mouse with quantity = 0...")
    print()
    try:
        order = Order("ORD005", regular)
        order.add_item(mouse, quantity=0)
    except ValueError as e:
        print(f"  ❌  ValueError caught → {e}")

    pause("Business rule: you cannot add 0 items to an order. Validation is in reduce_stock().")


# ═══════════════════════════════════════════════
# MAIN — Run all demos in order
# ═══════════════════════════════════════════════

if __name__ == "__main__":
    clear()
    print("\n" + "═" * 55)
    print("  🏪  Retail Order Management — Live Demo")
    print("  DataKern Data Engineering Programme")
    print("═" * 55)
    print()
    print("  This demo walks through 9 scenarios, one at a time.")
    print("  Press Enter after each to move forward.")
    print("  Press Ctrl+C at any time to stop.")
    pause()

    laptop, mouse, keyboard, regular, premium, corporate = setup()

    demo_01_objects(laptop, mouse, keyboard, regular, premium, corporate)
    demo_02_inheritance(regular, premium, corporate)
    demo_03_polymorphism(regular, premium, corporate)
    demo_04_encapsulation(laptop)

    # Re-setup fresh products for order demos (stock was not changed yet for laptop/mouse)
    laptop, mouse, keyboard, regular, premium, corporate = setup()

    demo_05_regular_order(laptop, mouse, regular)
    demo_06_premium_order(laptop, keyboard, premium)
    demo_07_corporate_order(mouse, corporate)
    demo_08_stock_error(keyboard, regular)
    demo_09_invalid_quantity(mouse, regular)

    clear()
    banner("All demos complete! ✅")
    print()
    print("  Concepts demonstrated:")
    print("  ✅  Classes & Objects")
    print("  ✅  Inheritance")
    print("  ✅  Polymorphism")
    print("  ✅  Encapsulation")
    print("  ✅  Error Handling")
    print()
    print("  Files to explore:")
    print("  • retail_order.py        — the implementation")
    print("  • docs/solution_design.md — design decisions")
    print("  • tests/test_results.md  — all test outcomes")
    print()
