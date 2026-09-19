"""
╔══════════════════════════════════════════════════════════════╗
║  Exercise 02 — The Customer Family Tree                     ║
║  Topic: Inheritance · Polymorphism · No IF statements       ║
╚══════════════════════════════════════════════════════════════╝

Real-world story
────────────────
Your store has three types of customers:
  • Regular    → no discount
  • Premium    → 10% discount
  • Corporate  → 20% discount

The WRONG way: write a giant IF/ELIF block inside the Order class.
The RIGHT way:  let each Customer type know their own discount.
                The Order just asks — it never needs to check who they are.

This is Polymorphism: same question, different answers.

────────────────────────────────────────────────────────────────
What to build
────────────────────────────────────────────────────────────────
Three classes:
  ✦ Customer(name)          → get_discount() returns 0.0
  ✦ PremiumCustomer(name)   → get_discount() returns 0.10
  ✦ CorporateCustomer(name) → get_discount() returns 0.20

The Order class is already written for you below.
Notice: it has NO if/elif — make it work without touching it.

────────────────────────────────────────────────────────────────
Thinking hints  💡
────────────────────────────────────────────────────────────────
  1. To inherit from a parent class:
     class PremiumCustomer(Customer):   ← Customer is the parent

  2. To override a method in the child, just redefine it with the
     exact same name: def get_discount(self):

  3. The __init__ in Customer already stores self.name.
     Child classes can either call  super().__init__(name)
     or just define their own.

────────────────────────────────────────────────────────────────
Expected output when you run this file
────────────────────────────────────────────────────────────────
  Alice  → 0%  discount → total: €1000.00
  Bob    → 10% discount → total: €900.00
  Carol  → 20% discount → total: €800.00
"""

# ── Provided — do NOT change ──────────────────────────────────────────────────

class Order:
    def calculate_total(self, base_price, customer):
        discount = customer.get_discount()   # polymorphism in action — no IF!
        return base_price * (1 - discount)


# ── Write your code here ──────────────────────────────────────────────────────

class Customer:
    def __init__(self, name):
        self.name=name
    def get_discount(self):
        return 0.0
class PremiumCustomer(Customer):
    def get_discount(self):
        return 0.10
class CorporateCustomer(Customer):
    def get_discount(self):
        return 0.20
# ── Test it — do not change anything below this line ─────────────────────────

if __name__ == "__main__":
    customers = [
        Customer("Alice"),
        PremiumCustomer("Bob"),
        CorporateCustomer("Carol"),
    ]
    order = Order()
    for c in customers:
        total = order.calculate_total(1000, c)
        print(f"{c.name:6s} → {c.get_discount()*100:.0f}% discount → total: €{total:.2f}")
