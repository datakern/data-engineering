"""
Solution 2 — Customer Hierarchy (Inheritance & Polymorphism)
"""


class Customer:
    def __init__(self, name: str):
        self.name = name

    def get_discount(self) -> float:
        return 0.0


class PremiumCustomer(Customer):
    def get_discount(self) -> float:
        return 0.10


class CorporateCustomer(Customer):
    def get_discount(self) -> float:
        return 0.20


# ── Provided Order class — unchanged ─────────────────────────────────────────

class Order:
    def calculate_total(self, base_price, customer):
        discount = customer.get_discount()   # no IF statement — polymorphism!
        return base_price * (1 - discount)


# ── Verification ──────────────────────────────────────────────────────────────

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
    # Alice  → 0%  discount → total: €1000.00
    # Bob    → 10% discount → total: €900.00
    # Carol  → 20% discount → total: €800.00
