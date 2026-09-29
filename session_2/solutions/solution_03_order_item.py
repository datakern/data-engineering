"""
Solution 3 — OrderItem: The Receipt Line
"""


class Product:
    def __init__(self, name: str, price: float):
        self.name  = name
        self.price = price


class OrderItem:
    def __init__(self, product: Product, quantity: int):
        self.product  = product
        self.quantity = quantity

    def line_total(self) -> float:
        return self.product.price * self.quantity

    def __repr__(self) -> str:
        return (
            f"OrderItem({self.product.name} x{self.quantity} "
            f"= €{self.line_total():.2f})"
        )


# ── Verification ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    laptop = Product("Laptop", 999.99)
    item   = OrderItem(laptop, 3)

    print(item.line_total())   # → 2999.97
    print(item)                # → OrderItem(Laptop x3 = €2999.97)

    mouse = Product("Mouse", 29.99)
    item2 = OrderItem(mouse, 5)
    print(item2)               # → OrderItem(Mouse x5 = €149.95)
