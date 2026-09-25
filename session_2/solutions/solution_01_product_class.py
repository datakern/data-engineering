"""
Solution 1 — Build the Product Class (Encapsulation)
"""

class Product:
    def __init__(self, name: str, price: float, stock: int):
        self.name  = name
        self.price = price
        self.__stock = stock          # private — protected by the shield

    def get_stock(self) -> int:
        return self.__stock

    def reduce_stock(self, quantity: int):
        if quantity < 0:
            raise ValueError("Quantity cannot be negative.")
        if quantity > self.__stock:
            raise ValueError(f"Not enough stock. Available: {self.__stock}")
        self.__stock -= quantity


# ── Verification ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    laptop = Product("Laptop", 999.99, 10)

    laptop.reduce_stock(3)
    print(laptop.get_stock())   # → 7

    try:
        laptop.reduce_stock(-1)
    except ValueError as e:
        print(e)                # → Quantity cannot be negative.

    try:
        laptop.reduce_stock(50)
    except ValueError as e:
        print(e)                # → Not enough stock. Available: 7
