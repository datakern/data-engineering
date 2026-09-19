"""
╔══════════════════════════════════════════════════════════════╗
║  Exercise 09 — Mastering Dictionaries                        ║
║  Topic: Dictionaries · Key-Value Pairs · Lookup · Update     ║
╚══════════════════════════════════════════════════════════════╝

Real-world story
────────────────
You are building a product catalogue for an online store.
Each product has a name, a price, and a stock count.

Instead of storing this as three separate lists (messy!),
a Dictionary lets you bundle all related data together under one label.

────────────────────────────────────────────────────────────────
What to explore
────────────────────────────────────────────────────────────────
Work through each section below. Uncomment one block at a time,
run the file, observe the output, and then move to the next block.

────────────────────────────────────────────────────────────────
SECTION 1 — Creating a dictionary and accessing values
────────────────────────────────────────────────────────────────
"""

# ✅ POSITIVE: Access values by their key
# product = {
#     "name": "Laptop",
#     "price": 999.99,
#     "stock": 50,
#     "in_stock": True
# }
#
# print("Full product:", product)
# print("Name:", product["name"])        # Access using the key
# print("Price:", product["price"])
# print("In stock?", product["in_stock"])


"""
────────────────────────────────────────────────────────────────
SECTION 2 — Adding, updating, and removing keys
────────────────────────────────────────────────────────────────
"""

# ✅ POSITIVE: Dictionaries are mutable — you can change them
# product = {"name": "Laptop", "price": 999.99, "stock": 50}
# print("Before:", product)
#
# # Update an existing key (overwrite its value)
# product["price"] = 899.99
# print("After price update:", product)
#
# # Add a brand new key
# product["category"] = "Electronics"
# print("After adding category:", product)
#
# # Remove a key entirely
# del product["stock"]
# print("After deleting stock:", product)


"""
────────────────────────────────────────────────────────────────
SECTION 3 — Duplicate keys: what actually happens?
────────────────────────────────────────────────────────────────
"""

# ✅ / ⚠️ OBSERVE: Duplicate keys silently overwrite — no error is raised!
# product = {
#     "name": "Laptop",
#     "price": 999.99,
#     "name": "Gaming Laptop"    # <--- duplicate key!
# }
# print("Result:", product)
# print("Name:", product["name"])
#
# # What did Python do?
# # It kept ONLY the last value for "name". The first one is gone.
# # This is a silent bug — Python will NOT warn you!


"""
────────────────────────────────────────────────────────────────
SECTION 4 — Looping through a dictionary
────────────────────────────────────────────────────────────────
"""

# ✅ POSITIVE: Three different ways to loop
# product = {"name": "Laptop", "price": 999.99, "stock": 50, "category": "Electronics"}
#
# print("--- Loop over keys only ---")
# for key in product:
#     print(key)
#
# print("--- Loop over values only ---")
# for value in product.values():
#     print(value)
#
# print("--- Loop over both key AND value ---")
# for key, value in product.items():
#     print(f"  {key}: {value}")


"""
────────────────────────────────────────────────────────────────
SECTION 5 — Safe lookup with .get()
────────────────────────────────────────────────────────────────
"""

# ✅ POSITIVE: Use .get() to avoid crashes when a key might not exist
# product = {"name": "Laptop", "price": 999.99}
#
# # Direct access — CRASHES if key is missing
# # print(product["discount"])   # KeyError!
#
# # Safe access with .get() — returns None by default, or a fallback value
# print(product.get("discount"))             # Returns None (no crash!)
# print(product.get("discount", 0))          # Returns 0 if key is missing
# print(product.get("name", "Unknown"))      # Returns "Laptop" (key exists)


"""
────────────────────────────────────────────────────────────────
SECTION 6 — A list of dictionaries (very common in data work!)
────────────────────────────────────────────────────────────────
"""

# ✅ POSITIVE: Real data often looks like this
# products = [
#     {"name": "Laptop",   "price": 999.99, "stock": 50},
#     {"name": "Mouse",    "price": 29.99,  "stock": 200},
#     {"name": "Monitor",  "price": 299.99, "stock": 75},
# ]
#
# print("All products:")
# for product in products:
#     print(f"  {product['name']} costs ${product['price']}, stock: {product['stock']}")
#
# # Calculate total stock value
# total_value = 0
# for product in products:
#     total_value += product["price"] * product["stock"]
# print(f"\nTotal inventory value: ${total_value:,.2f}")


"""
────────────────────────────────────────────────────────────────
SECTION 7 — NEGATIVE SCENARIOS (What can go wrong?)
────────────────────────────────────────────────────────────────
"""

# NEGATIVE 1: Accessing a key that doesn't exist
# product = {"name": "Laptop", "price": 999.99}
# print(product["discount"])   # KeyError: 'discount'
# Fix: use product.get("discount", 0) instead


# NEGATIVE 2: Duplicate keys silently replace data (no warning!)
# order = {"order_id": "ORD001", "product": "Laptop", "order_id": "ORD999"}
# print(order["order_id"])  # Prints "ORD999" — the first value is GONE
# Tip: Always check your data for duplicate keys!


# NEGATIVE 3: Keys must be immutable types (strings, numbers, tuples)
# bad_dict = {["Laptop", "Mouse"]: 100}  # TypeError! Lists cannot be dictionary keys


"""
────────────────────────────────────────────────────────────────
BONUS CHALLENGE
────────────────────────────────────────────────────────────────
Start with this list of orders:

    orders = [
        {"order_id": "ORD001", "product": "Laptop",   "quantity": 2, "price": 999.99},
        {"order_id": "ORD002", "product": "Mouse",    "quantity": 5, "price": 29.99},
        {"order_id": "ORD003", "product": "Monitor",  "quantity": 1, "price": 299.99},
        {"order_id": "ORD004", "product": "Keyboard", "quantity": 3, "price": 49.99},
    ]

1. Print each order_id and its total cost (quantity * price)
2. Find and print the order with the highest total cost
3. Add a new key "total" to each order dictionary with the computed total
4. Print the final updated list of orders

────────────────────────────────────────────────────────────────
"""

# Your bonus code here:
orders = [
        {"order_id": "ORD001", "product": "Laptop",   "quantity": 2, "price": 999.99},
        {"order_id": "ORD002", "product": "Mouse",    "quantity": 5, "price": 29.99},
        {"order_id": "ORD003", "product": "Monitor",  "quantity": 1, "price": 299.99},
        {"order_id": "ORD004", "product": "Keyboard", "quantity": 3, "price": 49.99},
    ]
for order in orders:
    total=order["quantity"]*order["price"]
    print(order["order_id"],"Total:",total)
highest_total=0
for order in orders:
    total=order["quantity"]*order["price"]
    if total>highest_total:
        highest_total=total
        highest_order_id=order["order_id"]
print("Highest total order:",highest_order_id)
print("Highest total:",highest_total)
for order in orders:
    total=order["quantity"]*order["price"]
    order["total"]=total
print("Final orders:")
for order in orders:
    print(order)