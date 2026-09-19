"""
╔══════════════════════════════════════════════════════════════╗
║  Exercise 08 — Mastering Sets                                ║
║  Topic: Sets · Uniqueness · Unordered · Membership Testing   ║
╚══════════════════════════════════════════════════════════════╝

Real-world story
────────────────
You are building a system to track which product categories
exist in a dataset of 25,000 orders.

The problem: the raw data has lots of repeated category names.
You don't care about duplicates — you just want to know
WHICH unique categories exist.

A Python Set is perfect for this — it automatically removes duplicates.

────────────────────────────────────────────────────────────────
What to explore
────────────────────────────────────────────────────────────────
Work through each section below. Uncomment one block at a time,
run the file, observe the output, and then move to the next block.

────────────────────────────────────────────────────────────────
SECTION 1 — Creating a set and observing uniqueness
────────────────────────────────────────────────────────────────
"""

# ✅ POSITIVE: Notice how duplicates are automatically removed
# raw_categories = ["Laptop", "Mouse", "Laptop", "Keyboard", "Mouse", "Mouse", "Desk"]
# print("Raw list (with duplicates):", raw_categories)
#
# unique_categories = set(raw_categories)
# print("Set (unique only):", unique_categories)
# print("Total unique categories:", len(unique_categories))


"""
────────────────────────────────────────────────────────────────
SECTION 2 — Adding and removing from a set
────────────────────────────────────────────────────────────────
"""

# ✅ POSITIVE: add() and discard()
# categories = {"Laptop", "Mouse", "Keyboard"}
# print("Before:", categories)
#
# categories.add("Monitor")       # Add a new item
# print("After adding Monitor:", categories)
#
# categories.add("Laptop")        # Add a DUPLICATE — nothing changes!
# print("After adding Laptop (duplicate):", categories)
#
# categories.discard("Mouse")     # Remove safely (no error if not found)
# print("After discarding Mouse:", categories)


"""
────────────────────────────────────────────────────────────────
SECTION 3 — Traversing a set one by one
────────────────────────────────────────────────────────────────
"""

# ✅ POSITIVE: You CAN loop through a set, but ORDER is not guaranteed!
# categories = {"Laptop", "Mouse", "Keyboard", "Monitor", "Desk"}
#
# print("Looping through the set:")
# for category in categories:
#     print(" -", category)
#
# print()
# print("Run this a few times. Does the order ever change?")
# print("That is because sets are UNORDERED!")


"""
────────────────────────────────────────────────────────────────
SECTION 4 — Membership testing (sets are very fast at this!)
────────────────────────────────────────────────────────────────
"""

# ✅ POSITIVE: 'in' keyword is the most common use case for sets
# approved_products = {"Laptop", "Monitor", "Keyboard", "Mouse", "Desk", "Chair"}
# order_product = "Laptop"
#
# if order_product in approved_products:
#     print(f"{order_product} is an approved product. Processing order...")
# else:
#     print(f"{order_product} is NOT approved. Rejecting order.")
#
# # Try changing order_product to "Headphones" and see what happens


"""
────────────────────────────────────────────────────────────────
SECTION 5 — Set operations (Union, Intersection, Difference)
────────────────────────────────────────────────────────────────
"""

# ✅ POSITIVE: Combine or compare two sets
# online_orders = {"Laptop", "Mouse", "Chair"}
# in_store_orders = {"Chair", "Desk", "Monitor"}
#
# # Union: All products ordered from either channel
# print("All products (union):", online_orders | in_store_orders)
#
# # Intersection: Products ordered from BOTH channels
# print("Ordered from both (intersection):", online_orders & in_store_orders)
#
# # Difference: Products ordered online but NOT in store
# print("Online only (difference):", online_orders - in_store_orders)


"""
────────────────────────────────────────────────────────────────
SECTION 6 — NEGATIVE SCENARIOS (What can go wrong?)
────────────────────────────────────────────────────────────────
"""

# NEGATIVE 1: Sets have NO index — you cannot access items by position
# categories = {"Laptop", "Mouse", "Keyboard"}
# print(categories[0])   # TypeError! Sets don't support indexing


# NEGATIVE 2: remove() raises an error if item not found (use discard() instead)
# categories = {"Laptop", "Mouse"}
# categories.remove("Headphones")  # KeyError! "Headphones" is not in the set
#
# # Safe alternative:
# categories.discard("Headphones")  # No error — silently does nothing
# print("After safe discard:", categories)


# NEGATIVE 3: You cannot put a mutable type (like a list) inside a set
# bad_set = {["Laptop", "Mouse"], "Keyboard"}  # TypeError! Lists are not hashable


"""
────────────────────────────────────────────────────────────────
BONUS CHALLENGE
────────────────────────────────────────────────────────────────
You have two lists of customer IDs:
    visited_page_a = ["C001", "C002", "C003", "C002", "C004"]
    visited_page_b = ["C003", "C004", "C005", "C001"]

Using sets, find:
1. All unique customers who visited either page (union)
2. Customers who visited BOTH pages (intersection)
3. Customers who visited page A but NOT page B (difference)
────────────────────────────────────────────────────────────────
"""

# Your bonus code here:
visited_page_a = {"C001", "C002", "C003", "C002", "C004"}
visited_page_b = {"C003", "C004", "C005", "C001"}
print(visited_page_a.union(visited_page_b))
print(visited_page_a.intersection(visited_page_b))
print(visited_page_a.difference(visited_page_b))