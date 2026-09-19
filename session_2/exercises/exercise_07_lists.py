"""
╔══════════════════════════════════════════════════════════════╗
║  Exercise 07 — Mastering Lists                               ║
║  Topic: Lists · Ordering · Indexing · Mutability             ║
╚══════════════════════════════════════════════════════════════╝

Real-world story
────────────────
You are managing an order queue for a warehouse.
Orders arrive and need to be tracked in the exact sequence they came in.
Some orders get cancelled and some new ones get added.

A Python List is perfect for this — it remembers order and allows changes.

────────────────────────────────────────────────────────────────
What to explore
────────────────────────────────────────────────────────────────
Work through each section below. Uncomment one block at a time,
run the file, observe the output, and then move to the next block.

────────────────────────────────────────────────────────────────
SECTION 1 — Creating a list and accessing items
────────────────────────────────────────────────────────────────
"""

# ✅ POSITIVE: Create a list of order IDs and access specific ones
# orders = ["ORD001", "ORD002", "ORD003", "ORD004", "ORD005"]
# print("All orders:", orders)
# print("First order:", orders[0])     # Index 0 = first item
# print("Last order:", orders[-1])     # Index -1 = last item
# print("First 3 orders:", orders[:3]) # Slice: from start up to index 3


"""
────────────────────────────────────────────────────────────────
SECTION 2 — Modifying a list (Lists are MUTABLE)
────────────────────────────────────────────────────────────────
"""

# ✅ POSITIVE: Add and remove items
# orders = ["ORD001", "ORD002", "ORD003"]
# print("Before:", orders)
#
# orders.append("ORD004")         # Add to the END
# print("After append:", orders)
#
# orders.insert(1, "ORD999")      # Insert at a specific position (index 1)
# print("After insert at index 1:", orders)
#
# orders.remove("ORD002")         # Remove a specific value
# print("After removing ORD002:", orders)
#
# popped = orders.pop()           # Remove and RETURN the last item
# print("Popped item:", popped)
# print("After pop:", orders)


"""
────────────────────────────────────────────────────────────────
SECTION 3 — Lists allow DUPLICATES
────────────────────────────────────────────────────────────────
"""

# ✅ POSITIVE: Duplicates are allowed — this is by design
# orders = ["ORD001", "ORD002", "ORD001", "ORD003"]
# print("List with duplicates:", orders)
# print("How many times does ORD001 appear?", orders.count("ORD001"))


"""
────────────────────────────────────────────────────────────────
SECTION 4 — Looping through a list
────────────────────────────────────────────────────────────────
"""

# ✅ POSITIVE: Loop with and without index
# orders = ["ORD001", "ORD002", "ORD003"]
#
# print("--- Simple loop ---")
# for order in orders:
#     print("Processing:", order)
#
# print("--- Loop with index using enumerate() ---")
# for i, order in enumerate(orders):
#     print(f"  Position {i}: {order}")


"""
────────────────────────────────────────────────────────────────
SECTION 5 — NEGATIVE SCENARIOS (What can go wrong?)
────────────────────────────────────────────────────────────────
"""

# NEGATIVE 1: Accessing an index that doesn't exist
# orders = ["ORD001", "ORD002", "ORD003"]
# print(orders[10])   # IndexError! The list only has 3 items (indexes 0, 1, 2)


# NEGATIVE 2: Trying to remove an item that isn't in the list
# orders = ["ORD001", "ORD002"]
# orders.remove("ORD999")  # ValueError! "ORD999" is not in the list


# NEGATIVE 3: Confusing index with count
# orders = ["ORD001", "ORD002", "ORD003"]
# print(orders[3])   # IndexError! Index 3 doesn't exist. The last valid index is 2.
# Tip: len(orders) gives you 3, but the last index is always len(orders) - 1


"""
────────────────────────────────────────────────────────────────
BONUS CHALLENGE
────────────────────────────────────────────────────────────────
Start with this list of products:
    products = ["Laptop", "Mouse", "Monitor", "Keyboard", "Mouse", "Desk"]

1. Print just the last 3 items
2. Count how many times "Mouse" appears
3. Sort the list alphabetically and print it
4. Remove the first "Mouse" from the list
5. Print the final list

Hint: Look up the .sort() method!
────────────────────────────────────────────────────────────────
"""

# Your bonus code here:
products = ["Laptop", "Mouse", "Monitor", "Keyboard", "Mouse", "Desk"]
for product in products[-3:]:
    print(product)

product_count = products.count("Mouse")
print(f"Mouse appears {product_count} times.")    

products.sort()
print("Sorted list:", products)

products.remove("Mouse")
print("Final list:", products)      