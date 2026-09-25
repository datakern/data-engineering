# Test Results
# Retail Order Management - Mini Project 1

Run command: `python3 retail_order.py`

---

## T001 — Regular customer places a normal order

**Expected:** Order succeeds. No discount applied. Final total = $1290.  
**Actual:**
```
Added: Laptop x1 to Order #ORD001
Added: Wireless Mouse x2 to Order #ORD001
Subtotal         : $1290.00
Discount ( 0%)   : -$0.00
Final Total      : $1290.00
```
**Status:** ✅ PASS  
**Notes:** Regular customer correctly gets 0% discount.

---

## T002 — Premium customer places an order

**Expected:** 10% discount applied. Subtotal = $1380. Final total = $1242.  
**Actual:**
```
Added: Laptop x1 to Order #ORD002
Added: Mechanical Keyboard x2 to Order #ORD002
Subtotal         : $1380.00
Discount (10%)   : -$138.00
Final Total      : $1242.00
```
**Status:** ✅ PASS  
**Notes:** PremiumCustomer.get_discount_rate() correctly returned 0.10. Polymorphism working.

---

## T003 — Corporate customer places an order

**Expected:** 20% discount applied. Subtotal = $450. Final total = $360.  
**Actual:**
```
Added: Wireless Mouse x10 to Order #ORD003
Subtotal         : $450.00
Discount (20%)   : -$90.00
Final Total      : $360.00
```
**Status:** ✅ PASS  
**Notes:** CorporateCustomer.get_discount_rate() correctly returned 0.20.

---

## T004 — Quantity exceeds available stock

**Expected:** ValueError raised. Order item not added.  
**Actual:**
```
Caught expected error: Not enough stock for 'Mechanical Keyboard'. Requested: 100, Available: 3
```
**Status:** ✅ PASS  
**Notes:** After T002 bought Keyboard x2, stock dropped to 3. Requesting 100 correctly fails.  
Stock was NOT reduced (error was raised before any change).

---

## T005 — Invalid quantity = 0

**Expected:** ValueError raised ("Quantity must be a positive number").  
**Actual:**
```
Caught expected error: Quantity must be a positive number.
```
**Status:** ✅ PASS  
**Notes:** Validation inside `reduce_stock()` catches quantity ≤ 0 before any state changes.

---

## T006 — Encapsulation: setting a negative price

**Expected:** ValueError raised ("Price cannot be negative").  
**Actual:**
```
Caught expected error: Price cannot be negative.
```
**Status:** ✅ PASS  
**Notes:** The @property setter blocked the invalid assignment. `_price` was not changed.

---

## Summary

| Test ID | Scenario                        | Status |
|---------|---------------------------------|--------|
| T001    | Regular customer order          | ✅ PASS |
| T002    | Premium customer order          | ✅ PASS |
| T003    | Corporate customer order        | ✅ PASS |
| T004    | Quantity exceeds stock          | ✅ PASS |
| T005    | Invalid quantity = 0            | ✅ PASS |
| T006    | Negative price (encapsulation)  | ✅ PASS |

All 6 tests passed. No failures.
