# Test Data Plan
# Retail Order Management - Mini Project 1

These are the scenarios I planned BEFORE running my code.
I wrote down what I expected to happen, then ran the tests and recorded results in `test_results.md`.

---

| Test ID | Scenario                             | Input                                                              | Expected Result                                         |
|---------|--------------------------------------|--------------------------------------------------------------------|----------------------------------------------------------|
| T001    | Regular customer places a normal order | Customer (Regular), Laptop x1, Mouse x2                         | Order succeeds. No discount. Total = $1290              |
| T002    | Premium customer places an order     | Customer (Premium), Laptop x1, Keyboard x2                        | 10% discount applied. Total = $1242                     |
| T003    | Corporate customer places an order   | Customer (Corporate), Mouse x10                                   | 20% discount applied. Total = $360                      |
| T004    | Quantity exceeds available stock     | Keyboard (stock=3 after T002), requested quantity = 100           | ValueError raised, order item NOT added                  |
| T005    | Quantity is zero (invalid)           | Any product, quantity = 0                                         | ValueError raised ("Quantity must be greater than zero") |
| T006    | Encapsulation: set negative price    | `laptop.price = -500`                                             | ValueError raised ("Price cannot be negative")           |

---

### Notes on test design

- Tests T001–T003 are the happy path scenarios — one per customer type.
- Tests T004–T006 are failure/edge cases to verify error handling and encapsulation.
- After T001 (Laptop x1), stock for Laptop = 9. After T002 (Laptop x1), stock = 8.
- After T002 (Keyboard x2), stock for Keyboard = 3. T004 tries to buy 100, which should fail.
- I deliberately ran T004 AFTER T002 to use the reduced stock, making it a realistic scenario.
