# Business Requirements Document

## Problem 1: Finance Sales Analysis

**Business problem:** Finance needs consistent sales numbers to track revenue trends.
**Business user:** Finance Team
**Business question:** "How is item sales value changing across product categories over time?"
**Metric:** Total Sales Value (Sum of item prices, excluding freight).
**Time definition:** Month, based on the order purchase timestamp.
**Included records:** Orders with a valid purchase timestamp. Order items associated with those orders.
**Excluded records:** Cancelled orders (if deemed non-revenue generating, assumption to document).
**Unknown/missing-value treatment:** If a product category is missing, it should be labeled as 'Unknown' rather than dropped.
**Expected final grain:** One row = one category for one month.
**Assumptions:** Sales value equals the sum of `price` from `order_items`. Freight is not considered product revenue.

---

## Problem 2: Operations Delivery Performance

**Business problem:** Operations needs to identify where and when late deliveries occur to improve logistics.
**Business user:** Operations Team
**Business question:** "How is delivery performance changing, and where are late deliveries occurring?"
**Metric:** Delivery Classification (Late, On-time), Total Orders, Delivered Orders, Late Delivery Rate.
**Time definition:** Month, based on the estimated delivery date or purchase date (we will use purchase date to group cohorts, but actual vs estimated to determine 'late').
**Included records:** Orders that have been delivered.
**Excluded records:** Orders that were cancelled or never delivered.
**Unknown/missing-value treatment:** Missing actual delivery date means the order is not yet delivered or status is unknown.
**Expected final grain:** One row = one month + one customer state/city.
**Assumptions:** 'Late' strictly means `actual_delivery_date > estimated_delivery_date`.
