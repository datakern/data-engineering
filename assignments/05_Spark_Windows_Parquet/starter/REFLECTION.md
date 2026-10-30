# Reflection: Assignment 5 (Spark windows and Parquet)

Answer in your own words, with numbers from YOUR runs. Two to four sentences each.

**1. CSV vs Parquet.** What were the sizes of `sales` as CSV and as Parquet on your machine? Name two reasons Parquet is smaller and one reason it is faster for a query that needs only `price`.

**2. Tidy write.** How many files did `write_silver` produce, and how many did it produce when you removed `repartition`? Explain the difference in terms of memory partitions and folders on disk.

**3. Window design.** For `latest_order_per_customer`, what would change in the output if you removed the `order_id` tie-break? Did it change on your data? How did you find out?

**4. Rank functions.** In `top3_products_per_category`, how many rows do you get with `dense_rank` ordered by revenue only, compared with `row_number`? Which would you choose for a dashboard and why?

**5. The one-partition trap.** What warning did Spark print when you removed `partitionBy` from a window? What did the Stage page in the Spark UI show (number of tasks), and what would happen at 100 GB?

**6. Pruning.** Paste the `PartitionFilters` line from `plan_text(...)` for the month read and for the version that filters on `purchase_ts` instead. What is the difference, and why does it matter?

**7. UDF.** Built-in vs UDF timings on your machine, and the ratio. Would you ever ship the UDF? Name one case where a UDF is unavoidable.

**8. Over-partitioning.** How many folders and files did `partitionBy("seller_id")` create, and how large was the average file? Why is that a problem for the next job that reads it?

**9. What surprised you,** and one thing you would still like to understand better.
