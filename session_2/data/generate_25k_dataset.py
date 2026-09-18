import csv
import random
from datetime import datetime, timedelta

def generate_orders_csv(filename, num_rows):
    customer_types = ["Regular", "Premium", "Corporate"]
    products = [
        {"name": "Laptop", "base_price": 999.99},
        {"name": "Mouse", "base_price": 29.99},
        {"name": "Keyboard", "base_price": 49.99},
        {"name": "Monitor", "base_price": 299.99},
        {"name": "Desk", "base_price": 199.99},
        {"name": "Chair", "base_price": 149.99},
    ]

    start_date = datetime(2025, 1, 1)

    with open(filename, mode='w', newline='') as file:
        writer = csv.writer(file)
        # Header
        writer.writerow(["order_id", "order_date", "customer_type", "product", "quantity", "unit_price", "has_discount"])

        for i in range(1, num_rows + 1):
            order_id = f"ORD{i:05d}"
            
            # Random date within the last 365 days
            days_offset = random.randint(0, 365)
            order_date = (start_date + timedelta(days=days_offset)).strftime("%Y-%m-%d")
            
            # Select random product and customer type
            product_info = random.choice(products)
            product = product_info["name"]
            base_price = product_info["base_price"]
            customer_type = random.choice(customer_types)
            quantity = random.randint(1, 20)
            has_discount = random.choice(["True", "False"])
            
            # Inject some NaNs (~5% missing values for cleaning exercises)
            if random.random() < 0.05:
                customer_type = ""
            
            # Price variations
            # Some missing unit prices
            if random.random() < 0.05:
                unit_price = ""
            else:
                # Add some small random variation to price
                unit_price = round(base_price + random.uniform(-5.0, 5.0), 2)
            
            # Rare chance of product being empty
            if random.random() < 0.01:
                product = ""

            writer.writerow([order_id, order_date, customer_type, product, quantity, unit_price, has_discount])

if __name__ == "__main__":
    generate_orders_csv("orders_25k.csv", 25000)
    print("Generated orders_25k.csv successfully!")
