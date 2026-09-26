"""
Solution Script for DataKern Pandas Assignment Part B
Instructor note: Ensure students implement logic based on their findings in observations.md.
"""
import pandas as pd
from pathlib import Path

# Setup paths
PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw"
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"

# Ensure output directory exists
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

def process_orders():
    print("Processing orders...")
    try:
        df = pd.read_csv(RAW_DIR / "olist_orders_dataset.csv")
    except FileNotFoundError:
        print("Instructor Note: orders data not found. Please ensure raw CSVs are in data/raw/")
        return
        
    # Convert date columns to datetime
    date_cols = [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date"
    ]
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col])
        
    out_path = PROCESSED_DIR / "processed_orders.csv"
    df.to_csv(out_path, index=False)
    print(f"Saved to {out_path} with shape {df.shape}")

def process_products():
    print("Processing products...")
    try:
        df = pd.read_csv(RAW_DIR / "olist_products_dataset.csv")
    except FileNotFoundError:
        print("Instructor Note: products data not found.")
        return
        
    # Fill missing category names
    if "product_category_name" in df.columns:
        df["product_category_name"] = df["product_category_name"].fillna("Unknown")
    
    out_path = PROCESSED_DIR / "processed_products.csv"
    df.to_csv(out_path, index=False)
    print(f"Saved to {out_path} with shape {df.shape}")

def process_reviews():
    print("Processing reviews...")
    try:
        df = pd.read_csv(RAW_DIR / "olist_order_reviews_dataset.csv")
    except FileNotFoundError:
        print("Instructor Note: reviews data not found.")
        return
        
    # Fill missing text comments
    if "review_comment_title" in df.columns:
        df["review_comment_title"] = df["review_comment_title"].fillna("No Title")
    if "review_comment_message" in df.columns:
        df["review_comment_message"] = df["review_comment_message"].fillna("No Comment")
    
    out_path = PROCESSED_DIR / "processed_reviews.csv"
    df.to_csv(out_path, index=False)
    print(f"Saved to {out_path} with shape {df.shape}")

if __name__ == "__main__":
    print("Starting data processing pipeline...")
    process_orders()
    process_products()
    process_reviews()
    print("Processing completed successfully.")
