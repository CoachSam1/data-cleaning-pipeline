# ==========================================
# Professional Data Cleaning Pipeline
# ==========================================

import pandas as pd
import argparse
import logging
import os

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

def load_data(path):
    """Load CSV file into a DataFrame."""
    return pd.read_csv(path)

def clean_columns(df):
    """Standardize column names."""
    df.columns = df.columns.str.strip().str.lower()
    return df

def remove_duplicates(df):
    """Drop duplicate rows."""
    return df.drop_duplicates()

def clean_numeric(series):
    """Remove non-numeric characters and convert to float."""
    return pd.to_numeric(series.astype(str).str.replace(r'[^0-9.]', '', regex=True), errors="coerce")

def handle_missing(df):
    """Fill or drop missing values in key columns."""
    if "quantity" in df: df["quantity"] = df["quantity"].fillna(1)
    if "discount" in df: df["discount"] = df["discount"].fillna(0)
    if "unit_price" in df: df = df.dropna(subset=["unit_price"])
    return df

def fix_total(df):
    """Recalculate totals and filter invalid rows."""
    if {"quantity", "unit_price"} <= set(df.columns):
        df["total"] = df["quantity"] * df["unit_price"]
        df = df[(df["total"] > 0) & (df["quantity"] > 0)]
    return df

def clean_text(df):
    """Format text fields consistently."""
    if "customer name" in df: df["customer name"] = df["customer name"].astype(str).str.strip().str.title()
    if "email" in df: df["email"] = df["email"].astype(str).str.strip().str.lower()
    if "product" in df: df["product"] = df["product"].astype(str).str.strip().str.title()
    if "region" in df: df["region"] = df["region"].astype(str).str.strip().str.upper()
    if "status" in df:
        status_map = {"completed":"Completed","shipped":"Shipped","pending":"Pending","cancelled":"Cancelled"}
        df["status"] = df["status"].astype(str).str.strip().str.lower().map(status_map)
        df = df.dropna(subset=["status"])
    return df

def clean_dates(df):
    """Convert order date to datetime."""
    if "order date" in df:
        df["order date"] = pd.to_datetime(df["order date"], errors="coerce")
        df = df.dropna(subset=["order date"])
    return df

def polish(df):
    """Final polish: fill text fields, reset index."""
    if "customer name" in df: df["customer name"] = df["customer name"].fillna("Unknown")
    if "phone number" in df: df["phone number"] = df["phone number"].fillna("Not Provided")
    return df.reset_index(drop=True)

def validate(df):
    """Basic validation checks."""
    logging.info(f"Shape: {df.shape}")
    logging.info(f"Missing values:\n{df.isnull().sum()}")
    if "status" in df: logging.info(f"Statuses: {df['status'].unique()}")
    if "region" in df: logging.info(f"Regions: {df['region'].unique()}")
    if {"quantity","unit_price","total"} <= set(df.columns):
        check = (df["total"] - df["quantity"]*df["unit_price"]).abs().sum()
        logging.info(f"Logic check (should be 0): {check}")

def save(df, path):
    """Save cleaned data to CSV."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8-sig")
    logging.info(f"Saved cleaned file: {path}")

def run_pipeline(input_file, output_file):
    df = load_data(input_file)
    df = clean_columns(df)
    df = remove_duplicates(df)
    if "quantity" in df: df["quantity"] = clean_numeric(df["quantity"])
    if "unit_price" in df: df["unit_price"] = clean_numeric(df["unit_price"])
    if "discount" in df: df["discount"] = clean_numeric(df["discount"])
    df = handle_missing(df)
    df = fix_total(df)
    df = clean_text(df)
    df = clean_dates(df)
    df = polish(df)
    validate(df)
    save(df, output_file)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Data Cleaning Pipeline")
    parser.add_argument("--input", required=True, help="Path to input CSV")
    parser.add_argument("--output", required=True, help="Path to output CSV")
    args = parser.parse_args()
    run_pipeline(args.input, args.output)
