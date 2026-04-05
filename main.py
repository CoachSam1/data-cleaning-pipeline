
# ==========================================
# DATA CLEANING PIPELINE
# Python & Pandas | Production Ready
# Author: Samuel | github.com/CoachSam1
# ==========================================

import pandas as pd
import numpy as np
import os
import logging

# I set up logging so every step prints clearly
# with a timestamp — makes it easy to debug later
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
log = logging.getLogger(__name__)

# These are the two paths I care about:
# where the raw data lives, and where I want
# the clean version to be saved
RAW_DATA_PATH = "data/raw/messy_sales_data.csv"
PROCESSED_DATA_PATH = "data/processed/cleaned_sales_final.csv"


# ------------------------------------------
# First things first — load the data
# ------------------------------------------
def load_data(filepath):
    log.info("Loading the raw data file...")
    df = pd.read_csv(filepath)
    log.info(f"  Got {len(df)} rows and {len(df.columns)} columns")
    log.info(f"  Columns: {list(df.columns)}")
    return df


# ------------------------------------------
# Column names are always messy — fix them
# ------------------------------------------
def clean_column_names(df):
    log.info("Cleaning up column names...")
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_", regex=False)
        .str.replace(r"[^\w]", "", regex=True)
    )
    log.info(f"  Clean columns: {list(df.columns)}")
    return df


# ------------------------------------------
# Nobody wants duplicate rows in their data
# ------------------------------------------
def remove_duplicates(df):
    log.info("Dropping duplicate rows...")
    before = len(df)
    df = df.drop_duplicates()
    removed = before - len(df)
    log.info(f"  Removed {removed} duplicate(s)")
    return df


# ------------------------------------------
# Strip out symbols like $ and , from numbers
# ------------------------------------------
def clean_numeric(series):
    return pd.to_numeric(
        series.astype(str).str.replace(r"[^0-9.]", "", regex=True),
        errors="coerce"
    )

def clean_numeric_columns(df):
    log.info("Cleaning numeric columns (quantity, price, discount)...")
    df["quantity"]   = clean_numeric(df["quantity"])
    df["unit_price"] = clean_numeric(df["unit_price"])
    df["discount"]   = clean_numeric(df["discount"])
    log.info("  Done — numbers are clean")
    return df


# ------------------------------------------
# Fill in the blanks where it makes sense
# ------------------------------------------
def handle_missing_values(df):
    log.info("Handling missing values...")
    before = df.isnull().sum().sum()

    # A quantity of 1 is a safe default
    df["quantity"] = df["quantity"].fillna(1)

    # No discount mentioned? Assume 0
    df["discount"] = df["discount"].fillna(0)

    # If we don't know the customer name, just say Unknown
    df["customer_name"] = df["customer_name"].fillna("Unknown")

    # Phone number missing? Mark it clearly
    df["phone_number"] = df["phone_number"].fillna("Not Provided")

    # Can't do anything useful without a price — drop those rows
    df = df.dropna(subset=["unit_price"])

    after = df.isnull().sum().sum()
    log.info(f"  Missing values: {before} → {after}")
    return df


# ------------------------------------------
# Tidy up text — consistent casing matters
# ------------------------------------------
def clean_text_columns(df):
    log.info("Standardizing text fields...")

    # Names look better in Title Case
    df["customer_name"] = df["customer_name"].astype(str).str.strip().str.title()

    # Emails should always be lowercase
    df["email"] = df["email"].astype(str).str.strip().str.lower()

    # Product names in Title Case
    df["product"] = df["product"].astype(str).str.strip().str.title()

    # Regions in UPPERCASE for consistency
    df["region"] = df["region"].astype(str).str.strip().str.upper()

    # Map all status variations to clean labels
    status_map = {
        "completed" : "Completed",
        "shipped"   : "Shipped",
        "pending"   : "Pending",
        "cancelled" : "Cancelled"
    }
    df["status"] = df["status"].astype(str).str.strip().str.lower().map(status_map)

    # Drop rows where status didn't match anything we recognize
    df = df.dropna(subset=["status"])

    log.info(f"  Statuses found : {df['status'].unique().tolist()}")
    log.info(f"  Regions found  : {df['region'].unique().tolist()}")
    return df


# ------------------------------------------
# Parse dates properly so they're usable
# ------------------------------------------
def clean_dates(df):
    log.info("Parsing order dates...")
    df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")

    # Drop rows where the date couldn't be parsed
    df = df.dropna(subset=["order_date"])

    log.info(f"  Date range: {df['order_date'].min()} → {df['order_date'].max()}")
    return df


# ------------------------------------------
# Calculate revenue and check the math
# ------------------------------------------
def validate_and_engineer(df):
    log.info("Calculating revenue and validating the numbers...")

    # Revenue = quantity × price × (1 - discount)
    df["revenue"] = (
        df["quantity"] * df["unit_price"] * (1 - df["discount"])
    ).round(2)

    # Remove anything that doesn't make business sense
    df = df[df["revenue"] > 0]
    df = df[df["quantity"] > 0]

    # Quick logic check — this should always be 0.0
    logic_check = (
        df["revenue"] - (df["quantity"] * df["unit_price"] * (1 - df["discount"]))
    ).abs().sum()

    log.info(f"  Logic check (must be 0.0): {logic_check}")
    log.info(f"  Rows after validation: {len(df)}")
    return df


# ------------------------------------------
# Save the clean file — job almost done
# ------------------------------------------
def export_data(df, filepath):
    log.info("Saving the clean dataset...")
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    df = df.reset_index(drop=True)
    df.to_csv(filepath, index=False, encoding="utf-8-sig")
    log.info(f"  Saved to: {filepath}")
    log.info(f"  Final size: {df.shape[0]} rows × {df.shape[1]} columns")


# ------------------------------------------
# Read it back and confirm everything is good
# ------------------------------------------
def verify_output(filepath):
    log.info("Verifying the output file...")
    test = pd.read_csv(filepath)
    log.info(f"  Rows confirmed  : {len(test)}")
    log.info(f"  Missing values  : {test.isnull().sum().sum()}")
    log.info(f"\n{test.head()}")


# ------------------------------------------
# This is where everything runs together
# ------------------------------------------
def run_pipeline():
    print("\n" + "=" * 50)
    print("   DATA CLEANING PIPELINE — STARTED")
    print("=" * 50 + "\n")

    df = load_data(RAW_DATA_PATH)
    df = clean_column_names(df)
    df = remove_duplicates(df)
    df = clean_numeric_columns(df)
    df = handle_missing_values(df)
    df = clean_text_columns(df)
    df = clean_dates(df)
    df = validate_and_engineer(df)
    export_data(df, PROCESSED_DATA_PATH)
    verify_output(PROCESSED_DATA_PATH)

    print("\n" + "=" * 50)
    print("   PIPELINE COMPLETE ✅ — DATA IS READY")
    print("=" * 50 + "\n")


if __name__ == "__main__":
    run_pipeline()
````
