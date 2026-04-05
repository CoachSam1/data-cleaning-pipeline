# ==========================================
# PROFESSIONAL DATA CLEANING PIPELINE (FIXED)
# ==========================================

import pandas as pd
import numpy as np
from google.colab import files

print("🚀 Starting Professional Data Pipeline...")

# ------------------------------------------
# STEP 1: UPLOAD FILE
# ------------------------------------------
uploaded = files.upload()

if not uploaded:
    raise Exception("❌ No file uploaded")

filename = list(uploaded.keys())[0]
print(f"✅ File uploaded: {filename}")

# ------------------------------------------
# STEP 2: LOAD DATA
# ------------------------------------------
df = pd.read_csv(filename)
print("✅ Data loaded successfully")

# ------------------------------------------
# STEP 3: CLEAN COLUMN NAMES (FIXED)
# ------------------------------------------
df.columns = df.columns.str.strip().str.lower()

print("✅ Columns cleaned:")
print(df.columns.tolist())

# ------------------------------------------
# STEP 4: REMOVE DUPLICATES
# ------------------------------------------
df = df.drop_duplicates()
print("✅ Duplicates removed")

# ------------------------------------------
# STEP 5: CLEAN NUMERIC DATA
# ------------------------------------------
def clean_numeric(series):
    return pd.to_numeric(
        series.astype(str).str.replace(r'[^0-9.]', '', regex=True),
        errors='coerce'
    )

df['quantity'] = clean_numeric(df['quantity'])
df['unit_price'] = clean_numeric(df['unit_price'])
df['discount'] = clean_numeric(df['discount'])

print("✅ Numeric columns cleaned")

# ------------------------------------------
# STEP 6: HANDLE MISSING VALUES
# ------------------------------------------
df['quantity'] = df['quantity'].fillna(1)
df['discount'] = df['discount'].fillna(0)

df = df.dropna(subset=['unit_price'])

print("✅ Missing values handled")

# ------------------------------------------
# STEP 7: FIX TOTAL
# ------------------------------------------
df['total'] = df['quantity'] * df['unit_price']

df = df[df['total'] > 0]
df = df[df['quantity'] > 0]

print("✅ Total fixed")

# ------------------------------------------
# STEP 8: CLEAN TEXT
# ------------------------------------------
def clean_text(series):
    return series.astype(str).str.strip()

df['customer name'] = clean_text(df['customer name']).str.title()
df['email'] = clean_text(df['email']).str.lower()
df['product'] = clean_text(df['product']).str.title()
df['region'] = clean_text(df['region']).str.upper()

df['status'] = clean_text(df['status']).str.lower()

status_map = {
    'completed': 'Completed',
    'shipped': 'Shipped',
    'pending': 'Pending',
    'cancelled': 'Cancelled'
}

df['status'] = df['status'].map(status_map)

df = df.dropna(subset=['status'])

print("✅ Text cleaned")

# ------------------------------------------
# STEP 9: CLEAN DATES
# ------------------------------------------
df['order date'] = pd.to_datetime(df['order date'], errors='coerce')

df = df.dropna(subset=['order date'])

print("✅ Dates cleaned")

# ------------------------------------------
# STEP 10: FINAL POLISH
# ------------------------------------------
df['customer name'] = df['customer name'].fillna('Unknown')
df['phone number'] = df['phone number'].fillna('Not Provided')

df = df.reset_index(drop=True)

print("✅ Final polish done")

# ------------------------------------------
# STEP 11: VALIDATION
# ------------------------------------------
print("\n🔍 VALIDATION")

print("Shape:", df.shape)
print("\nMissing values:\n", df.isnull().sum())
print("\nStatus:", df['status'].unique())
print("\nRegion:", df['region'].unique())

# Logic check
check = (df['total'] - (df['quantity'] * df['unit_price'])).abs().sum()
print("\nLogic check (should be 0):", check)

# ------------------------------------------
# STEP 12: SAVE FILE (GOOGLE SHEETS SAFE)
# ------------------------------------------
output_file = "cleaned_sales_final.csv"

df.to_csv(output_file, index=False, encoding='utf-8-sig')

print("\n✅ File saved correctly")

# ------------------------------------------
# STEP 13: VERIFY FILE
# ------------------------------------------
test = pd.read_csv(output_file)
print("\n✅ Verification preview:")
print(test.head())

# ------------------------------------------
# STEP 14: DOWNLOAD
# ------------------------------------------
files.download(output_file)

print("\n🎉 DONE! Open file in Google Sheets")
