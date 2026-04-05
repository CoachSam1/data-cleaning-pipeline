# Data Cleaning Pipeline (Python + Pandas)

## Overview
This project implements a structured data cleaning pipeline that transforms raw sales data into a clean, consistent, and analysis-ready dataset.

The pipeline focuses on data reliability, validation, and enforcing correct business logic, which are critical in real-world data engineering workflows.

---

## Key Highlights

- Built a modular data cleaning pipeline using Python and Pandas
- Handled real-world data quality issues (missing values, duplicates, corrupted data)
- Implemented validation checks to ensure data correctness
- Designed for reproducibility and scalability

---

## Dataset
The dataset contains:

- Customer information (name, email, phone)
- Product and region
- Quantity, unit price, discount
- Order date and status

---

## Data Issues Identified
The raw dataset contained:

- Duplicate records
- Inconsistent column naming (case and spacing)
- Corrupted numeric values (e.g. "$1,200", "10pcs")
- Missing values in critical fields
- Inconsistent categorical values (status, region)
- Invalid date formats

---

## Solution

The pipeline performs the following steps:

- Standardizes column names (lowercase, trimmed)
- Removes duplicate records
- Cleans numeric fields using regex
- Converts values to correct data types
- Handles missing values with clear rules
- Recalculates total revenue (quantity × unit price)
- Standardizes text fields (names, emails, regions)
- Validates and converts date fields

---

## Validation

To ensure data reliability:

- No missing values in critical columns
- Revenue calculations verified (error = 0)
- Consistent categorical values
- Clean and structured dataset

---

## Output

The final dataset is:

- Clean
- Consistent
- Validated
- Ready for analysis, reporting, or machine learning

---

## Project Structure
