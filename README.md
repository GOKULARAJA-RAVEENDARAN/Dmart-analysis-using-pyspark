# Dmart-analysis-using-pyspark
Dmart analysis using pyspark

**Project Overview**
This project focuses on building a data pipeline using PySpark to integrate and analyze sales data from multiple sources:
- Product information
- Sales transactions
- Customer details


**Technologies Used**
- Python
- SQL
- PySpark
- GitHub for version control

**Dataset**
The project uses the Dmart dataset, which includes:
- products.csv — Contains product details
- sales.csv — Includes transaction details
- customer.csv — Stores customer information

**Problem Statement**
The objective is to answer business-related analytical questions based on the integrated dataset by performing data transformations and querying with PySpark.

**Tasks Implemented**

1️⃣ Establish PySpark Connection
✔️ Set up PySpark environment
✔️ Read CSV files into PySpark DataFrames

2️⃣ Load Data into PySpark
✔️ Load products.csv, sales.csv, customer.csv into DataFrames

3️⃣ Data Transformation & Cleaning
✔️ Rename columns for consistency
✔️ Handle missing values
✔️ Ensure correct data types
✔️ Join DataFrames on relevant keys (Product ID, Customer ID)

4️⃣ Data Analysis & Querying
✔️ Answer key business questions using PySpark
✔️ Perform analytical queries

**Analytical Questions Answered**
- Total sales per product category
- Customer with highest number of purchases
- Average discount given across all products
- Unique products sold per region
- Total profit generated per state
- Top-selling product sub-category
- Average customer age per segment
- Orders shipped per shipping mode
- Total quantity of products sold per city
- Customer segment with highest profit margin

**How to Run the Project**
- Install dependencies:
pip install pyspark pandas numpy
- Clone this repository:
git clone <repo-link>
- Navigate to the project folder:
cd dmart-analysis-pyspark
- Run the Python script:
  project5.py
- View results in console or save to CSV.

