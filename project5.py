"""
Dmart analysis using PySpark

Author: GOKULARAJA R
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, countDistinct

# Initialize Spark Session
def create_spark_session():
    return SparkSession.builder \
        .appName("DmartAnalysis") \
        .getOrCreate()

# Load CSV Files
def load_data(spark, path):
    return spark.read.option("header", True).option("inferSchema", True).csv(path)

#Filling numeric columns with averages
def fill_numeric_with_avg(df, cols):
    for column in cols:
        avg_val = df.select(avg(column)).first()[0]
        if avg_val is not None:
            df = df.fillna({column: avg_val})
    return df

#Fillna with default for string
def fill_string_with_default(df, cols, default="Unknown"):
    return df.fillna({col: default for col in cols})

# Clean and Transform Data
def clean_transform_data(products_df, sales_df, customers_df):
    #Renaming columns for consistency
    products_df = products_df.withColumnRenamed("Product ID", "Product_ID") \
                             .withColumnRenamed("Category", "Product_Category") \
                             .withColumnRenamed("Sub-Category", "Product_Sub_Category") \
                             .withColumnRenamed("Product Name", "Product_Name")

    customers_df = customers_df.withColumnRenamed("Customer ID", "Customer_ID") \
                               .withColumnRenamed("Customer Name", "Customer_Name") \
                               .withColumnRenamed("Segment", "Customer_Segment") \
                               .withColumnRenamed("Age", "Age") \
                               .withColumnRenamed("Country", "Country") \
                               .withColumnRenamed("City", "City") \
                               .withColumnRenamed("State", "State") \
                               .withColumnRenamed("Postal Code", "Postal_Code") \
                               .withColumnRenamed("Region", "Region")

    sales_df = sales_df.withColumnRenamed("Ship Date", "Ship_Date") \
                       .withColumnRenamed("Ship Mode", "Shipping_Mode") \
                       .withColumnRenamed("Customer ID", "Customer_ID") \
                       .withColumnRenamed("Product ID", "Product_ID") \
                       .withColumnRenamed("Sales", "Sales_Amount") \
                       .withColumnRenamed("Quantity", "Quantity") \
                       .withColumnRenamed("Discount", "Discount") \
                       .withColumnRenamed("Profit", "Profit")

    #Drop rows where IDs are missing
    sales_df = sales_df.dropna(subset=["Customer_ID", "Product_ID"])
    customers_df = customers_df.dropna(subset=["Customer_ID"])
    products_df = products_df.dropna(subset=["Product_ID"])

    # Fill numeric columns with averages
    sales_df = fill_numeric_with_avg(sales_df, ["Sales_Amount", "Quantity", "Discount", "Profit"])
    customers_df = fill_numeric_with_avg(customers_df, ["Age"])

    # Fill string columns with defaults
    sales_df = fill_string_with_default(sales_df, ["Ship_Date", "Shipping_Mode"])
    customers_df = fill_string_with_default(customers_df, ["Customer_Name", "Customer_Segment", "Country", "City", "State", "Region"])
    products_df = fill_string_with_default(products_df, ["Product_Category", "Product_Sub_Category", "Product_Name"])
    customers_df = customers_df.fillna({"Postal_Code": "00000"})

    # Showing schemas
    print("Schemas after cleaning and renaming:")
    sales_df.printSchema()
    customers_df.printSchema()
    products_df.printSchema()

    #Join all data
    df = sales_df.join(products_df, on="Product_ID", how="inner") \
                 .join(customers_df, on="Customer_ID", how="inner")

    print("Combined DataFrame Sample:")
    df.show()

    return df

#Analysis using PySpark
def run_dataframe_queries(df):
    print("\n PySpark DataFrame API Analysis Results\n")

    print("1. Total Sales for Each Product Category")
    df.groupBy("Product_Category").agg(sum("Sales_Amount").alias("Total_Sales")).show()

    print("2. Customer with Highest Number of Purchases")
    df.groupBy("Customer_ID", "Customer_Name") \
      .count() \
      .orderBy(col("count").desc()) \
      .limit(1).show()

    print("3. Average Discount Given Across All Products")
    df.agg(avg("Discount").alias("Average_Discount")).show()

    print("4. Unique Products Sold in Each Region")
    df.groupBy("Region").agg(countDistinct("Product_ID").alias("Unique_Products_Sold")).show()

    print("5. Total Profit Generated in Each State")
    df.groupBy("State").agg(sum("Profit").alias("Total_Profit")).show()

    print("6. Product Sub-Category with Highest Sales")
    df.groupBy("Product_Sub_Category") \
      .agg(sum("Sales_Amount").alias("Total_Sales")) \
      .orderBy(col("Total_Sales").desc()) \
      .limit(1).show()

    print("7. Average Age of Customers in Each Segment")
    df.groupBy("Customer_Segment").agg(avg("Age").alias("Average_Age")).show()

    print("8. Orders Shipped in Each Shipping Mode")
    df.groupBy("Shipping_Mode").count().show()

    print("9. Total Quantity of Products Sold in Each City")
    df.groupBy("City").agg(sum("Quantity").alias("Total_Quantity")).show()

    print("10. Customer Segment with Highest Profit Margin")
    df.groupBy("Customer_Segment") \
      .agg(sum("Profit").alias("Total_Profit")) \
      .orderBy(col("Total_Profit").desc()) \
      .limit(1).show()

# Main Execution
def main():
    spark = create_spark_session()

    products_path = "F:/Guvi/Mini project 5 Dmart analysis using pyspark/dataset/product.csv"
    sales_path = "F:/Guvi/Mini project 5 Dmart analysis using pyspark/dataset/sales.csv"
    customers_path = "F:/Guvi/Mini project 5 Dmart analysis using pyspark/dataset/customer.csv"

    products_df = load_data(spark, products_path)
    sales_df = load_data(spark, sales_path)
    customers_df = load_data(spark, customers_path)

    df = clean_transform_data(products_df, sales_df, customers_df)
    run_dataframe_queries(df)

    spark.stop()

if __name__ == "__main__":
    main()
