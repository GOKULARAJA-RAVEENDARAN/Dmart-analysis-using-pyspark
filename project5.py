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

# Clean and Transform Data
def clean_transform_data(products_df, sales_df, customers_df):
    # Drop missing values
    products_df = products_df.dropna()
    sales_df = sales_df.dropna()
    customers_df = customers_df.dropna()

    # Rename columns for consistent use
    products_df = products_df.withColumnRenamed("Product ID", "Product_ID") \
                             .withColumnRenamed("Category", "Product_Category") \
                             .withColumnRenamed("Sub-Category", "Product_Sub_Category") \
                             .withColumnRenamed("Product Name", "Product_Name")

    customers_df = customers_df.withColumnRenamed("Customer ID", "Customer_ID") \
                               .withColumnRenamed("Customer Name", "Customer_Name") \
                               .withColumnRenamed("Segment", "Customer_Segment")

    sales_df = sales_df.withColumnRenamed("Customer ID", "Customer_ID") \
                       .withColumnRenamed("Product ID", "Product_ID") \
                       .withColumnRenamed("Sales", "Sales_Amount") \
                       .withColumnRenamed("Ship Mode", "Shipping_Mode")
    print("printing the schema for df's")
    sales_df.printSchema()
    products_df.printSchema()
    customers_df.printSchema()                   

    # Join the DataFrames
    df = sales_df.join(products_df, on="Product_ID", how="inner") \
                 .join(customers_df, on="Customer_ID", how="inner")
    print("##########Printing the Resultant DF after combining all 3 ##############")
    df.show()

    return df

# Analysis using PySpark
def run_dataframe_queries(df):
    print("PySpark DataFrame API Analysis")

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

    # Loading datasets
    products_df = load_data(spark, products_path)
    sales_df = load_data(spark, sales_path)
    customers_df = load_data(spark, customers_path)

    # Clean and transform
    df = clean_transform_data(products_df, sales_df, customers_df)

    # Run analysis
    run_dataframe_queries(df)

    spark.stop()

if __name__ == "__main__":
    main()
