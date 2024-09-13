import os
import psycopg2
import logging
from pyspark.sql import SparkSession
from dotenv import load_dotenv

# Load environment variables from .env fileclear

load_dotenv()

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Sales Data Pipeline") \
    .getOrCreate()

# Read the sales_data.csv file
sales_data = spark.read.csv("/home/ubuntu1234/Desktop/coding/pyspark_pipeline_project/data/sales_data.csv", header=True, inferSchema=True)


# Define PostgreSQL connection parameters
db_url = os.getenv("POSTGRES_DB")
db_user = os.getenv("POSTGRES_USER")
db_password = os.getenv("POSTGRES_PASSWORD")
db_host = os.getenv("POSTGRES_HOST")
db_port = os.getenv("POSTGRES_PORT")

# Convert the Spark DataFrame to Pandas (optional, if handling small data)
sales_data_pandas = sales_data.toPandas()

# Define function to insert data into PostgreSQL
def write_to_postgres(data):
    # Connect to PostgreSQL
    conn = psycopg2.connect(
        dbname=db_url,
        user=db_user,
        password=db_password,
        host=db_host,
        port=db_port
    )
    cur = conn.cursor()
    
    # Define insert query
    insert_query = """
        INSERT INTO sales_data ("OrderID", "Date", "CustomerID", "ProductID", "Quantity", "Price", "SalesRep", "Region", "TotalAmount")
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    
    # Insert each row into PostgreSQL
    for _, row in data.iterrows():
        cur.execute(insert_query, (
            row['OrderID'],
            row['Date'],
            row['CustomerID'],
            row['ProductID'],
            row['Quantity'],
            row['Price'],
            row['SalesRep'],
            row['Region'],
            row['TotalAmount']
        ))
    
    conn.commit()
    cur.close()
    conn.close()
    logger.info("Data inserted into PostgreSQL successfully.")

# Call the function to write data
write_to_postgres(sales_data_pandas)

# Stop the Spark session
spark.stop()
