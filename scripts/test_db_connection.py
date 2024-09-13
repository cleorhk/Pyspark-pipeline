import os
import psycopg2
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Define PostgreSQL connection parameters
db_url = os.getenv("POSTGRES_DB")
db_user = os.getenv("POSTGRES_USER")
db_password = os.getenv("POSTGRES_PASSWORD")
db_host = os.getenv("POSTGRES_HOST")
db_port = os.getenv("POSTGRES_PORT")

def test_connection():
    try:
        # Attempt to connect to the PostgreSQL database
        conn = psycopg2.connect(
            dbname=db_url,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port
        )
        cur = conn.cursor()
        cur.execute("SELECT version();")
        db_version = cur.fetchone()
        print(f"Connected to PostgreSQL database. Version: {db_version}")
        
        # Display database name
        display_database_name(cur)

        # Close the connection
        cur.close()
        conn.close()
    except psycopg2.OperationalError as e:
        print(f"Unable to connect to the database. Error: {e}")

def display_database_name(cur):
    try:
        # Query to get the current database name
        cur.execute("SELECT current_database();")
        db_name = cur.fetchone()
        print(f"Connected to database: {db_name[0]}")
    except psycopg2.Error as e:
        print(f"Failed to retrieve database name. Error: {e}")

# Call the function to test the connection
if __name__ == "__main__":
    test_connection()
