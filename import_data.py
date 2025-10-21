import os
import pandas as pd
import sqlalchemy
import urllib

# Load CSV files
try:
    brands = pd.read_csv('brand_detail.csv')
    spend = pd.read_csv('daily_spend.csv')
    print("CSV files loaded successfully")
except Exception as e:
    print("Failed to load CSVs:", e)
    raise

# Get DB credentials from environment variables
server = "dataman.database.windows.net"
database = "DataManagement"
username = os.getenv("SQL_USERNAME")
password = os.getenv("SQL_PASSWORD")

# Build connection string
conn_str = (
    f"Driver={{ODBC Driver 18 for SQL Server}};"
    f"Server={server};"
    f"Database={database};"
    f"Uid={username};"
    f"Pwd={password};"
    f"Encrypt=yes;"
    f"TrustServerCertificate=yes;"
    f"Connection Timeout=30;"
)

# Encode connection string and create SQLAlchemy engine
params = urllib.parse.quote_plus(conn_str)
engine = sqlalchemy.create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

# Upload data to Azure SQL
try:
    print("Uploading CSV data to Azure SQL...")
    brands.to_sql('BrandDetail', con=engine, if_exists='replace', index=False)
    spend.to_sql('DailySpend', con=engine, if_exists='replace', index=False)
    print("Data uploaded successfully!")
except Exception as e:
    print("Failed to upload data:", e)
    raise
