import duckdb

# Step 1: Set Up Persistent Database (so we can access it again later) and Load Data

# Connect to a persistent DuckDB database
con = duckdb.connect(database="rfm_analysis.duckdb")

# File paths for the datasets (Don't forget to change your pathfiles to match where you stored your data)
customers_parquet = r"C:\Users\busca\RFM_Analysis_Olist\data\customers.parquet"
orders_parquet = r"C:\Users\busca\RFM_Analysis_Olist\data\orders.parquet"
products_parquet = r"C:\Users\busca\RFM_Analysis_Olist\data\products.parquet"

# Load datasets into the database
con.execute(
    f"CREATE TABLE IF NOT EXISTS customers AS SELECT * FROM '{customers_parquet}'"
)
con.execute(f"CREATE TABLE IF NOT EXISTS orders AS SELECT * FROM '{orders_parquet}'")
con.execute(
    f"CREATE TABLE IF NOT EXISTS products AS SELECT * FROM '{products_parquet}'"
)

# Verify tables are loaded
print(con.execute("SHOW TABLES").fetchall())


rfm_query = """
SELECT 
    c.customer_unique_id,
    MAX(o.order_purchase_timestamp) AS last_purchase_date,
    COUNT(DISTINCT o.order_id) AS frequency,
    SUM(oi.price) AS monetary
FROM 
    customers c
JOIN 
    orders o ON c.customer_id = o.customer_id
JOIN 
    order_items oi ON o.order_id = oi.order_id
WHERE 
    o.order_status = 'delivered'
GROUP BY 
    c.customer_unique_id
"""

# Execute the query and fetch results
rfm_results = con.execute(rfm_query).df()  # Convert result to pandas DataFrame
print(rfm_results.head())


import pandas as pd

# Define current date
current_date = pd.Timestamp.now()

# Add recency calculation to the RFM DataFrame
rfm_results["recency"] = (
    current_date - pd.to_datetime(rfm_results["last_purchase_date"])
).dt.days

# Drop last_purchase_date (optional)
rfm_results = rfm_results.drop(columns=["last_purchase_date"])

print(rfm_results.head())

rfm_results.to_csv("rfm_results.csv", index=False)
