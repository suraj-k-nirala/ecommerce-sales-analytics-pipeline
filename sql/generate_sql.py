import pandas as pd
import random
import os

RAW_CSV_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "raw", "csv")

sellers_df = pd.read_csv(os.path.join(RAW_CSV_DIR, "sellers.csv"))
products_df = pd.read_csv(os.path.join(RAW_CSV_DIR, "products.csv"))
order_items_df = pd.read_csv(os.path.join(RAW_CSV_DIR, "order_items.csv"))

# Get unique product_ids from order_items
unique_products = order_items_df["product_id"].unique()[:500]  # limit to 500

sql = ""

sql += """-- Drop and recreate sellers table
DROP TABLE IF EXISTS sellers CASCADE;
CREATE TABLE sellers (
    seller_id VARCHAR(50) PRIMARY KEY,
    seller_name VARCHAR(100),
    city VARCHAR(50),
    state VARCHAR(50)
);

"""

sql += "INSERT INTO sellers (seller_id, seller_name, city, state) VALUES\n"
rows = []
for _, row in sellers_df.iterrows():
    seller_id = str(row["seller_id"]).replace("'", "''")
    seller_name = f"Seller_{seller_id[:8]}"
    city = str(row["seller_city"]).replace("'", "''")
    state = str(row["seller_state"]).replace("'", "''")
    rows.append(f"('{seller_id}', '{seller_name}', '{city}', '{state}')")

sql += ",\n".join(rows) + ";\n\n"

sql += """-- Drop and recreate inventory table
DROP TABLE IF EXISTS inventory CASCADE;
CREATE TABLE inventory (
    product_id VARCHAR(50),
    stock_quantity INT,
    warehouse_location VARCHAR(50),
    last_updated DATE
);

"""

sql += "INSERT INTO inventory (product_id, stock_quantity, warehouse_location, last_updated) VALUES\n"
rows = []
warehouses = ["WH1", "WH2", "WH3", "WH4", "WH5"]
for product_id in unique_products:
    product_id = str(product_id).replace("'", "''")
    stock = random.randint(10, 500)
    warehouse = random.choice(warehouses)
    rows.append(f"('{product_id}', {stock}, '{warehouse}', '2017-01-01')")

sql += ",\n".join(rows) + ";\n"

# Write to file
output_path = os.path.join(os.path.dirname(__file__), "source_db_init.sql")
with open(output_path, "w", encoding="utf-8") as f:
    f.write(sql)

print("SQL file generated successfully!")
print(f"Sellers: {len(sellers_df)} rows")
print(f"Inventory: {len(unique_products)} rows")
