import pandas as pd
import sys
sys.stdout.reconfigure(encoding='utf-8')

# define raw data locations
RAW_CSV  = "data/raw/csv"
RAW_DB   = "data/raw/db"
RAW_API  = "data/raw/api"

# dataset name → (file path, primary key column(s))
DATASETS = {
    "customers"       : (f"{RAW_CSV}/customers.csv",        "customer_id"),
    "orders"          : (f"{RAW_CSV}/orders.csv",           "order_id"),
    "products"        : (f"{RAW_CSV}/products.csv",         "product_id"),
    "order_items"     : (f"{RAW_CSV}/order_items.csv",      ["order_id", "order_item_id"]),
    "sellers"         : (f"{RAW_DB}/sellers.csv",           "seller_id"),
    "inventory"       : (f"{RAW_DB}/inventory.csv",         "product_id"),
    "product_metadata": (f"{RAW_API}/product_metadata.csv", "product_id"),
}


def profile(name, path, primary_key):
    print("="   * 60)
    print(f"DATASET : {name}")
    print(f"FILE    : {path}")
    print("=" * 60)

    df = pd.read_csv(path)

    # 1. shape
    print(f"\n[1] SHAPE")
    print(f"    Rows    : {df.shape[0]}")
    print(f"    Columns : {df.shape[1]}")

    # 2. column names and data types
    print(f"\n[2] COLUMNS & DATA TYPES")
    for col, dtype in df.dtypes.items():
        print(f"    {col:<40} {dtype}")

    # 3. null counts per column
    print(f"\n[3] NULL COUNTS PER COLUMN")
    null_counts = df.isnull().sum()
    for col, count in null_counts.items():
        flag = " << HAS NULLS" if count > 0 else ""
        print(f"    {col:<40} {count}{flag}")

    # 4. duplicate check on primary key
    print(f"\n[4] DUPLICATE CHECK ON PRIMARY KEY")
    if isinstance(primary_key, list):
        dup_count = df.duplicated(primary_key).sum()
        print(f"    Primary key : {primary_key}")
    else:
        dup_count = df.duplicated(primary_key).sum()
        print(f"    Primary key : {primary_key}")
    flag = " << HAS DUPLICATES" if dup_count > 0 else ""
    print(f"    Duplicates  : {dup_count}{flag}")

    # 5. sample rows
    print(f"\n[5] SAMPLE ROWS (first 3)")
    print(df.head(3).to_string())

    print()


if __name__ == "__main__":
    for name, (path, pk) in DATASETS.items():
        profile(name, path, pk)
