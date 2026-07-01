from sqlalchemy import create_engine
import pandas as pd
import os
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Read database configuration from environment variables
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("POSTGRES_DB")
DB_USER = os.getenv("POSTGRES_USER")
DB_PASS = os.getenv("POSTGRES_PASSWORD")

# Validate that all required database configurations are available
if not (DB_HOST and DB_PORT and DB_NAME and DB_USER and DB_PASS):
    raise RuntimeError("Database configuration must be provided via environment variables")

# Create PostgreSQL database connection
engine = create_engine(
    f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

# Define output directory for extracted raw database files
OUTPUT_PATH = "/app/data/raw/db"

# Define file path used to store the last successful extraction timestamp
WATERMARK_FILE = "/app/data/raw/db/.watermark"

# Create output directory if it does not already exist
os.makedirs(OUTPUT_PATH, exist_ok=True)

# Read last watermark
if os.path.exists(WATERMARK_FILE):
    with open(WATERMARK_FILE, "r") as f:
        last_updated = f.read().strip()
    logger.info(f"Incremental load from watermark: {last_updated}")
else:
    last_updated = "1900-01-01"
    logger.info("No watermark found, doing full load")

# Extracting Data From PostgreSQL
sellers_df = pd.read_sql("SELECT * FROM sellers", engine)
inventory_df = pd.read_sql(
    f"SELECT * FROM inventory WHERE last_updated > '{last_updated}'", engine
)

logger.info(f"Extracted {len(sellers_df)} sellers rows")
logger.info(f"Extracted {len(inventory_df)} inventory rows since {last_updated}")

# If no new inventory records, fall back to full load
if len(inventory_df) == 0:
    logger.warning("No new inventory records found, falling back to full load")
    inventory_df = pd.read_sql("SELECT * FROM inventory", engine)
    logger.info(f"Full load extracted {len(inventory_df)} inventory rows")

# Save CSV
sellers_df.to_csv(f"{OUTPUT_PATH}/sellers.csv", index=False)
inventory_df.to_csv(f"{OUTPUT_PATH}/inventory.csv", index=False)

# Update watermark
new_watermark = datetime.today().strftime("%Y-%m-%d")
with open(WATERMARK_FILE, "w") as f:
    f.write(new_watermark)
logger.info(f"Watermark updated to: {new_watermark}")

logger.info("Database extraction completed successfully.")