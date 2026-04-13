"""
config/config.py
----------------
Central configuration for the ETL pipeline.
All sensitive values are read from environment variables first;
the hard-coded defaults are for local Docker development only.
"""

import os

# ── Database ──────────────────────────────────────────────────────────────────
DB_CONFIG = {
    "host":     os.getenv("DB_HOST",     "localhost"),
    "port":     int(os.getenv("DB_PORT", "5432")),
    "database": os.getenv("DB_NAME",     "sales_db"),
    "user":     os.getenv("DB_USER",     "etl_user"),
    "password": os.getenv("DB_PASSWORD", "etl_password"),
}

# SQLAlchemy connection string (used by pandas.to_sql and load.py)
DATABASE_URL = (
    "postgresql+psycopg2://"
    f"{DB_CONFIG['user']}:{DB_CONFIG['password']}"
    f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}"
    f"/{DB_CONFIG['database']}"
)

# ── File Paths ────────────────────────────────────────────────────────────────
BASE_DIR        = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DATA_DIR    = os.path.join(BASE_DIR, "data", "raw")
PROCESSED_DIR   = os.path.join(BASE_DIR, "data", "processed")
LOG_DIR         = os.path.join(BASE_DIR, "logs")
SQL_DIR         = os.path.join(BASE_DIR, "sql")

RAW_CSV_FILE    = os.path.join(RAW_DATA_DIR,  "sales_data.csv")
PROCESSED_FILE  = os.path.join(PROCESSED_DIR, "sales_data_clean.csv")

# ── Pipeline Settings ─────────────────────────────────────────────────────────
CHUNK_SIZE          = 5_000       # rows per DB insert batch
DB_CONNECT_RETRIES  = 3           # retry attempts on connection failure
DB_RETRY_DELAY_SEC  = 5           # seconds between retries

# ── Table Names ───────────────────────────────────────────────────────────────
TABLE_SALES         = "sales_fact"
TABLE_CUSTOMERS     = "dim_customers"
TABLE_PRODUCTS      = "dim_products"
