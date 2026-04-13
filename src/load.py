"""
src/load.py
-----------
Load layer of the ETL pipeline.

Responsibilities:
  - Create the PostgreSQL schema (idempotent)
  - Insert clean data in batches using SQLAlchemy
  - Retry connection on transient errors
  - Persist a processed CSV alongside the DB load
"""

import time
import logging
import os

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError, SQLAlchemyError

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Connection
# ─────────────────────────────────────────────────────────────────────────────

def get_engine(database_url: str, retries: int = 3, delay: int = 5):
    """
    Create and return a SQLAlchemy engine, retrying on connection failure.

    Parameters
    ----------
    database_url : str  – SQLAlchemy connection string
    retries      : int  – max attempts
    delay        : int  – seconds between attempts

    Returns
    -------
    sqlalchemy.Engine
    """
    for attempt in range(1, retries + 1):
        try:
            engine = create_engine(database_url, pool_pre_ping=True)
            # Test the connection
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info("Database connection established (attempt %d/%d).", attempt, retries)
            return engine
        except OperationalError as exc:
            logger.warning(
                "Connection attempt %d/%d failed: %s", attempt, retries, exc
            )
            if attempt < retries:
                logger.info("Retrying in %d seconds …", delay)
                time.sleep(delay)
            else:
                logger.error("All %d connection attempts failed.", retries)
                raise


# ─────────────────────────────────────────────────────────────────────────────
# Schema creation
# ─────────────────────────────────────────────────────────────────────────────

def create_schema(engine, schema_path: str) -> None:
    """
    Execute schema.sql to create tables if they don't already exist.

    Parameters
    ----------
    engine      : sqlalchemy.Engine
    schema_path : str – path to sql/schema.sql
    """
    if not os.path.exists(schema_path):
        raise FileNotFoundError(f"Schema file not found: {schema_path}")

    with open(schema_path, "r") as fh:
        sql = fh.read()

    try:
        with engine.begin() as conn:
            # Execute each statement separately for compatibility
            statements = [s.strip() for s in sql.split(";") if s.strip()]
            for stmt in statements:
                conn.execute(text(stmt))
        logger.info("Schema created / verified from: %s", schema_path)
    except SQLAlchemyError as exc:
        logger.error("Schema creation failed: %s", exc)
        raise


# ─────────────────────────────────────────────────────────────────────────────
# Data loading
# ─────────────────────────────────────────────────────────────────────────────

def load(
    df: pd.DataFrame,
    engine,
    table_name: str,
    chunk_size: int = 5_000,
    if_exists: str = "append",
) -> int:
    """
    Insert *df* into *table_name* in batches of *chunk_size*.

    Parameters
    ----------
    df         : pd.DataFrame  – clean data from transform stage
    engine     : sqlalchemy.Engine
    table_name : str           – target PostgreSQL table
    chunk_size : int           – rows per INSERT batch
    if_exists  : str           – 'append' (default) or 'replace'

    Returns
    -------
    int
        Total rows inserted.
    """
    logger.info("─── LOAD STAGE ──────────────────────────────────────────")
    logger.info("Target table : %s", table_name)
    logger.info("Rows to load : %d", len(df))
    logger.info("Chunk size   : %d", chunk_size)
    logger.info("if_exists    : %s", if_exists)

    if df.empty:
        logger.warning("DataFrame is empty – nothing to load.")
        return 0

    # Prepare dtypes for PostgreSQL compatibility
    df = _prepare_for_db(df)

    total_inserted = 0
    n_chunks = (len(df) // chunk_size) + (1 if len(df) % chunk_size else 0)

    for i, start in enumerate(range(0, len(df), chunk_size), 1):
        chunk = df.iloc[start : start + chunk_size]
        try:
            chunk.to_sql(
                name=table_name,
                con=engine,
                if_exists=if_exists if i == 1 else "append",
                index=False,
                method="multi",
            )
            total_inserted += len(chunk)
            logger.info(
                "Chunk %d/%d inserted (%d rows). Running total: %d.",
                i, n_chunks, len(chunk), total_inserted,
            )
        except SQLAlchemyError as exc:
            logger.error("Failed to insert chunk %d: %s", i, exc)
            raise

    logger.info("Load complete. Total rows inserted: %d", total_inserted)
    return total_inserted


def save_processed_csv(df: pd.DataFrame, output_path: str) -> None:
    """
    Persist the cleaned DataFrame to a CSV file in data/processed/.

    Parameters
    ----------
    df          : pd.DataFrame
    output_path : str
    """
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=False)
    size_mb = os.path.getsize(output_path) / (1024 ** 2)
    logger.info("Processed CSV saved → %s (%.2f MB, %d rows)", output_path, size_mb, len(df))


# ─────────────────────────────────────────────────────────────────────────────
# Internal helpers
# ─────────────────────────────────────────────────────────────────────────────

def _prepare_for_db(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert pandas-specific nullable types and period types to standard
    Python / PostgreSQL-compatible types.
    """
    df = df.copy()

    for col in df.select_dtypes(include=["Int64", "Int32"]).columns:
        df[col] = df[col].astype("float64")  # NaN-safe integer → float

    # Period dtype → string
    for col in df.columns:
        if hasattr(df[col], "dt") and hasattr(df[col].dt, "freqstr"):
            df[col] = df[col].astype(str)
        if str(df[col].dtype) == "period[M]":
            df[col] = df[col].astype(str)

    return df
