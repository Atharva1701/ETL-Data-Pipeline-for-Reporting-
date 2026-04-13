"""
src/extract.py
--------------
Extraction layer of the ETL pipeline.

Responsibilities:
  - Read raw CSV data from disk
  - Perform basic file-level validation
  - Return a raw DataFrame for the transform stage
"""

import os
import logging
import pandas as pd

logger = logging.getLogger(__name__)

# Columns we expect in the raw file – used for schema validation
EXPECTED_COLUMNS = {
    "order_id", "customer_id", "customer_name",
    "category", "sub_category",
    "unit_price", "quantity", "discount_pct",
    "payment_method", "order_date",
    "shipping_status", "region", "state",
}


def extract(filepath: str) -> pd.DataFrame:
    """
    Read a CSV file from *filepath* and return a raw DataFrame.

    Parameters
    ----------
    filepath : str
        Absolute or relative path to the source CSV file.

    Returns
    -------
    pd.DataFrame
        Raw, unprocessed data ready for the transform stage.

    Raises
    ------
    FileNotFoundError
        If *filepath* does not exist.
    ValueError
        If the file is empty or missing required columns.
    RuntimeError
        On any unexpected read error.
    """
    logger.info("─── EXTRACT STAGE ───────────────────────────────────────")
    logger.info("Source file: %s", filepath)

    # ── 1. File existence check ───────────────────────────────────────────────
    if not os.path.exists(filepath):
        logger.error("File not found: %s", filepath)
        raise FileNotFoundError(f"Raw data file not found: {filepath}")

    file_size_mb = os.path.getsize(filepath) / (1024 ** 2)
    logger.info("File size: %.2f MB", file_size_mb)

    # ── 2. Read CSV ───────────────────────────────────────────────────────────
    try:
        df = pd.read_csv(filepath, dtype=str)   # read everything as str first
        logger.info("Rows read: %d | Columns: %d", len(df), len(df.columns))
    except pd.errors.EmptyDataError:
        logger.error("CSV file is empty: %s", filepath)
        raise ValueError(f"CSV file is empty: {filepath}")
    except Exception as exc:
        logger.error("Unexpected error reading %s: %s", filepath, exc)
        raise RuntimeError(f"Failed to read CSV: {exc}") from exc

    # ── 3. Empty-file guard ───────────────────────────────────────────────────
    if df.empty:
        logger.error("DataFrame is empty after reading.")
        raise ValueError("Extracted DataFrame contains no rows.")

    # ── 4. Column schema validation ───────────────────────────────────────────
    actual_cols   = set(df.columns.str.strip().str.lower())
    missing_cols  = EXPECTED_COLUMNS - actual_cols
    if missing_cols:
        logger.error("Missing expected columns: %s", missing_cols)
        raise ValueError(f"Source file is missing columns: {missing_cols}")

    logger.info("Schema validation passed. All expected columns present.")

    # ── 5. Snapshot stats ─────────────────────────────────────────────────────
    null_summary = df.isnull().sum()
    null_cols    = null_summary[null_summary > 0]
    if not null_cols.empty:
        logger.info("Null value counts per column (raw):\n%s", null_cols.to_string())

    logger.info("Extraction complete. %d rows extracted.", len(df))
    return df


def extract_in_chunks(filepath: str, chunksize: int = 5_000):
    """
    Generator that yields DataFrames in chunks.
    Useful for very large files that don't fit in memory.

    Parameters
    ----------
    filepath  : str   – path to source CSV
    chunksize : int   – rows per chunk

    Yields
    ------
    pd.DataFrame
    """
    logger.info("Extracting '%s' in chunks of %d rows.", filepath, chunksize)
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"Raw data file not found: {filepath}")
    try:
        for chunk in pd.read_csv(filepath, dtype=str, chunksize=chunksize):
            yield chunk
    except Exception as exc:
        logger.error("Chunk-read error: %s", exc)
        raise RuntimeError(f"Failed during chunk extraction: {exc}") from exc
