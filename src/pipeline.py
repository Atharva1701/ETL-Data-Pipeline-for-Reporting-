"""
src/pipeline.py
---------------
Orchestrates the full ETL pipeline:
  Extract → Transform → Load

Run directly:
    python src/pipeline.py [--skip-db]

Flags
-----
--skip-db   Run extract + transform only; skip the database load step.
            Useful when PostgreSQL is not running locally.
"""

import sys
import os
import logging
import argparse
import time
from datetime import datetime

# Make project root importable regardless of cwd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.config import (
    DATABASE_URL,
    RAW_CSV_FILE,
    PROCESSED_FILE,
    LOG_DIR,
    SQL_DIR,
    TABLE_SALES,
    CHUNK_SIZE,
    DB_CONNECT_RETRIES,
    DB_RETRY_DELAY_SEC,
)
from src.extract   import extract
from src.transform import transform
from src.load      import get_engine, create_schema, load, save_processed_csv


# ─────────────────────────────────────────────────────────────────────────────
# Logging setup
# ─────────────────────────────────────────────────────────────────────────────

def setup_logging() -> None:
    """Configure root logger to write to both console and a daily log file."""
    os.makedirs(LOG_DIR, exist_ok=True)
    timestamp  = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file   = os.path.join(LOG_DIR, f"pipeline_{timestamp}.log")

    fmt = "%(asctime)s  %(levelname)-8s  %(name)-20s  %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"

    logging.basicConfig(
        level=logging.INFO,
        format=fmt,
        datefmt=datefmt,
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(log_file, encoding="utf-8"),
        ],
    )
    logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)
    logging.info("Log file: %s", log_file)


# ─────────────────────────────────────────────────────────────────────────────
# Pipeline
# ─────────────────────────────────────────────────────────────────────────────

def run_pipeline(skip_db: bool = False) -> dict:
    """
    Execute the full ETL pipeline.

    Parameters
    ----------
    skip_db : bool
        If True, skip the PostgreSQL load step (dry-run / test mode).

    Returns
    -------
    dict
        Summary statistics for the run.
    """
    logger = logging.getLogger("pipeline")
    pipeline_start = time.time()

    logger.info("=" * 60)
    logger.info("  ETL PIPELINE STARTED")
    logger.info("  %s", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    logger.info("=" * 60)

    summary = {
        "status":          "UNKNOWN",
        "rows_extracted":  0,
        "rows_transformed": 0,
        "rows_loaded":     0,
        "duration_sec":    0.0,
    }

    try:
        # ── EXTRACT ───────────────────────────────────────────────────────────
        t0     = time.time()
        raw_df = extract(RAW_CSV_FILE)
        summary["rows_extracted"] = len(raw_df)
        logger.info("Extract finished in %.2f s.", time.time() - t0)

        # ── TRANSFORM ─────────────────────────────────────────────────────────
        t0         = time.time()
        clean_df   = transform(raw_df)
        summary["rows_transformed"] = len(clean_df)
        logger.info("Transform finished in %.2f s.", time.time() - t0)

        # Save processed CSV (always)
        save_processed_csv(clean_df, PROCESSED_FILE)

        # ── LOAD ──────────────────────────────────────────────────────────────
        if skip_db:
            logger.info("--skip-db flag set. Database load step SKIPPED.")
        else:
            t0     = time.time()
            schema = os.path.join(SQL_DIR, "schema.sql")
            engine = get_engine(DATABASE_URL, retries=DB_CONNECT_RETRIES,
                                delay=DB_RETRY_DELAY_SEC)
            create_schema(engine, schema)
            rows   = load(clean_df, engine, TABLE_SALES,
                          chunk_size=CHUNK_SIZE, if_exists="replace")
            summary["rows_loaded"] = rows
            logger.info("Load finished in %.2f s.", time.time() - t0)

        summary["status"] = "SUCCESS"

    except Exception as exc:
        summary["status"] = "FAILED"
        logger.exception("Pipeline FAILED with exception: %s", exc)
        raise

    finally:
        summary["duration_sec"] = round(time.time() - pipeline_start, 2)
        logger.info("=" * 60)
        logger.info("  PIPELINE %s", summary["status"])
        logger.info("  Rows extracted  : %d", summary["rows_extracted"])
        logger.info("  Rows transformed: %d", summary["rows_transformed"])
        logger.info("  Rows loaded     : %d", summary["rows_loaded"])
        logger.info("  Duration        : %.2f s", summary["duration_sec"])
        logger.info("=" * 60)

    return summary


# ─────────────────────────────────────────────────────────────────────────────
# CLI entry-point
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the Sales ETL pipeline.")
    parser.add_argument(
        "--skip-db",
        action="store_true",
        help="Extract and transform only; skip PostgreSQL load.",
    )
    args = parser.parse_args()

    setup_logging()
    run_pipeline(skip_db=args.skip_db)
