"""
src/transform.py
----------------
Transformation layer of the ETL pipeline.

Responsibilities:
  - Clean raw data (nulls, duplicates, bad values)
  - Normalise formats (dates, text casing, whitespace)
  - Enforce correct dtypes
  - Add derived / computed columns
  - Validate business rules
  - Return a clean DataFrame ready for loading
"""

import logging
import numpy  as np
import pandas as pd

logger = logging.getLogger(__name__)

# ── Canonical value sets (used for validation) ────────────────────────────────
VALID_CATEGORIES = {
    "Electronics", "Clothing", "Home & Garden",
    "Sports", "Books", "Toys", "Beauty", "Food & Beverage",
}
VALID_PAYMENT_METHODS = {
    "Credit Card", "Debit Card", "PayPal", "Bank Transfer", "Gift Card",
}
VALID_SHIPPING_STATUSES = {
    "Delivered", "Shipped", "Processing", "Cancelled", "Returned",
}
VALID_REGIONS = {"North", "South", "East", "West", "Central"}

# ── Date format candidates (tried in order) ───────────────────────────────────
DATE_FORMATS = ["%Y-%m-%d", "%m/%d/%Y", "%d-%b-%Y"]


# ─────────────────────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────────────────────

def transform(df: pd.DataFrame) -> pd.DataFrame:
    """
    Run the full transformation pipeline on *df*.

    Parameters
    ----------
    df : pd.DataFrame
        Raw DataFrame produced by the extract stage.

    Returns
    -------
    pd.DataFrame
        Clean, enriched DataFrame ready for the load stage.
    """
    logger.info("─── TRANSFORM STAGE ─────────────────────────────────────")
    logger.info("Input shape: %d rows × %d columns", *df.shape)

    df = _normalise_column_names(df)
    df = _remove_duplicates(df)
    df = _clean_text_columns(df)
    df = _parse_dates(df)
    df = _cast_numerics(df)
    df = _handle_missing_values(df)
    df = _validate_values(df)
    df = _add_derived_columns(df)
    df = _final_column_order(df)

    logger.info("Transformation complete. Output shape: %d rows × %d columns", *df.shape)
    return df


# ─────────────────────────────────────────────────────────────────────────────
# Private helpers
# ─────────────────────────────────────────────────────────────────────────────

def _normalise_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Strip whitespace and lowercase all column headers."""
    df.columns = df.columns.str.strip().str.lower()
    logger.debug("Column names normalised.")
    return df


def _remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """Drop exact duplicate rows, keeping the first occurrence."""
    before = len(df)
    df = df.drop_duplicates()
    removed = before - len(df)
    if removed:
        logger.info("Duplicates removed: %d rows dropped.", removed)
    else:
        logger.info("No duplicate rows found.")
    return df.reset_index(drop=True)


def _clean_text_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Strip leading/trailing whitespace and apply Title Case to
    categorical string columns.
    """
    text_cols = ["category", "sub_category", "payment_method",
                 "shipping_status", "region", "state", "customer_name"]
    for col in text_cols:
        if col in df.columns:
            df[col] = (
                df[col]
                .astype(str)
                .str.strip()
                .str.title()
                .replace("Nan", np.nan)   # re-introduce NaN after str cast
            )
    logger.info("Text columns cleaned (whitespace stripped, Title Case applied).")
    return df


def _parse_dates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Parse order_date column that may contain mixed format strings.
    Falls back to NaT for unparseable values.
    """
    if "order_date" not in df.columns:
        return df

    parsed = pd.Series([pd.NaT] * len(df), dtype="datetime64[ns]")
    unparsed_mask = pd.Series([True] * len(df))

    for fmt in DATE_FORMATS:
        if not unparsed_mask.any():
            break
        trial = pd.to_datetime(
            df.loc[unparsed_mask, "order_date"],
            format=fmt,
            errors="coerce",
        )
        success = trial.notna()
        parsed[unparsed_mask & success.reindex(df.index, fill_value=False)] = \
            trial[success].values
        unparsed_mask = unparsed_mask & ~success.reindex(df.index, fill_value=False)

    nat_count = parsed.isna().sum()
    if nat_count:
        logger.warning("Could not parse %d order_date values → set to NaT.", nat_count)

    df["order_date"] = parsed
    logger.info("Date parsing complete. Valid dates: %d / %d.",
                parsed.notna().sum(), len(parsed))
    return df


def _cast_numerics(df: pd.DataFrame) -> pd.DataFrame:
    """Convert numeric columns to correct dtypes."""
    numeric_map = {
        "unit_price":   float,
        "quantity":     "Int64",    # nullable integer
        "discount_pct": float,
    }
    for col, dtype in numeric_map.items():
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype(dtype)
    logger.info("Numeric dtypes enforced.")
    return df


def _handle_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    """
    Strategy per column:
      - unit_price   → median imputation
      - quantity     → median imputation (cast to int)
      - discount_pct → fill 0 (assume no discount if unknown)
      - payment_method, shipping_status → fill 'Unknown'
      - customer_name → fill 'Anonymous'
      - order_date   → drop rows (date is critical)
    """
    before = len(df)

    # Drop rows where the primary key or date is missing
    df = df.dropna(subset=["order_id", "order_date"])
    dropped = before - len(df)
    if dropped:
        logger.info("Rows dropped (missing order_id or order_date): %d", dropped)

    # Numeric imputation
    for col in ["unit_price", "quantity"]:
        if col in df.columns:
            median_val = df[col].median()
            null_count = df[col].isna().sum()
            df[col] = df[col].fillna(median_val)
            if null_count:
                logger.info("'%s' – %d nulls filled with median (%.2f).",
                            col, null_count, median_val)

    if "discount_pct" in df.columns:
        null_count = df["discount_pct"].isna().sum()
        df["discount_pct"] = df["discount_pct"].fillna(0.0)
        if null_count:
            logger.info("'discount_pct' – %d nulls filled with 0.", null_count)

    # Categorical fill
    for col, fill_val in [("payment_method", "Unknown"),
                          ("shipping_status", "Unknown"),
                          ("customer_name",   "Anonymous")]:
        if col in df.columns:
            null_count = df[col].isna().sum()
            df[col] = df[col].fillna(fill_val)
            if null_count:
                logger.info("'%s' – %d nulls filled with '%s'.", col, null_count, fill_val)

    return df


def _validate_values(df: pd.DataFrame) -> pd.DataFrame:
    """
    Business rule enforcement:
      - unit_price must be > 0  (negative → absolute value; zero → median)
      - quantity must be >= 1
      - discount_pct clamped to [0, 100]
      - unknown categories / payment methods flagged as 'Other'
    """
    # Fix negative unit prices
    neg_mask = df["unit_price"] < 0
    if neg_mask.any():
        df.loc[neg_mask, "unit_price"] = df.loc[neg_mask, "unit_price"].abs()
        logger.info("Negative unit_price corrected (abs): %d rows.", neg_mask.sum())

    zero_mask = df["unit_price"] == 0
    if zero_mask.any():
        median_price = df.loc[~zero_mask, "unit_price"].median()
        df.loc[zero_mask, "unit_price"] = median_price
        logger.info("Zero unit_price replaced with median: %d rows.", zero_mask.sum())

    # Quantity floor
    qty_bad = df["quantity"] < 1
    if qty_bad.any():
        df.loc[qty_bad, "quantity"] = 1
        logger.info("Quantity < 1 corrected to 1: %d rows.", qty_bad.sum())

    # Discount clamp
    df["discount_pct"] = df["discount_pct"].clip(0, 100)

    # Unknown categories → 'Other'
    if "category" in df.columns:
        bad_cats = ~df["category"].isin(VALID_CATEGORIES)
        if bad_cats.any():
            df.loc[bad_cats, "category"] = "Other"
            logger.info("Unrecognised categories set to 'Other': %d rows.", bad_cats.sum())

    logger.info("Business rule validation complete.")
    return df


def _add_derived_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute new columns:
      - discount_amount  : monetary value of the discount
      - line_total       : revenue after discount  (unit_price × qty × (1 - disc%))
      - order_year       : year extracted from order_date
      - order_month      : month number
      - order_quarter    : Q1–Q4
      - order_year_month : 'YYYY-MM' string (useful for grouping)
      - is_returned      : boolean flag
    """
    # Ensure quantity is numeric (pandas nullable Int64 → float for arithmetic)
    qty = df["quantity"].astype(float)

    df["discount_amount"] = (df["unit_price"] * qty * df["discount_pct"] / 100).round(2)
    df["line_total"]      = (df["unit_price"] * qty - df["discount_amount"]).round(2)

    df["order_year"]       = df["order_date"].dt.year.astype("Int64")
    df["order_month"]      = df["order_date"].dt.month.astype("Int64")
    df["order_quarter"]    = df["order_date"].dt.quarter.astype("Int64")
    df["order_year_month"] = df["order_date"].dt.to_period("M").astype(str)

    df["is_returned"] = (
        df["shipping_status"].str.lower() == "returned"
    ).astype(bool)

    logger.info(
        "Derived columns added: discount_amount, line_total, order_year, "
        "order_month, order_quarter, order_year_month, is_returned."
    )
    return df


def _final_column_order(df: pd.DataFrame) -> pd.DataFrame:
    """Reorder columns for a clean, logical layout."""
    preferred = [
        "order_id", "customer_id", "customer_name",
        "category", "sub_category",
        "unit_price", "quantity", "discount_pct", "discount_amount", "line_total",
        "payment_method", "order_date", "order_year", "order_month",
        "order_quarter", "order_year_month",
        "shipping_status", "is_returned",
        "region", "state",
    ]
    existing = [c for c in preferred if c in df.columns]
    extras   = [c for c in df.columns if c not in preferred]
    return df[existing + extras]
