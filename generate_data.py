"""
generate_data.py
----------------
Generates a realistic synthetic e-commerce sales dataset (~15,000 rows)
and saves it to data/raw/sales_data.csv.

Intentionally introduces:
  - Missing values (~3-5% per column)
  - Duplicate rows (~1%)
  - Inconsistent casing / whitespace in text columns
  - Out-of-range / negative values (to demonstrate validation)
  - Mixed date formats
"""

import os
import random
import numpy as np
import pandas as pd
from datetime import datetime, timedelta

# ── Reproducibility ─────────────────────────────────────────────────────────
SEED = 42
random.seed(SEED)
np.random.seed(SEED)

# ── Constants ────────────────────────────────────────────────────────────────
N_ROWS = 15_000
OUTPUT_PATH = os.path.join(os.path.dirname(__file__), "data", "raw", "sales_data.csv")

CATEGORIES = ["Electronics", "Clothing", "Home & Garden", "Sports", "Books", "Toys", "Beauty", "Food & Beverage"]
SUBCATEGORIES = {
    "Electronics":      ["Laptops", "Smartphones", "Tablets", "Headphones", "Cameras"],
    "Clothing":         ["Men's Wear", "Women's Wear", "Kids' Wear", "Footwear", "Accessories"],
    "Home & Garden":    ["Furniture", "Kitchenware", "Bedding", "Garden Tools", "Lighting"],
    "Sports":           ["Fitness Equipment", "Outdoor Gear", "Team Sports", "Water Sports", "Cycling"],
    "Books":            ["Fiction", "Non-Fiction", "Academic", "Comics", "Children's"],
    "Toys":             ["Action Figures", "Board Games", "Puzzles", "Dolls", "STEM Toys"],
    "Beauty":           ["Skincare", "Haircare", "Makeup", "Fragrances", "Personal Care"],
    "Food & Beverage":  ["Snacks", "Beverages", "Organic", "Supplements", "Coffee & Tea"],
}
PAYMENT_METHODS = ["Credit Card", "Debit Card", "PayPal", "Bank Transfer", "Gift Card"]
SHIPPING_STATUS = ["Delivered", "Shipped", "Processing", "Cancelled", "Returned"]
REGIONS = ["North", "South", "East", "West", "Central"]
STATES = {
    "North":   ["New York", "New Jersey", "Pennsylvania", "Connecticut", "Massachusetts"],
    "South":   ["Texas", "Florida", "Georgia", "North Carolina", "Virginia"],
    "East":    ["Ohio", "Michigan", "Indiana", "Illinois", "Wisconsin"],
    "West":    ["California", "Washington", "Oregon", "Nevada", "Arizona"],
    "Central": ["Colorado", "Minnesota", "Missouri", "Kansas", "Iowa"],
}

FIRST_NAMES = ["James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda",
               "William", "Barbara", "David", "Susan", "Richard", "Jessica", "Joseph", "Sarah",
               "Thomas", "Karen", "Charles", "Lisa", "Christopher", "Nancy", "Daniel", "Betty",
               "Matthew", "Margaret", "Anthony", "Sandra", "Donald", "Ashley", "Mark", "Emily"]
LAST_NAMES  = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
               "Wilson", "Taylor", "Anderson", "Thomas", "Jackson", "White", "Harris", "Martin",
               "Thompson", "Young", "Robinson", "Lewis", "Walker", "Allen", "King", "Wright"]

# ── Helpers ──────────────────────────────────────────────────────────────────

def random_date(start: datetime, end: datetime) -> datetime:
    delta = end - start
    return start + timedelta(seconds=random.randint(0, int(delta.total_seconds())))


def format_date_mixed(dt: datetime, idx: int) -> str:
    """Introduce three different date formats to mimic real-world messiness."""
    fmt_choice = idx % 3
    if fmt_choice == 0:
        return dt.strftime("%Y-%m-%d")          # ISO format
    elif fmt_choice == 1:
        return dt.strftime("%m/%d/%Y")          # US format
    else:
        return dt.strftime("%d-%b-%Y")          # e.g. 14-Mar-2023


def inject_noise(series: pd.Series, null_rate: float = 0.04) -> pd.Series:
    """Randomly replace some values with NaN."""
    mask = np.random.rand(len(series)) < null_rate
    series = series.astype(object)
    series[mask] = np.nan
    return series


def messy_category(value: str) -> str:
    """Randomly apply inconsistent casing or extra whitespace."""
    r = random.random()
    if r < 0.10:
        return value.upper()
    elif r < 0.20:
        return value.lower()
    elif r < 0.25:
        return "  " + value + "  "
    return value

# ── Build Base Dataset ────────────────────────────────────────────────────────

print(f"Generating {N_ROWS:,} rows …")

order_ids   = [f"ORD-{100000 + i}" for i in range(N_ROWS)]
customer_ids = [f"CUST-{random.randint(1000, 5999)}" for _ in range(N_ROWS)]
customer_names = [
    f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}" for _ in range(N_ROWS)
]

categories   = [random.choice(CATEGORIES) for _ in range(N_ROWS)]
subcategories = [random.choice(SUBCATEGORIES[cat]) for cat in categories]

unit_prices  = np.round(np.random.uniform(5.0, 1500.0, N_ROWS), 2)
quantities   = np.random.randint(1, 10, N_ROWS)

start_dt = datetime(2022, 1, 1)
end_dt   = datetime(2024, 6, 30)
order_dates = [random_date(start_dt, end_dt) for _ in range(N_ROWS)]

payment_methods = [random.choice(PAYMENT_METHODS) for _ in range(N_ROWS)]
shipping_statuses = [random.choice(SHIPPING_STATUS) for _ in range(N_ROWS)]

regions = [random.choice(REGIONS) for _ in range(N_ROWS)]
states  = [random.choice(STATES[r]) for r in regions]

discount_pct = np.round(np.random.choice([0, 5, 10, 15, 20, 25], N_ROWS,
                                          p=[0.50, 0.15, 0.15, 0.10, 0.07, 0.03]), 2)

df = pd.DataFrame({
    "order_id":        order_ids,
    "customer_id":     customer_ids,
    "customer_name":   customer_names,
    "category":        categories,
    "sub_category":    subcategories,
    "unit_price":      unit_prices,
    "quantity":        quantities,
    "discount_pct":    discount_pct,
    "payment_method":  payment_methods,
    "order_date":      [format_date_mixed(d, i) for i, d in enumerate(order_dates)],
    "shipping_status": shipping_statuses,
    "region":          regions,
    "state":           states,
})

# ── Inject Messiness ──────────────────────────────────────────────────────────

# Inconsistent casing on text columns
df["category"]        = df["category"].apply(messy_category)
df["payment_method"]  = df["payment_method"].apply(lambda v: messy_category(v))
df["shipping_status"] = df["shipping_status"].apply(lambda v: messy_category(v))

# Missing values
for col, rate in [("customer_name", 0.03), ("unit_price", 0.04),
                  ("quantity", 0.03), ("discount_pct", 0.05),
                  ("payment_method", 0.04), ("shipping_status", 0.03)]:
    df[col] = inject_noise(df[col], null_rate=rate)

# A few negative unit prices (bad data)
bad_idx = np.random.choice(df.index, size=40, replace=False)
df.loc[bad_idx, "unit_price"] = -df.loc[bad_idx, "unit_price"]

# Duplicate rows (~1%)
n_dupes = int(N_ROWS * 0.01)
dupe_rows = df.sample(n=n_dupes, random_state=SEED)
df = pd.concat([df, dupe_rows], ignore_index=True)

df = df.sample(frac=1, random_state=SEED).reset_index(drop=True)

# ── Save ──────────────────────────────────────────────────────────────────────
os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
df.to_csv(OUTPUT_PATH, index=False)
print(f"Dataset saved → {OUTPUT_PATH}  ({len(df):,} rows × {len(df.columns)} cols)")
