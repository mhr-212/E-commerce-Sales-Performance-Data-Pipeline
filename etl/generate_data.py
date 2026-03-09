"""
Synthetic data generator for the e-commerce sales pipeline.

Generates 5 years (2020-2024) of realistic transaction data:
  - ~100 products across 6 categories
  - ~50,000 customers across 5 regions
  - ~1,000-3,000 orders per day (with seasonality) → ~5M rows total

Output: chunked CSVs at data/raw/sales_YYYY_MM.csv
Run:    python -m etl.generate_data [--rows 5000000] [--out-dir data/raw]
"""
from __future__ import annotations

import argparse
import os
import random
import uuid
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
from faker import Faker

from etl.utils.logger import get_logger

logger = get_logger(__name__)
fake = Faker()
rng = np.random.default_rng(seed=42)

# ---------------------------------------------------------------------------
# Reference master data
# ---------------------------------------------------------------------------

CATEGORIES = {
    "Electronics":   ["Smartphones", "Laptops", "Tablets", "Accessories"],
    "Clothing":      ["Men's", "Women's", "Kids'", "Sportswear"],
    "Home & Garden": ["Furniture", "Decor", "Garden Tools", "Kitchen"],
    "Books":         ["Fiction", "Non-Fiction", "Technical", "Children's"],
    "Sports":        ["Fitness", "Outdoor", "Team Sports", "Water Sports"],
    "Beauty":        ["Skincare", "Makeup", "Haircare", "Fragrance"],
}

CHANNELS   = ["online", "mobile_app", "marketplace", "in_store"]
SEGMENTS   = ["Consumer", "Corporate", "Home Office"]
REGIONS    = {
    "North America": ["United States", "Canada", "Mexico"],
    "Europe":        ["United Kingdom", "Germany", "France", "Spain"],
    "Asia Pacific":  ["Japan", "Australia", "India", "Singapore"],
    "Latin America": ["Brazil", "Argentina", "Colombia"],
    "Middle East":   ["UAE", "Saudi Arabia", "Turkey"],
}

# Seasonal multipliers: index 0 = Jan
SEASONAL = np.array([0.80, 0.75, 0.90, 0.95, 1.00, 1.05,
                     1.10, 1.10, 1.05, 1.15, 1.30, 1.60])

# ---------------------------------------------------------------------------
# Generate dimension data
# ---------------------------------------------------------------------------

def build_products(n: int = 100) -> pd.DataFrame:
    rows = []
    prod_num = 1
    for cat, subs in CATEGORIES.items():
        per_sub = max(1, n // (len(CATEGORIES) * len(subs)))
        for sub in subs:
            for _ in range(per_sub):
                cost  = round(rng.uniform(5, 500), 2)
                price = round(cost * rng.uniform(1.15, 2.5), 2)
                rows.append({
                    "product_id":   f"PROD-{prod_num:05d}",
                    "product_name": fake.catch_phrase()[:60],
                    "category":     cat,
                    "subcategory":  sub,
                    "brand":        fake.company()[:40],
                    "unit_cost":    cost,
                    "unit_price":   price,
                })
                prod_num += 1
    return pd.DataFrame(rows)


def build_customers(n: int = 50_000) -> pd.DataFrame:
    rows = []
    for i in range(1, n + 1):
        region  = random.choice(list(REGIONS.keys()))
        country = random.choice(REGIONS[region])
        signup  = fake.date_between(start_date=date(2018, 1, 1), end_date=date(2020, 12, 31))
        rows.append({
            "customer_id": f"CUST-{i:07d}",
            "first_name":  fake.first_name(),
            "last_name":   fake.last_name(),
            "email":       fake.email(),
            "segment":     random.choice(SEGMENTS),
            "country":     country,
            "region":      region,
            "city":        fake.city()[:60],
            "signup_date": signup,
        })
        if i % 10_000 == 0:
            logger.info(f"[generate] customers: {i}/{n}")
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Generate transactions day-by-day
# ---------------------------------------------------------------------------

def _daily_rows(
    day: date,
    products: pd.DataFrame,
    customers: pd.DataFrame,
    base_orders: int = 1500,
) -> pd.DataFrame:
    """Return a DataFrame of individual line items for a single day."""
    seasonal_factor = SEASONAL[day.month - 1]
    # Weekend bump
    weekday_factor = 1.2 if day.weekday() >= 5 else 1.0
    n_orders = max(50, int(rng.poisson(base_orders * seasonal_factor * weekday_factor)))

    order_ids    = [str(uuid.uuid4()) for _ in range(n_orders)]
    cust_ids     = customers["customer_id"].sample(n=n_orders, replace=True).values
    line_counts  = rng.integers(1, 5, size=n_orders)   # 1-4 lines per order

    records = []
    for i, (oid, cid, nlc) in enumerate(zip(order_ids, cust_ids, line_counts)):
        prod_sample = products.sample(n=nlc, replace=False)
        for _, prod in prod_sample.iterrows():
            qty      = int(rng.integers(1, 11))
            disc_pct = float(round(rng.choice([0, 0.05, 0.10, 0.15, 0.20, 0.25],
                                              p=[0.55, 0.15, 0.12, 0.08, 0.06, 0.04]), 2))
            gross    = round(qty * prod["unit_price"], 4)
            disc_amt = round(gross * disc_pct, 4)
            net      = round(gross - disc_amt, 4)
            records.append({
                "sale_id":       str(uuid.uuid4()),
                "order_id":      oid,
                "sale_date":     day,
                "product_id":    prod["product_id"],
                "customer_id":   cid,
                "quantity":      qty,
                "unit_price":    prod["unit_price"],
                "discount_pct":  disc_pct,
                "gross_amount":  gross,
                "discount_amount": disc_amt,
                "net_amount":    net,
                "shipping_cost": round(float(rng.uniform(0, 15)), 2),
                "return_flag":   bool(rng.random() < 0.03),  # 3% return rate
                "channel":       rng.choice(CHANNELS, p=[0.50, 0.30, 0.15, 0.05]),
            })

    return pd.DataFrame(records)


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def generate(
    start: date = date(2020, 1, 1),
    end:   date = date(2024, 12, 31),
    out_dir: str = "data/raw",
    n_customers: int = 50_000,
    n_products: int = 100,
) -> None:
    out_path = Path(out_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    logger.info("[generate] Building dimension data …")
    products  = build_products(n_products)
    customers = build_customers(n_customers)

    # Persist dim CSVs for reference
    products.to_csv(out_path / "dim_products.csv", index=False)
    customers.to_csv(out_path / "dim_customers.csv", index=False)
    logger.info(f"[generate] {len(products)} products, {len(customers)} customers saved.")

    current   = start
    buffer    = []
    current_month = (start.year, start.month)

    total_rows = 0
    while current <= end:
        month_key = (current.year, current.month)

        # Flush buffer when month changes
        if month_key != current_month and buffer:
            _flush(buffer, current_month, out_path)
            total_rows += len(buffer)
            buffer = []
            current_month = month_key

        daily_df = _daily_rows(current, products, customers)
        buffer.append(daily_df)
        current += timedelta(days=1)

    # Flush remainder
    if buffer:
        _flush(buffer, current_month, out_path)
        total_rows += sum(len(b) for b in buffer)

    logger.info(f"[generate] Done. Total rows generated: {total_rows:,}")


def _flush(frames: list[pd.DataFrame], month_key: tuple, out_path: Path) -> None:
    yr, mo = month_key
    fname  = out_path / f"sales_{yr}_{mo:02d}.csv"
    df     = pd.concat(frames, ignore_index=True)
    df.to_csv(fname, index=False)
    logger.info(f"[generate] Written {len(df):,} rows → {fname}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate synthetic sales data")
    parser.add_argument("--start",      default="2020-01-01")
    parser.add_argument("--end",        default="2024-12-31")
    parser.add_argument("--out-dir",    default="data/raw")
    parser.add_argument("--customers",  type=int, default=50_000)
    parser.add_argument("--products",   type=int, default=100)
    args = parser.parse_args()

    generate(
        start=date.fromisoformat(args.start),
        end=date.fromisoformat(args.end),
        out_dir=args.out_dir,
        n_customers=args.customers,
        n_products=args.products,
    )
