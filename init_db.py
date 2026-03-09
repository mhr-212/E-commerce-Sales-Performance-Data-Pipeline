"""
init_db.py — Bootstrap the data warehouse schema from Python.
Useful for development when Docker is not available.

Usage:
    DATABASE_URL=postgresql://pipeline:pipeline@localhost/ecommerce_dw python init_db.py
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

from sqlalchemy import create_engine, text

SQL_DIR = Path(__file__).parent / "sql"


def run_sql_file(engine, fpath: Path) -> None:
    print(f"  → Running {fpath.name} …", end=" ")
    sql = fpath.read_text(encoding="utf-8")
    with engine.begin() as conn:
        conn.execute(text(sql))
    print("✅")


def main() -> None:
    url = os.environ.get(
        "DATABASE_URL",
        "postgresql://pipeline:pipeline@localhost:5432/ecommerce_dw",
    )
    print(f"Connecting to: {url}")
    engine = create_engine(url, echo=False)

    for fname in ["01_create_schema.sql", "02_seed_dim_date.sql"]:
        run_sql_file(engine, SQL_DIR / fname)

    print("\n✅ Database initialised successfully.")
    engine.dispose()


if __name__ == "__main__":
    main()
