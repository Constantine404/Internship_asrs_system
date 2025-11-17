"""
Simple script to import basket data from an Excel or CSV file into the
`basket_data` table in the project's PostgreSQL database.

Features:
- Reads a data file (defaults to ./basket_data.csv)
- Normalizes column names (mapping 'code'->'basket_id' and 'shelf'->'shelf_id')
- Sorts by basket id ascending (numeric-aware if ids include digits)
- Inserts rows using ON CONFLICT to upsert shelf assignments

Usage examples:
  # from workspace root (PowerShell)
  # Note: Use the actual CSV filename you uploaded
  python -m asrs_lib.putdata2db --file ".\basket_data.xlsx - Sheet1.csv" --dry-run

  # Specify DB settings via .env or environment variables (DB_HOST, DB_PORT, DB_USER, DB_PASS, DB_NAME)

"""
from __future__ import annotations

import re
import argparse
from typing import Optional

import pandas as pd

# ensure .env is loaded so Pg picks up env vars
from . import config
from .db import Pg


def _numeric_key_for_basket(bid: Optional[str]):
    """Return a sort key for basket ids. If the id contains trailing digits (e.g. B00000001)
    the numeric portion is used so ordering is numeric; otherwise fall back to the raw string.
    """
    if bid is None:
        return (1, "")
    s = str(bid).strip()
    m = re.search(r"(\d+)$", s)
    if m:
        try:
            return (0, int(m.group(1)), s)
        except Exception:
            return (0, s)
    return (0, s)


def import_excel_to_db(file_path: str, dry_run: bool = False) -> dict:
    # ensure environment variables from .env are loaded
    config.load()

    # --- CHANGED: Handle .csv or .xlsx file ---
    if file_path.endswith(".csv"):
        print(f"Reading CSV: {file_path}")
        df = pd.read_csv(file_path, dtype=str)
    elif file_path.endswith(".xlsx"):
        print(f"Reading Excel: {file_path}")
        df = pd.read_excel(file_path, dtype=str, engine="openpyxl")
    else:
        raise ValueError("File must be .csv or .xlsx")
    # --- END CHANGE ---

    # normalize column names to simple lower-case names
    df.columns = [str(c).strip().lower() for c in df.columns]

    # --- NEW: Map CSV column names ('code', 'shelf') to expected DB names ('basket_id', 'shelf_id') ---
    rename_map = {
        "code": "basket_id",
        "shelf": "shelf_id"
    }
    df = df.rename(columns=rename_map)
    # --- END NEW ---

    if "basket_id" not in df.columns:
        # Updated error message to be more helpful
        raise RuntimeError("File must contain a 'basket_id' or 'code' column")

    # shelf_id may be missing or present
    shelf_col = "shelf_id" if "shelf_id" in df.columns else None

    # Keep only relevant columns
    if shelf_col:
        df = df[["basket_id", "shelf_id"]]
    else:
        df = df[["basket_id"]]
        df["shelf_id"] = None

    # Strip whitespace and normalize empty -> None
    df["basket_id"] = df["basket_id"].astype(str).str.strip()
    df["shelf_id"] = df["shelf_id"].where(df["shelf_id"].notna(), None)
    # Handle empty strings like "" for shelf_id and convert them to None
    df["shelf_id"] = df["shelf_id"].apply(lambda x: None if pd.isna(x) or str(x).strip() == "" else x)


    # Sort using numeric-aware key
    df["_sort_key"] = df["basket_id"].apply(_numeric_key_for_basket)
    df = df.sort_values(by="_sort_key")
    df = df.drop(columns=["_sort_key"])  # cleanup

    total = len(df)
    print(f"Rows to import: {total}")

    if dry_run:
        print("--- Dry Run Preview (first 20 rows) ---")
        print(df.head(20).to_string(index=False))
        print("-----------------------------------------")
        return {"inserted": 0, "updated": 0, "total": total, "status": "dry_run"}

    pg = Pg()
    inserted = 0
    updated = 0 # Note: script doesn't track updates, only upserts
    errors = 0
    
    upsert_sql = (
        "INSERT INTO basket_data (basket_id, shelf_id) VALUES (%s, %s) "
        "ON CONFLICT (basket_id) DO UPDATE SET shelf_id = EXCLUDED.shelf_id"
    )

    with pg.cursor() as cur:
        for _, row in df.iterrows():
            basket_id = row["basket_id"].strip() if row["basket_id"] else None
            shelf = row["shelf_id"]

            if basket_id is None or basket_id == "":
                print("[WARN] Skipping row with empty basket_id")
                errors += 1
                continue

            # try to coerce shelf to int if possible
            if shelf is not None and str(shelf).strip() != "":
                try:
                    # Convert via float first to handle values like "516.0"
                    shelf_val = int(float(str(shelf).strip()))
                except Exception:
                    # keep raw value and let DB raise if invalid (or print warning)
                    print(f"[WARN] Could not parse shelf '{shelf}' for basket '{basket_id}'. Setting to NULL.")
                    shelf_val = None # Safer to set to NULL if parsing fails
            else:
                shelf_val = None

            try:
                cur.execute(upsert_sql, (basket_id, shelf_val))
                inserted += 1 # This counts successful upserts (inserts or updates)
            except Exception as e:
                print(f"[ERROR] Failed to upsert {basket_id}: {e}")
                errors += 1

    print(f"Completed. rows processed: {total}, successful upserts: {inserted}, errors: {errors}")
    return {"inserted": inserted, "total": total, "errors": errors}


def main():
    parser = argparse.ArgumentParser(description="Import basket_data from Excel/CSV into DB")
    # --- CHANGED: Updated default file and help text ---
    parser.add_argument("--file", "-f", required=False, default="basket_data.csv", help="Path to Excel or CSV file")
    # --- END CHANGE ---
    parser.add_argument("--dry-run", action="store_true", help="Do not write to DB; show preview only")
    args = parser.parse_args()

    try:
        res = import_excel_to_db(args.file, dry_run=args.dry_run)
        print(f"Result: {res}")
    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    main()
# Run only file 
# python -m asrs_lib.putdata2db --file ".\basket_data.xlsx - Sheet1.csv" 