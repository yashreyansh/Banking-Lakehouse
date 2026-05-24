

import pandas as pd
from sqlalchemy import create_engine
import urllib, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
from generators.base import CFG
from sqlalchemy import text
from dotenv import load_dotenv
import os

load_dotenv()   # reads .env file

SERVER   = os.getenv("DB_SERVER")
DATABASE = os.getenv("DB_NAME")
USERNAME = os.getenv("DB_USER")
PASSWORD = os.getenv("DB_PASS")

params = urllib.parse.quote_plus(
    f"DRIVER={{ODBC Driver 18 for SQL Server}};"
    f"SERVER={SERVER};"
    f"DATABASE={DATABASE};"
    f"UID={USERNAME};"
    f"PWD={PASSWORD};"
    f"Encrypt=yes;TrustServerCertificate=no;"
)
engine = create_engine(
    f"mssql+pyodbc:///?odbc_connect={params}",
    fast_executemany=True   # much faster bulk inserts
)

# DATE-only columns per table — these must be cast to Python date, not datetime
DATE_COLUMNS = {
    "user_profiles":           ["date_of_birth", "customer_since"],
    "accounts":                ["opened_on", "closed_on"],
    "historical_transactions": [],
}

def clean_df(df: pd.DataFrame, date_cols: list) -> pd.DataFrame:
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            # Strip timezone
            try:
                df[col] = df[col].dt.tz_convert(None)
            except TypeError:
                df[col] = df[col].dt.tz_localize(None)

            # Cast DATE-only columns to Python date object
            if col in date_cols:
                df[col] = df[col].dt.date

    return df

# ── Load in order (FK: users → accounts → transactions) ───
base_path = Path(__file__).parent.parent / "output"
user_profile_path = base_path /"user_profiles.parquet" 
acccounts_path = base_path / "accounts.parquet"
historical_txns_path = base_path / "historical_txn.parquet"

files = [
    (user_profile_path,  "user_profiles",           "BANKING"),
    (acccounts_path,     "accounts",                 "BANKING"),
    (historical_txns_path, "historical_transactions", "BANKING"),  # ← was "historical_txn"
]

for path, table, schema in files:
        # Truncate existing data before re-loading
    with engine.connect() as conn:
        conn.execute(text(f"TRUNCATE TABLE {schema}.{table}"))
        conn.commit()
    
    print(f"\nLoading {path} → {schema}.{table} ...")
    df        = pd.read_parquet(path)
    date_cols = DATE_COLUMNS.get(table, [])
    df        = clean_df(df, date_cols)
    print(f"  Rows: {len(df):,}")
    print(df.dtypes)          # ← shows column types BEFORE insert
    print(df.head(1))         # ← shows first row values

    try:
        df.to_sql(
            name=table,
            schema=schema,
            con=engine,
            if_exists="append",
            index=False,
            chunksize=500,
            method=None,       # ← remove "multi" — conflicts with fast_executemany
        )
        print(f"  ✅ Done → {schema}.{table}")
    except Exception as e:
       
        print(f"\n❌ FULL ERROR:\n{e}")        # ← print e directly, not e.__cause__
        import traceback
        traceback.print_exc()                  # ← prints the complete stack with root cause
        break