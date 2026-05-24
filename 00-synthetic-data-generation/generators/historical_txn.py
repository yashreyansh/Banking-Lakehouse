"""
synthetic_data/generators/historical_txns.py
─────────────────────────────────────────────
Generates the `historical_transactions` ONE-TIME migration source.

Schema is identical to transactions.py plus:
  legacy_txn_id     str    original ID in legacy system (TXN-YYYYMMDD-{seq})
  source_system     str    LEGACY_CORE_V1 | LEGACY_CORE_V2
  data_quality_flag str    nullable — NULL_AMOUNT | UNKNOWN_CHANNEL |
                                      DUPLICATE_SUSPECTED | MISSING_ACCOUNT
 
Usage:
  from generators.historical_txns import run
  df = run(df_accounts, days=365, per_day=8_000)
  # default: 365 days × 8,000/day = 2.92M rows
  # or standalone

transactions Schema
"transaction_id" : str(uuid.uuid4()),
"account_id":random.choice(active_accounts),
"timestamp":ts,
"amount":amount,
"currency":currency,
"status":status,
"txn_type":txn_type,
"channel":channel,
"_ingest_time":ts + timedelta(milliseconds=random.randint(50, 800)),
"_source": "event_hub",

"""
from base import (
    CFG, FAKER, date_range_timestamps,
    now_utc, seed_all, wchoice, write,days_ago
)
from pathlib import Path
import pandas as pd
import random, uuid
import numpy as np
from datetime import timedelta
import random


def _legacy_txn_id(ts: "pd.Timestamp", seq: int) -> str:
    return f"TXN-{ts.strftime('%Y%m%d')}-{seq:08d}"

def generate(df_accounts:pd.DataFrame | None=None,
             days: int | None=None,
             per_day: int =5000  )-> pd.DataFrame:
    seed_all()
    print(f"Generating historical txns.")

    row= []
    days = days or CFG["historical_days"]
    total_txn = days * per_day
    print(f"\n[Historical_txn] Generation {total_txn:,} historical transactions.. {days} Days * {per_day} Per_day\n")

    account_ids = df_accounts["account_id"].to_list()
    timestamp_list = date_range_timestamps(days, per_day)



    for seq, ts in enumerate(timestamp_list):
        
        account_id = random.choice(account_ids)
        amount = round(float(np.random.lognormal(mean=7.5, sigma=1.8)), 2)
        amount = max(1.0, min(amount, 500_000.0))
        currency = wchoice(CFG["currencies"], CFG["currency_weights"])

        # reusing status logic from transactions.py
        status = wchoice(["approved","declined","pending"],[0.85,0.08,0.07])
        channel = wchoice(CFG["channels"], CFG["channel_weights"])
        txn_type = wchoice(CFG["txn_types"],CFG["txn_type_weights"])


        # mark the historical transaction past dated as per the current day
        D = random.randint(CFG["historical_day_start"] , CFG["historical_day_start"])
        transaction_ts = days_ago(D).replace(hour=0, minute=0, second=0, microsecond=0)

        row.append({
            "transaction_id" : str(uuid.uuid4()),
            "account_id": account_id,
            "timestamp": ts,
            "amount":amount,
            "currency": wchoice([currency,"XXX","YYY","---"], [0.98, 0.005,0.005,0.01]),  # legacy bad code/currency
            "status":status,
            "txn_type":txn_type,
            "channel":channel,
            "city": FAKER.city(),
            "_ingest_time":ts + timedelta(milliseconds=random.randint(50, 800)),
            "_source": wchoice(["legacy_V1","legacy_V2"], [0.75,0.25]),
            "legacy_txn_id": _legacy_txn_id(ts, seq),
            "source_system": "historical_migration"
        })
    
    df = pd.DataFrame(row)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["_ingest_time"] = pd.to_datetime(df["_ingest_time"])
    return df


def run_historical_txn(df_accounts: pd.DataFrame, days= CFG["historical_days"],per_day=CFG["historical_per_day_txn"] ) -> pd.DataFrame:
    df = generate(df_accounts,days,per_day )
    write(df, "historical_txn")
    return df


if __name__ =="__main__":
    # check if accounts exists
    account_path = Path(__file__).parent.parent / CFG["output"]["dir"] / "accounts.parquet"
    if not account_path.exists():
        print(f"[Historial_TXN] Accounts do not exists... Generating accounts first...")
        from accounts import run_accounts
        df_accounts = run_accounts()
    else:df_accounts=pd.read_parquet(account_path)
    print("[Historical_txn] Running historical txns...")
    run_historical_txn(df_accounts,days=30, per_day=5000)