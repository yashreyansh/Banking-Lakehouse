from base  import CFG, FAKER, days_ago, now_utc, seed_all, wchoice, write, date_range_timestamps

import random, uuid, pandas as pd

from datetime import timedelta
from pathlib import Path


def generate(df_accounts, days, per_day: int | None=None):
    seed_all()
    
    active_accounts_records = df_accounts[df_accounts["status"]=="active"]
    active_accounts = active_accounts_records["account_id"].tolist()
    per_day = per_day or  CFG["volumes"]["transactions_per_day"]

    total = days*per_day
    print(f"\n[Transactions] Generating {total:,} transactions... ")
    timestamp = date_range_timestamps(days, per_day)
    row = []

    for ts in timestamp:
        channel = wchoice(CFG["channels"], CFG["channel_weights"])
        txn_type = wchoice(CFG["txn_types"],CFG["txn_type_weights"])

        # Amount: lognormal — most are small, tail is large
        import numpy as np
        amount = round(float(
            np.random.lognormal(mean=7.5, sigma=1.8)
        ), 2)
        amount = max(1.0, min(amount, 500_000.0))

        currency = wchoice(CFG["currencies"], CFG["currency_weights"])

        status = wchoice(["approved","declined","pending"],[0.85,0.08,0.07])

        row.append({
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
        })
    df = pd.DataFrame(row)
    df["timestamp"]= pd.to_datetime(df["timestamp"])
    df["_ingest_time"]= pd.to_datetime(df["_ingest_time"])

    return df


def run_transactions(
    df_accounts: pd.DataFrame | None=None,
    fmt: str | None=None)-> pd.DataFrame:    # json for event hub replay

    path = Path(__file__).parent.parent / "output" / "accounts.parquet"
    if not path.exists():
        print(f"[Transactions] Accounts do not exists, generating accounts...\n")
        # geenrate account
        from accounts import run_accounts
        run_accounts()
        df_accounts = pd.read_parquet(path)
    else:df_accounts = pd.read_parquet(path)   

    transaction_per_day = CFG["volumes"]["transactions_per_day"]


    df_transactions = generate(df_accounts, days=1, per_day=transaction_per_day)
    print(f"[Transactions] Transactions are generated..")
    write(df_transactions, "transactions", fmt=fmt)
    return df_transactions


if __name__ =="__main__":

    run_transactions(fmt="json1")