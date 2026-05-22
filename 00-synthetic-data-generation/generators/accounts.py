import pandas as pd
import random
from base import CFG, FAKER, days_ago, now_utc, seed_all, wchoice, write
from datetime import timedelta


def generate(df_users):
    if df_users is None:
        p = Path(__file__).parent.parent / CFG["output"]["dir"] / "user_profiles.parquet"
        if not p.exists():
            raise FileNotFoundError(
                "user_profiles.parquet not found. Run user profile generator first....."
            )
        df_users = pd.read_parquet(p)
    
    total = CFG["volumes"]["accounts"]
    print(f"\n [accounts] Generating - {total:,} accounts for {len(df_users):,} users ...")

    row = []
    acc_counter = 1

    for _ , user in df_users.iterrows():
        n_accounts = int(user["account_count"])   # number of account for that user 
        user_since = pd.to_datetime(user["customer_since"]).tz_localize('UTC')
        now = now_utc()

        #distribute account type
        types_pool = ["checking"]   # default one account
        
        # no loop required as we set user to have either 1 or 2 accounts
        if n_accounts>1:
            extra = wchoice(
                ["savings","credit"],
                weights=[0.65,0.35]
            )
            types_pool.append(extra)   
        
        for acc_type in types_pool:
            acc_id = f"ACC-{acc_counter:010d}"   # ACC-0000000001
            acc_counter +=1

            days_open = max(1, (now-user_since).days )
            opened_days_ago = random.randint(0,days_open)
            opened_on = now - timedelta(days =opened_days_ago )

            currency = wchoice(
                CFG["currencies"], CFG["currency_weights"]
            )

            status = wchoice(
                ["active","closed","frozen"], [0.88,0.05,0.07]
            )
            closed_on = None
            if status=="closed":
                closed_days_ago = random.randint(0,opened_days_ago)
                closed_on = now - timedelta(days=closed_days_ago)
                
            row.append({
                "account_id" : acc_id,
                "opened_on"  :  opened_on,
                "closed_on": closed_on.date() if closed_on else None,
                "user_id" : user["user_id"],
                #"balance": balance ,
                "status" : status,
                "currency": currency

            })
    
    df = pd.DataFrame(row)
    df["opened_on"] = pd.to_datetime(df["opened_on"])

    return df

def run(df_users :pd.DataFrame | None=None) -> pd.DataFrame:
    
    df_accounts = generate(df_users)
    write(df_accounts,'accounts')
    return df_accounts



if __name__ == "__main__":

    from pathlib import Path
    users_path = Path(__file__).parent.parent / CFG["output"]["dir"] / "user_profiles.parquet"

    if not users_path.exists():
        print(f"[accounts] user_profiles.parquet not found - running user_profiles generator first ...")

        from user_profiles import run as run_users
        df_users = run_users()   # returns the user_dataframe
    else:
        df_users = pd.read_parquet(users_path)
    
    run(df_users=df_users)
    