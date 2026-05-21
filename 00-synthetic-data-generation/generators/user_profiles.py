"""
SCHEMA
user_id     str  USR-{8-digit}
full_name   str masked in silver
email       str sha256 in silver
phone       str masked in silver
kyc_tier    int 1|2|3
risk_score  float 0.0–1.0
nationality str
date_of_birth date
created_at  timestamp
updated_at   timestamp
"""

from __future__ import annotations

import random
import uuid
from datetime import timedelta
 
import pandas as pd
 
from base import CFG, FAKER, days_ago, now_utc, seed_all, wchoice, write


MX_STATES = [
    "Aguascalientes","Baja California","Baja California Sur","Campeche",
    "Chiapas","Chihuahua","Ciudad de México","Coahuila","Colima","Durango",
    "Guanajuato","Guerrero","Hidalgo","Jalisco","Estado de México",
    "Michoacán","Morelos","Nayarit","Nuevo León","Oaxaca","Puebla",
    "Querétaro","Quintana Roo","San Luis Potosí","Sinaloa","Sonora",
    "Tabasco","Tamaulipas","Tlaxcala","Veracruz","Yucatán","Zacatecas",
]
NATIONALITIES = ["MX"] * 80 + ["US"] * 8 + ["CA"] * 3 + ["DE"] * 2 +  ["ES"] * 2 + ["BR"] * 2 + ["CO"] * 1 + ["AR"] * 1 + ["FR"] * 1
 
def generate(n: int | None= None)-> pd.DataFrame:
    seed_all()

    n = CFG["volumes"]["users"]
    print(f"\n[user_profiles] Generating {n:,} users ...")
    rows = []

    for i in range(n):
        user_id = f"USR-{i+1:08d}"
        now = now_utc()
        
        dob_days  = random.randint(18*365, 80*365)  # min 18 years max 80
        dob = (now - timedelta(days=dob_days)).date()

        since_days = random.randint(30, 3650)
        since = (now- timedelta(days=since_days)).date()

        updated = now - timedelta(days=random.randint(0,since_days))

        rows.append({
            "user_id": user_id,
            "full_name": FAKER.name(),
            "email":          FAKER.email(),
            "phone":          FAKER.numerify("+52 1 ### ### ####"),
            "date_of_birth":  dob,
            "nationality":    random.choice(NATIONALITIES),
            "gender":         random.choices(["M", "F", "NB"], weights=[48, 48, 4])[0],
            "address":        FAKER.address().replace("\n", ", "),
            "city":           FAKER.city(),
            "state":          random.choice(MX_STATES),
            "is_pep":         random.random() < 0.01,
            "is_sanctioned":  random.random() < 0.0005,
            "account_count":  random.choices([1, 2], weights=[70, 30])[0],
            "customer_since": since,
            "created_at":     now - timedelta(days=since_days),
            "updated_at":     updated,
            "status":         wchoice(
                                  ["active", "inactive", "blocked"],
                                  [0.88, 0.09, 0.03]
                              )
        })
        
    df = pd.DataFrame(rows)
    df["date_of_birth"]  = pd.to_datetime(df["date_of_birth"])
    df["customer_since"] = pd.to_datetime(df["customer_since"])
    df["created_at"]     = pd.to_datetime(df["created_at"])
    df["updated_at"]     = pd.to_datetime(df["updated_at"])

    return df
    


def run() -> pd.DataFrame:
    df = generate()
    write(df, "user_profiles")
    print("User profiles written...")
    return df


if __name__ == "__main__":
    run()