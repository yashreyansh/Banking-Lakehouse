# base file for all generators

from pathlib import Path
import yaml
from faker import Faker
from datetime import datetime, timedelta, timezone
import random
import pandas as pd
import numpy as np

# ---- Load config-----
_CONFIG_PATH = Path(__file__).parent.parent / "config.yml"

def load_config() -> dict:
    with open(_CONFIG_PATH) as f:
        return yaml.safe_load(f)

CFG = load_config()


#------------------- 

def make_faker(seed: int | None= None) -> Faker:
    locale = CFG.get("locale","es_MX")

    fk = Faker(locale)
    Faker.seed(seed if seed is not None else CFG["seed"])

    return fk

FAKER = make_faker()

# ── Weighted random choice ───────────────────────────────────────────────────
def wchoice(options: list, weights: list) -> any:
    return random.choices(options, weights=weights, k=1)[0]

#------ Timestamp Helper

def now_utc() -> datetime:
    return datetime.now(timezone.utc)

def random_ts(start: datetime, end:datetime) -> datetime:
    delta = end-start
    return start + timedelta(seconds=random.random() * delta.total_seconds())

def days_ago(n: int) -> datetime:
    return now_utc() - timedelta(days=n)

def date_range_timestamps(days: int, per_day: int) -> list[datetime]:
    """Generate `per_day` random timestamps for each of the last `days` days."""
    ts_list = []
    for d in range(days, 0, -1):
        day_start = days_ago(d).replace(hour=0, minute=0, second=0, microsecond=0)
        day_end   = day_start + timedelta(days=1)
        ts_list.extend(
            random_ts(day_start, day_end) for _ in range(per_day)
        )
    return ts_list


#----Writers----------------------------------
def write(df: pd.DataFrame, name: str, fmt: str | None = None)-> Path:
    fmt = fmt or CFG["output"]["format"]
    out_dir = Path(__file__).parent.parent /CFG["output"]["dir"]
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{name}.{fmt}"

    if fmt =="parquet":
        df.to_parquet(path, index=False,engine="pyarrow")
    elif fmt =="json1":
        df.to_json(path, orient="records", lines=True, date_format="iso")
    elif fmt =="csv":
        df.to_csv(path, index=False)
    else:
        raise ValueError(f"Unsupported format: {fmt}")
    
    size_mb = path.stat().st_size / 1_078_576
    print(f" Wrote {len(df):>8,} rows -> {path.name} ({size_mb:.2f} MB)")
    return path

# ── Seed everything ──────────────────────────────────────────────────────────
def seed_all(seed: int | None = None) -> None:
    s = seed if seed is not None else CFG["seed"]
    random.seed(s)
    np.random.seed(s)
    Faker.seed(s)