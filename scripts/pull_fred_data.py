import os
import ssl
import certifi
from datetime import date, timedelta

import pandas as pd
from fredapi import Fred
from dotenv import load_dotenv

# Fix SSL certs for macOS Python
os.environ["SSL_CERT_FILE"] = certifi.where()
ssl._create_default_https_context = ssl.create_default_context
ssl._create_default_https_context = lambda: ssl.create_default_context(cafile=certifi.where())

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

RAW_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "raw")
os.makedirs(RAW_DIR, exist_ok=True)

FRED_API_KEY = os.environ["FRED_API_KEY"]
fred = Fred(api_key=FRED_API_KEY)

END_DATE = date.today()
START_DATE = END_DATE - timedelta(days=2 * 365)

SERIES = {
    "FEDFUNDS": "Federal Funds Effective Rate",
    "GDP": "Gross Domestic Product",
}


def pull_fred():
    print(f"Pulling FRED data from {START_DATE} to {END_DATE}...")

    for series_id, description in SERIES.items():
        print(f"  Pulling {series_id} ({description})...")
        data = fred.get_series(series_id, observation_start=str(START_DATE), observation_end=str(END_DATE))

        df = data.reset_index()
        df.columns = ["date", "value"]
        df["indicator_code"] = series_id
        df = df.dropna(subset=["value"])

        out_path = os.path.join(RAW_DIR, f"{series_id}.parquet")
        df.to_parquet(out_path, index=False)
        print(f"    {len(df)} observations -> {out_path}")


def validate():
    print("\n--- Validation ---")
    errors = []

    for series_id in SERIES:
        path = os.path.join(RAW_DIR, f"{series_id}.parquet")
        if not os.path.exists(path):
            errors.append(f"Missing file: {series_id}")
            continue

        df = pd.read_parquet(path)
        nulls = df["value"].isna().sum()
        if nulls > 0:
            errors.append(f"{series_id}: {nulls} null values")

        min_date = pd.to_datetime(df["date"]).min().date()
        max_date = pd.to_datetime(df["date"]).max().date()
        print(f"  {series_id}: {len(df)} rows, range {min_date} to {max_date}")

        # Check coverage overlaps with price data window
        if min_date > START_DATE + timedelta(days=90):
            errors.append(f"{series_id}: starts too late ({min_date}), expected near {START_DATE}")

    if errors:
        print(f"\nFAILED with {len(errors)} error(s):")
        for e in errors:
            print(f"  - {e}")
    else:
        print("\nAll validations passed.")


if __name__ == "__main__":
    pull_fred()
    validate()
