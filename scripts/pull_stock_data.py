import os
from datetime import date, timedelta

import pandas as pd
import yfinance as yf

RAW_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "raw")
os.makedirs(RAW_DIR, exist_ok=True)

SEMIS = ["NVDA", "AMD", "INTC", "QCOM", "AVGO", "TSM", "AMAT", "LRCX", "KLAC", "MU"]
BANKS = ["JPM", "BAC", "GS", "MS", "WFC", "C", "USB", "TFC", "PNC", "SCHW"]
ETFS = ["XLK", "XLF"]
VIX = ["^VIX"]

ALL_TICKERS = SEMIS + BANKS + ETFS + VIX

END_DATE = date.today()
START_DATE = END_DATE - timedelta(days=2 * 365)

OPTIONS_TICKERS = ["NVDA", "AMD", "JPM"]

def pull_prices():
    print(f"Pulling prices for {len(ALL_TICKERS)} tickers from {START_DATE} to {END_DATE}...")
    df = yf.download(ALL_TICKERS, start=str(START_DATE), end=str(END_DATE), group_by="ticker")

    for ticker in ALL_TICKERS:
        try:
            ticker_df = df[ticker].copy()
        except KeyError:
            print(f"  WARNING: {ticker} not found in download, trying individual download...")
            ticker_df = yf.download(ticker, start=str(START_DATE), end=str(END_DATE))

        ticker_df = ticker_df.dropna(how="all")
        if ticker_df.empty:
            print(f"  ERROR: No data for {ticker}")
            continue

        ticker_df = ticker_df.reset_index()
        ticker_df.columns = [c.lower().replace(" ", "_") for c in ticker_df.columns]

        safe_name = ticker.replace("^", "")
        out_path = os.path.join(RAW_DIR, f"{safe_name}_prices.parquet")
        ticker_df.to_parquet(out_path, index=False)
        print(f"  {ticker}: {len(ticker_df)} rows -> {out_path}")


def pull_options():
    today = date.today()
    print(f"\nPulling options chains for {OPTIONS_TICKERS} (trade_date={today})...")

    for ticker in OPTIONS_TICKERS:
        try:
            tk = yf.Ticker(ticker)
            expirations = tk.options
            if not expirations:
                print(f"  WARNING: No options expirations for {ticker}")
                continue

            all_chains = []
            for exp in expirations:
                chain = tk.option_chain(exp)
                for option_type, frame in [("call", chain.calls), ("put", chain.puts)]:
                    frame = frame.copy()
                    frame["option_type"] = option_type
                    frame["expiration_date"] = pd.to_datetime(exp).date()
                    frame["trade_date"] = today
                    frame["ticker"] = ticker
                    all_chains.append(frame)

            options_df = pd.concat(all_chains, ignore_index=True)
            options_df.columns = [c.lower().replace(" ", "_") for c in options_df.columns]

            out_path = os.path.join(RAW_DIR, f"{ticker}_options.parquet")
            options_df.to_parquet(out_path, index=False)
            print(f"  {ticker}: {len(options_df)} contracts across {len(expirations)} expirations -> {out_path}")

        except Exception as e:
            print(f"  ERROR pulling options for {ticker}: {e}")


def validate():
    print("\n--- Validation ---")
    errors = []

    expected_price_files = [t.replace("^", "") for t in ALL_TICKERS]
    for name in expected_price_files:
        path = os.path.join(RAW_DIR, f"{name}_prices.parquet")
        if not os.path.exists(path):
            errors.append(f"Missing price file: {name}")
            continue

        df = pd.read_parquet(path)
        date_col = [c for c in df.columns if "date" in c][0]
        dates = pd.to_datetime(df[date_col]).sort_values()
        gaps = dates.diff().dt.days
        max_gap = gaps.max()
        if max_gap > 7:  # 5 trading days --> 7 normal days
            errors.append(f"{name}: max gap of {max_gap} calendar days")
        print(f"  {name}: {len(df)} rows, max gap {max_gap} cal days")

    for ticker in OPTIONS_TICKERS:
        path = os.path.join(RAW_DIR, f"{ticker}_options.parquet")
        if not os.path.exists(path):
            errors.append(f"Missing options file: {ticker}")
            continue
        df = pd.read_parquet(path)
        if df.empty:
            errors.append(f"{ticker} options file is empty")
        print(f"  {ticker} options: {len(df)} contracts")

    if errors:
        print(f"\nFAILED with {len(errors)} error(s):")
        for e in errors:
            print(f"  - {e}")
    else:
        print("\nAll validations passed.")


if __name__ == "__main__":
    pull_prices()
    pull_options()
    validate()
