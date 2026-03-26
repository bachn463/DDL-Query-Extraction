import os
from datetime import date

import duckdb
import pandas as pd
import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog

PROJECT_ROOT = os.path.join(os.path.dirname(__file__), "..")
RAW_DIR = os.path.join(PROJECT_ROOT, "data", "raw")
WAREHOUSE_DIR = os.path.join(PROJECT_ROOT, "warehouse")
CATALOG_DB = os.path.join(WAREHOUSE_DIR, "catalog.db")

catalog = SqlCatalog(
    "local",
    **{
        "uri": f"sqlite:///{CATALOG_DB}",
        "warehouse": f"file://{os.path.abspath(WAREHOUSE_DIR)}",
    },
)

SEMIS = ["NVDA", "AMD", "INTC", "QCOM", "AVGO", "TSM", "AMAT", "LRCX", "KLAC", "MU"]
BANKS = ["JPM", "BAC", "GS", "MS", "WFC", "C", "USB", "TFC", "PNC", "SCHW"]
ETFS = ["XLK", "XLF"]

SYMBOL_REF = {
    # Semiconductors / xlk
    "NVDA": ("NVIDIA Corp", "XLK", "MEGA", True),
    "AMD": ("Advanced Micro Devices", "XLK", "LARGE", True),
    "INTC": ("Intel Corp", "XLK", "LARGE", True),
    "QCOM": ("Qualcomm Inc", "XLK", "LARGE", True),
    "AVGO": ("Broadcom Inc", "XLK", "MEGA", True),
    "TSM": ("Taiwan Semiconductor", "XLK", "MEGA", False),
    "AMAT": ("Applied Materials", "XLK", "LARGE", True),
    "LRCX": ("Lam Research", "XLK", "LARGE", True),
    "KLAC": ("KLA Corp", "XLK", "LARGE", True),
    "MU": ("Micron Technology", "XLK", "LARGE", True),
    # Banks / xlf
    "JPM": ("JPMorgan Chase", "XLF", "MEGA", True),
    "BAC": ("Bank of America", "XLF", "LARGE", True),
    "GS": ("Goldman Sachs", "XLF", "LARGE", True),
    "MS": ("Morgan Stanley", "XLF", "LARGE", True),
    "WFC": ("Wells Fargo", "XLF", "LARGE", True),
    "C": ("Citigroup Inc", "XLF", "LARGE", True),
    "USB": ("US Bancorp", "XLF", "LARGE", True),
    "TFC": ("Truist Financial", "XLF", "LARGE", True),
    "PNC": ("PNC Financial", "XLF", "LARGE", True),
    "SCHW": ("Charles Schwab", "XLF", "LARGE", True),
}


def load_symbol_ref():
    """Load symbol reference data."""
    print("Loading symbol_ref...")
    rows = []
    for ticker, (name, etf, cap, sp500) in SYMBOL_REF.items():
        rows.append({
            "ticker": ticker,
            "company_name": name,
            "sector_etf": etf,
            "market_cap_tier": cap,
            "sp500_flag": sp500,
        })
    df = pd.DataFrame(rows)
    table = catalog.load_table("market.symbol_ref")
    table.append(pa.Table.from_pandas(df, schema=table.schema().as_arrow()))
    print(f"  {len(df)} rows loaded")


def load_daily_prices():
    """Load all 20 stock tickers into daily_prices."""
    print("Loading daily_prices...")
    all_tickers = SEMIS + BANKS
    frames = []
    for ticker in all_tickers:
        path = os.path.join(RAW_DIR, f"{ticker}_prices.parquet")
        df = pd.read_parquet(path)
        df["ticker"] = ticker
        df["trade_date"] = pd.to_datetime(df["date"]).dt.date
        df["adj_close"] = df["close"] # Use close as adj_close since yfinance adj_close is often null
        df = df[["ticker", "trade_date", "open", "high", "low", "close", "adj_close", "volume"]]
        frames.append(df)

    combined = pd.concat(frames, ignore_index=True)
    table = catalog.load_table("market.daily_prices")
    table.append(pa.Table.from_pandas(combined, schema=table.schema().as_arrow()))
    print(f"  {len(combined)} rows loaded ({len(all_tickers)} tickers)")


def load_sector_etfs():
    """Load XLK and XLF ETF prices into sector_etfs."""
    print("Loading sector_etfs...")
    frames = []
    for etf in ETFS:
        path = os.path.join(RAW_DIR, f"{etf}_prices.parquet")
        df = pd.read_parquet(path)
        df["etf_ticker"] = etf
        df["trade_date"] = pd.to_datetime(df["date"]).dt.date
        df["adj_close"] = df["close"]
        df = df[["etf_ticker", "trade_date", "open", "high", "low", "close", "adj_close", "volume"]]
        frames.append(df)

    combined = pd.concat(frames, ignore_index=True)
    table = catalog.load_table("market.sector_etfs")
    table.append(pa.Table.from_pandas(combined, schema=table.schema().as_arrow()))
    print(f"  {len(combined)} rows loaded ({len(ETFS)} ETFs)")


def load_options_chain():
    """Load NVDA, AMD, JPM options into options_chain."""
    print("Loading options_chain...")
    options_tickers = ["NVDA", "AMD", "JPM"]
    frames = []
    for ticker in options_tickers:
        path = os.path.join(RAW_DIR, f"{ticker}_options.parquet")
        df = pd.read_parquet(path)

        mapped = pd.DataFrame({
            "contract_id": df["contractsymbol"],
            "ticker": df["ticker"],
            "trade_date": pd.to_datetime(df["trade_date"]).dt.date,
            "expiration_date": pd.to_datetime(df["expiration_date"]).dt.date,
            "strike_price": df["strike"],
            "option_type": df["option_type"],
            "delta": None,  # not available from yfinance
            "iv": df["impliedvolatility"],
            "pop": None,  # not available from yfinance
            "dte": (pd.to_datetime(df["expiration_date"]) - pd.to_datetime(df["trade_date"])).dt.days,
        })
        mapped["delta"] = mapped["delta"].astype("Float64")
        mapped["pop"] = mapped["pop"].astype("Float64")
        frames.append(mapped)

    combined = pd.concat(frames, ignore_index=True)
    table = catalog.load_table("options.options_chain")
    table.append(pa.Table.from_pandas(combined, schema=table.schema().as_arrow()))
    print(f"  {len(combined)} contracts loaded ({len(options_tickers)} tickers)")


def load_macro_indicators():
    """Load FRED data + VIX into macro_indicators."""
    print("Loading macro_indicators...")
    frames = []

    for series_id in ["FEDFUNDS", "GDP"]:
        path = os.path.join(RAW_DIR, f"{series_id}.parquet")
        df = pd.read_parquet(path)

        # For monthly/quarterly data: announcement_date = date, period_end_date = previous period
        dates = pd.to_datetime(df["date"])
        values = df["value"].tolist()
        prior_values = [None] + values[:-1]

        mapped = pd.DataFrame({
            "indicator_code": df["indicator_code"],
            "announcement_date": dates.dt.date,
            "period_end_date": (dates - pd.DateOffset(months=1)).dt.date if series_id == "FEDFUNDS"
                               else (dates - pd.DateOffset(months=3)).dt.date,
            "value": values,
            "prior_value": prior_values,
        })
        frames.append(mapped)

    # VIX from yfinance (daily closing values)
    vix_path = os.path.join(RAW_DIR, "VIX_prices.parquet")
    vix_df = pd.read_parquet(vix_path)
    vix_values = vix_df["close"].tolist()
    vix_prior = [None] + vix_values[:-1]

    vix_mapped = pd.DataFrame({
        "indicator_code": "VIXCLS",
        "announcement_date": pd.to_datetime(vix_df["date"]).dt.date,
        "period_end_date": pd.to_datetime(vix_df["date"]).dt.date,  # same day for daily
        "value": vix_values,
        "prior_value": vix_prior,
    })
    frames.append(vix_mapped)

    combined = pd.concat(frames, ignore_index=True)
    table = catalog.load_table("macro.macro_indicators")
    table.append(pa.Table.from_pandas(combined, schema=table.schema().as_arrow()))
    print(f"  {len(combined)} rows loaded (FEDFUNDS, GDP, VIXCLS)")


def validate():
    """Validate loaded data via DuckDB iceberg_scan."""
    print("\n--- Validation via DuckDB iceberg_scan ---")
    errors = []
    con = duckdb.connect()
    con.execute("INSTALL iceberg; LOAD iceberg;")
    con.execute("SET unsafe_enable_version_guessing = true;")

    def scan(table_name):
        namespace, tbl = table_name.split(".")
        table_dir = os.path.abspath(os.path.join(WAREHOUSE_DIR, namespace, tbl))
        return f"iceberg_scan('{table_dir}', allow_moved_paths = true)"

    # Check 20 distinct tickers in daily_prices
    result = con.execute(f"SELECT COUNT(DISTINCT ticker) as cnt FROM {scan('market.daily_prices')}").fetchone()
    ticker_count = result[0]
    print(f"  Distinct tickers in daily_prices: {ticker_count}")
    if ticker_count != 20:
        errors.append(f"Expected 20 tickers, got {ticker_count}")

    # Check 20 rows in symbol_ref
    result = con.execute(f"SELECT COUNT(*) as cnt FROM {scan('market.symbol_ref')}").fetchone()
    ref_count = result[0]
    print(f"  Rows in symbol_ref: {ref_count}")
    if ref_count != 20:
        errors.append(f"Expected 20 symbol_ref rows, got {ref_count}")

    # Check Zero nulls on ticker and adj_close
    result = con.execute(f"""
        SELECT COUNT(*) FROM {scan('market.daily_prices')}
        WHERE ticker IS NULL OR adj_close IS NULL
    """).fetchone()
    null_count = result[0]
    print(f"  Null ticker/adj_close in daily_prices: {null_count}")
    if null_count > 0:
        errors.append(f"Found {null_count} nulls in ticker/adj_close")

    # Check Both XLK and XLF in sector_etfs
    result = con.execute(f"""
        SELECT DISTINCT etf_ticker FROM {scan('market.sector_etfs')} ORDER BY etf_ticker
    """).fetchall()
    etfs = [r[0] for r in result]
    print(f"  ETFs in sector_etfs: {etfs}")
    if set(etfs) != {"XLK", "XLF"}:
        errors.append(f"Expected XLK and XLF, got {etfs}")

    # Check if Options loaded
    result = con.execute(f"SELECT COUNT(*) FROM {scan('options.options_chain')}").fetchone()
    print(f"  Options contracts: {result[0]}")

    # Check if Macro indicators loaded
    result = con.execute(f"""
        SELECT indicator_code, COUNT(*) as cnt
        FROM {scan('macro.macro_indicators')}
        GROUP BY indicator_code ORDER BY indicator_code
    """).fetchall()
    for code, cnt in result:
        print(f"  Macro {code}: {cnt} rows")

    con.close()

    if errors:
        print(f"\nFAILED with {len(errors)} error(s):")
        for e in errors:
            print(f"  - {e}")
    else:
        print("\nAll validations passed.")


if __name__ == "__main__":
    load_symbol_ref()
    load_daily_prices()
    load_sector_etfs()
    load_options_chain()
    load_macro_indicators()
    validate()
