import json
import os

import duckdb
import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog

PROJECT_ROOT = os.path.join(os.path.dirname(__file__), "..")
WAREHOUSE_DIR = os.path.join(PROJECT_ROOT, "warehouse")
CATALOG_DB = os.path.join(WAREHOUSE_DIR, "catalog.db")
RUN2_PATH = os.path.join(PROJECT_ROOT, "data", "joins", "run2_combined.json")

catalog = SqlCatalog(
    "local",
    **{
        "uri": f"sqlite:///{CATALOG_DB}",
        "warehouse": f"file://{os.path.abspath(WAREHOUSE_DIR)}",
    },
)


def scan(table_name):
    namespace, tbl = table_name.split(".")
    table_dir = os.path.abspath(os.path.join(WAREHOUSE_DIR, namespace, tbl))
    return f"iceberg_scan('{table_dir}', allow_moved_paths = true)"


def load_join_relationships():
    """Load Run 2 JSON into governance.join_relationships."""
    print("Loading Run 2 results into governance.join_relationships...")

    with open(RUN2_PATH) as f:
        run2 = json.load(f)

    rows = []
    for r in run2:
        rows.append({
            "table_a": r["table_a"],
            "col_a": r["col_a"],
            "table_b": r["table_b"],
            "col_b": r["col_b"],
            "confidence": r.get("confidence"),
            "frequency": r.get("frequency", 0),
            "reasoning": r.get("reasoning"),
            "warning": r.get("warning"),
        })

    import pandas as pd
    df = pd.DataFrame(rows)
    table = catalog.load_table("governance.join_relationships")
    table.append(pa.Table.from_pandas(df, schema=table.schema().as_arrow()))
    print(f"  {len(df)} rows loaded")
    return len(run2)


def run_metrics(expected_rows):
    """Run the 5 success metric queries."""
    print("\n--- Final Validation Metrics ---")
    con = duckdb.connect()
    con.execute("INSTALL iceberg; LOAD iceberg;")
    con.execute("SET unsafe_enable_version_guessing = true;")

    results = {}

    # Metric 1: Zero nulls
    print("\nMetric 1: Zero nulls on ticker/adj_close in daily_prices")
    r = con.execute(f"""
        SELECT COUNT(*) FROM {scan('market.daily_prices')}
        WHERE ticker IS NULL OR adj_close IS NULL
    """).fetchone()[0]
    passed = r == 0
    results["metric_1_zero_nulls"] = passed
    print(f"  Null count: {r} -> {'PASS' if passed else 'FAIL'}")

    # Metric 2: 20 tickers joined
    print("\nMetric 2: 20 tickers in daily_prices joined to symbol_ref")
    r = con.execute(f"""
        SELECT COUNT(DISTINCT d.ticker)
        FROM {scan('market.daily_prices')} d
        JOIN {scan('market.symbol_ref')} s ON d.ticker = s.ticker
    """).fetchone()[0]
    passed = r == 20
    results["metric_2_ticker_count"] = passed
    print(f"  Distinct tickers: {r} -> {'PASS' if passed else 'FAIL'}")

    # Metric 3: High confidence relationships from query history
    print("\nMetric 3: HIGH confidence relationships with frequency > 0")
    r = con.execute(f"""
        SELECT COUNT(*) FROM {scan('governance.join_relationships')}
        WHERE frequency > 0 AND confidence = 'HIGH'
    """).fetchone()[0]
    passed = r >= 2
    results["metric_3_high_conf"] = passed
    print(f"  Count: {r} (target >= 2) -> {'PASS' if passed else 'FAIL'}")

    # Metric 4: Warnings flagged
    print("\nMetric 4: Warnings present")
    rows = con.execute(f"""
        SELECT table_a, col_a, table_b, col_b, warning
        FROM {scan('governance.join_relationships')}
        WHERE warning IS NOT NULL
    """).fetchall()
    passed = len(rows) >= 1
    results["metric_4_warnings"] = passed
    print(f"  Warning rows: {len(rows)} (target >= 1) -> {'PASS' if passed else 'FAIL'}")
    for row in rows:
        print(f"    {row[0]}.{row[1]} -> {row[2]}.{row[3]}: {row[4]}")

    # Metric 5: Multi-table join using discovered keys
    print("\nMetric 5: Multi-table join (daily_prices + symbol_ref + sector_etfs)")
    rows = con.execute(f"""
        SELECT d.ticker, d.adj_close, s.sector_etf, e.adj_close AS etf_close
        FROM {scan('market.daily_prices')} d
        JOIN {scan('market.symbol_ref')} s ON d.ticker = s.ticker
        JOIN {scan('market.sector_etfs')} e
          ON s.sector_etf = e.etf_ticker AND d.trade_date = e.trade_date
        WHERE d.trade_date = '2025-01-15'
        LIMIT 10
    """).fetchall()
    passed = len(rows) > 0
    results["metric_5_multi_join"] = passed
    print(f"  Rows returned: {len(rows)} -> {'PASS' if passed else 'FAIL'}")
    if rows:
        print(f"  Sample: ticker={rows[0][0]}, adj_close={rows[0][1]:.2f}, sector={rows[0][2]}, etf_close={rows[0][3]:.2f}")

    # Metric 6: Row count matches JSON
    print(f"\nMetric 6: join_relationships row count matches Run 2 JSON ({expected_rows})")
    r = con.execute(f"""
        SELECT COUNT(*) FROM {scan('governance.join_relationships')}
    """).fetchone()[0]
    passed = r == expected_rows
    results["metric_6_row_count"] = passed
    print(f"  Iceberg rows: {r} -> {'PASS' if passed else 'FAIL'}")

    con.close()

    # Summary
    total = len(results)
    passed_count = sum(1 for v in results.values() if v)
    print(f"\n{'='*40}")
    print(f"  {passed_count}/{total} metrics passed")
    if passed_count == total:
        print("  Pipeline complete.")
    else:
        failed = [k for k, v in results.items() if not v]
        print(f"  Failed: {failed}")
    print(f"{'='*40}")


if __name__ == "__main__":
    expected = load_join_relationships()
    run_metrics(expected)
