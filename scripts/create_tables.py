import os

import duckdb
import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    NestedField,
    StringType,
)

PROJECT_ROOT = os.path.join(os.path.dirname(__file__), "..")
WAREHOUSE_DIR = os.path.join(PROJECT_ROOT, "warehouse")
CATALOG_DB = os.path.join(WAREHOUSE_DIR, "catalog.db")
os.makedirs(WAREHOUSE_DIR, exist_ok=True)

catalog = SqlCatalog(
    "local",
    **{
        "uri": f"sqlite:///{CATALOG_DB}",
        "warehouse": f"file://{os.path.abspath(WAREHOUSE_DIR)}",
    },
)

TABLES = {
    "market.symbol_ref": Schema(
        NestedField(1, "ticker", StringType(), required=True),
        NestedField(2, "company_name", StringType()),
        NestedField(3, "sector_etf", StringType()),
        NestedField(4, "market_cap_tier", StringType()),
        NestedField(5, "sp500_flag", BooleanType()),
    ),
    "market.daily_prices": Schema(
        NestedField(1, "ticker", StringType(), required=True),
        NestedField(2, "trade_date", DateType(), required=True),
        NestedField(3, "open", DoubleType()),
        NestedField(4, "high", DoubleType()),
        NestedField(5, "low", DoubleType()),
        NestedField(6, "close", DoubleType()),
        NestedField(7, "adj_close", DoubleType()),
        NestedField(8, "volume", LongType()),
    ),
    "market.sector_etfs": Schema(
        NestedField(1, "etf_ticker", StringType(), required=True),
        NestedField(2, "trade_date", DateType(), required=True),
        NestedField(3, "open", DoubleType()),
        NestedField(4, "high", DoubleType()),
        NestedField(5, "low", DoubleType()),
        NestedField(6, "close", DoubleType()),
        NestedField(7, "adj_close", DoubleType()),
        NestedField(8, "volume", LongType()),
    ),
    "options.options_chain": Schema(
        NestedField(1, "contract_id", StringType(), required=True),
        NestedField(2, "ticker", StringType(), required=True),
        NestedField(3, "trade_date", DateType(), required=True),
        NestedField(4, "expiration_date", DateType()),
        NestedField(5, "strike_price", DoubleType()),
        NestedField(6, "option_type", StringType()),
        NestedField(7, "delta", DoubleType()),
        NestedField(8, "iv", DoubleType()),
        NestedField(9, "pop", DoubleType()),
        NestedField(10, "dte", IntegerType()),
    ),
    "macro.macro_indicators": Schema(
        NestedField(1, "indicator_code", StringType(), required=True),
        NestedField(2, "announcement_date", DateType(), required=True),
        NestedField(3, "period_end_date", DateType()),
        NestedField(4, "value", DoubleType()),
        NestedField(5, "prior_value", DoubleType()),
    ),
    "governance.join_relationships": Schema(
        NestedField(1, "table_a", StringType(), required=True),
        NestedField(2, "col_a", StringType(), required=True),
        NestedField(3, "table_b", StringType(), required=True),
        NestedField(4, "col_b", StringType(), required=True),
        NestedField(5, "confidence", StringType()),
        NestedField(6, "frequency", IntegerType()),
        NestedField(7, "reasoning", StringType()),
        NestedField(8, "warning", StringType()),
    ),
}


DDL_TYPE_MAP = {
    StringType: "STRING",
    DoubleType: "DOUBLE",
    LongType: "BIGINT",
    IntegerType: "INT",
    DateType: "DATE",
    BooleanType: "BOOLEAN",
}


def schema_to_ddl(table_name: str, schema: Schema) -> str:
    cols = []
    for field in schema.fields:
        sql_type = DDL_TYPE_MAP.get(type(field.field_type), "STRING")
        nullable = "" if field.required else " -- nullable"
        cols.append(f"  {field.name} {sql_type}{nullable}")
    col_str = ",\n".join(cols)
    return f"CREATE TABLE {table_name} (\n{col_str}\n);"


def create_tables():
    print("Creating Iceberg tables...")

    for full_name, schema in TABLES.items():
        namespace, table_name = full_name.split(".")

        existing_ns = [ns[0] for ns in catalog.list_namespaces()]
        if namespace not in existing_ns:
            catalog.create_namespace(namespace)
            print(f"  Created namespace: {namespace}")

        existing_tables = [t[1] for t in catalog.list_tables(namespace)]
        if table_name in existing_tables:
            catalog.drop_table(f"{namespace}.{table_name}")

        catalog.create_table(f"{namespace}.{table_name}", schema=schema)
        print(f"  Created table: {full_name}")

    # Save DDLs for LLM input
    ddl_path = os.path.join(PROJECT_ROOT, "data", "ddls.sql")
    os.makedirs(os.path.dirname(ddl_path), exist_ok=True)
    with open(ddl_path, "w") as f:
        for full_name, schema in TABLES.items():
            f.write(schema_to_ddl(full_name, schema))
            f.write("\n\n")
    print(f"\n  DDLs saved to {ddl_path}")


def validate():
    print("\n--- Validation ---")
    errors = []

    for full_name in TABLES:
        namespace, table_name = full_name.split(".")
        try:
            table = catalog.load_table(f"{namespace}.{table_name}")
            print(f"  {full_name}: exists, {len(table.schema().fields)} columns")
        except Exception as e:
            errors.append(f"{full_name}: {e}")

    print("\n  Testing DuckDB iceberg_scan reads...")
    con = duckdb.connect()
    con.execute("INSTALL iceberg; LOAD iceberg;")
    con.execute("SET unsafe_enable_version_guessing = true;")

    for full_name in TABLES:
        namespace, table_name = full_name.split(".")
        table_dir = os.path.abspath(os.path.join(WAREHOUSE_DIR, namespace, table_name))

        try:
            result = con.execute(
                f"SELECT * FROM iceberg_scan('{table_dir}', allow_moved_paths = true) LIMIT 0"
            ).fetchdf()
            print(f"  {full_name}: iceberg_scan OK ({len(result.columns)} columns)")
        except Exception as e:
            errors.append(f"{full_name} iceberg_scan: {e}")

    con.close()

    if errors:
        print(f"\nFAILED with {len(errors)} error(s):")
        for e in errors:
            print(f"  - {e}")
    else:
        print("\nAll validations passed.")


if __name__ == "__main__":
    create_tables()
    validate()
