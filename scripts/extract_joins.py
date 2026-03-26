import json
import os
from collections import Counter

import sqlglot
from sqlglot import exp

PROJECT_ROOT = os.path.join(os.path.dirname(__file__), "..")
QUERY_HISTORY = os.path.join(PROJECT_ROOT, "data", "query_history.json")
OUTPUT_PATH = os.path.join(PROJECT_ROOT, "data", "joins", "join_frequency.json")


def resolve_alias(alias_map, alias):
    return alias_map.get(alias, alias)


def build_alias_map(parsed):
    alias_map = {}
    for table in parsed.find_all(exp.Table):
        full_name = table.name
        if table.args.get("db"):
            full_name = f"{table.args['db'].name}.{table.name}"
        alias = table.alias
        if alias:
            alias_map[alias] = full_name
        else:
            alias_map[table.name] = full_name
    return alias_map


def extract_join_pairs(parsed, alias_map):
    pairs = []
    for join in parsed.find_all(exp.Join):
        on_clause = join.args.get("on")
        if not on_clause:
            continue
        # on_clause may be a single EQ or an And containing multiple EQs
        eqs = list(on_clause.find_all(exp.EQ)) if not isinstance(on_clause, exp.EQ) else [on_clause]
        for eq in eqs:
            left = eq.left
            right = eq.right
            if isinstance(left, exp.Column) and isinstance(right, exp.Column):
                left_table = resolve_alias(alias_map, str(left.table)) if left.table else "UNKNOWN"
                right_table = resolve_alias(alias_map, str(right.table)) if right.table else "UNKNOWN"
                pairs.append({
                    "table_a": left_table,
                    "col_a": left.name,
                    "table_b": right_table,
                    "col_b": right.name,
                })
    return pairs


def main():
    with open(QUERY_HISTORY) as f:
        queries = json.load(f)

    print(f"Parsing {len(queries)} queries...")

    all_pairs = []
    frequency = Counter()

    for i, query in enumerate(queries):
        try:
            parsed = sqlglot.parse_one(query)
            alias_map = build_alias_map(parsed)
            pairs = extract_join_pairs(parsed, alias_map)
            for p in pairs:
                key = (p["table_a"], p["col_a"], p["table_b"], p["col_b"])
                frequency[key] += 1
                all_pairs.append(p)
            if pairs:
                print(f"  Query {i+1}: {len(pairs)} join pair(s)")
            else:
                print(f"  Query {i+1}: no joins")
        except Exception as e:
            print(f"  Query {i+1}: parse error - {e}")

    # Build freq map output
    freq_list = []
    for (table_a, col_a, table_b, col_b), count in frequency.most_common():
        freq_list.append({
            "table_a": table_a,
            "col_a": col_a,
            "table_b": table_b,
            "col_b": col_b,
            "frequency": count,
        })

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    with open(OUTPUT_PATH, "w") as f:
        json.dump(freq_list, f, indent=2)
    print(f"\n{len(freq_list)} distinct join pairs -> {OUTPUT_PATH}")

    # Validation
    print("\n--- Validation ---")
    errors = []

    if len(freq_list) < 10:
        errors.append(f"Expected >= 10 distinct join pairs, got {len(freq_list)}")

    ticker_join = [p for p in freq_list
                   if p["col_a"] == "ticker" and p["col_b"] == "ticker"
                   and "daily_prices" in p["table_a"] and "symbol_ref" in p["table_b"]]
    if ticker_join and ticker_join[0]["frequency"] >= 5:
        print(f"  daily_prices.ticker -> symbol_ref.ticker frequency: {ticker_join[0]['frequency']}")
    else:
        errors.append("daily_prices.ticker -> symbol_ref.ticker frequency < 5")

    print(f"  Distinct join pairs: {len(freq_list)}")

    if errors:
        print(f"\nFAILED with {len(errors)} error(s):")
        for e in errors:
            print(f"  - {e}")
    else:
        print("\nAll validations passed.")


if __name__ == "__main__":
    main()
