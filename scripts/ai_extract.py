import json
import os
import sys

import anthropic
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

PROJECT_ROOT = os.path.join(os.path.dirname(__file__), "..")
DDL_PATH = os.path.join(PROJECT_ROOT, "data", "ddls.sql")
FREQ_PATH = os.path.join(PROJECT_ROOT, "data", "joins", "join_frequency.json")
OUTPUT_DIR = os.path.join(PROJECT_ROOT, "data", "joins")

client = anthropic.Anthropic()
MODEL = "claude-sonnet-4-20250514"

SYSTEM_PROMPT = """You are a data governance expert. Your job is to analyze database table definitions
and identify join key relationships between tables.

For each relationship you find, return a JSON object with these fields:
- table_a: fully qualified table name (e.g. "market.daily_prices")
- col_a: column name in table_a
- table_b: fully qualified table name
- col_b: column name in table_b
- confidence: "HIGH", "MEDIUM", or "LOW"
- join_type: "INNER", "LEFT", "CROSS", or "NATURAL"
- reasoning: brief explanation of why these columns should be joined
- warning: null if the join is safe, or a warning string if the join could be problematic

Return ONLY a JSON array of these objects. No markdown, no code fences, no explanation outside the JSON."""


def run_ddl_only():
    """Run 1: Extract join relationships from DDLs only."""
    print("Run 1: DDL only...")

    with open(DDL_PATH) as f:
        ddls = f.read()

    user_prompt = f"""Analyze these table DDLs and identify all possible join key relationships between tables.

DDLs:
{ddls}

Return a JSON array of join relationships."""

    response = client.messages.create(
        model=MODEL,
        max_tokens=4096,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": user_prompt}],
        temperature=0,
    )

    content = response.content[0].text.strip()
    # Strip markdown code fences if present
    if content.startswith("```"):
        content = content.split("\n", 1)[1]
        content = content.rsplit("```", 1)[0]

    results = json.loads(content)
    out_path = os.path.join(OUTPUT_DIR, "run1_ddl_only.json")
    with open(out_path, "w") as f:
        json.dump(results, f, indent=2)
    print(f"  {len(results)} relationships -> {out_path}")
    return results


def run_combined():
    """Run 2: Extract join relationships from DDLs + query history frequency map."""
    print("Run 2: DDL + query history...")

    with open(DDL_PATH) as f:
        ddls = f.read()

    with open(FREQ_PATH) as f:
        freq_map = json.load(f)

    user_prompt = f"""Analyze these table DDLs and the query history frequency map to identify all join key relationships.

The frequency map shows how often each join pair appears in real query history — use this to boost confidence
for frequently used joins and to discover relationships that may not be obvious from DDLs alone.

IMPORTANT: Flag any join on period_end_date or filed_date as a warning. These date columns often represent
different time semantics than trade_date or announcement_date and joining on them can produce incorrect results.

DDLs:
{ddls}

Query History Join Frequency Map:
{json.dumps(freq_map, indent=2)}

Return a JSON array of join relationships. Include a "frequency" field (integer) showing how many times
this join appeared in query history (0 if not observed)."""

    response = client.messages.create(
        model=MODEL,
        max_tokens=4096,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": user_prompt}],
        temperature=0,
    )

    content = response.content[0].text.strip()
    if content.startswith("```"):
        content = content.split("\n", 1)[1]
        content = content.rsplit("```", 1)[0]

    results = json.loads(content)
    out_path = os.path.join(OUTPUT_DIR, "run2_combined.json")
    with open(out_path, "w") as f:
        json.dump(results, f, indent=2)
    print(f"  {len(results)} relationships -> {out_path}")
    return results


def compare(run1, run2):
    """Compare Run 1 and Run 2 outputs."""
    print("\n--- Comparison ---")

    def pair_key(r):
        return (r["table_a"], r["col_a"], r["table_b"], r["col_b"])

    run1_keys = {pair_key(r) for r in run1}
    run2_keys = {pair_key(r) for r in run2}

    only_in_run2 = run2_keys - run1_keys
    only_in_run1 = run1_keys - run2_keys

    print(f"  Run 1 relationships: {len(run1)}")
    print(f"  Run 2 relationships: {len(run2)}")
    print(f"  Only in Run 2 (discovered via query history): {len(only_in_run2)}")
    for key in only_in_run2:
        print(f"    {key[0]}.{key[1]} -> {key[2]}.{key[3]}")
    if only_in_run1:
        print(f"  Only in Run 1: {len(only_in_run1)}")
        for key in only_in_run1:
            print(f"    {key[0]}.{key[1]} -> {key[2]}.{key[3]}")

    # Confidence changes
    run1_map = {pair_key(r): r.get("confidence") for r in run1}
    run2_map = {pair_key(r): r.get("confidence") for r in run2}
    print("\n  Confidence changes:")
    for key in run1_keys & run2_keys:
        c1, c2 = run1_map[key], run2_map[key]
        if c1 != c2:
            print(f"    {key[0]}.{key[1]} -> {key[2]}.{key[3]}: {c1} -> {c2}")

    # Warnings in Run 2
    warnings = [r for r in run2 if r.get("warning")]
    print(f"\n  Warnings in Run 2: {len(warnings)}")
    for w in warnings:
        print(f"    {w['table_a']}.{w['col_a']} -> {w['table_b']}.{w['col_b']}: {w['warning']}")

    comparison = {
        "run1_count": len(run1),
        "run2_count": len(run2),
        "only_in_run2": [{"table_a": k[0], "col_a": k[1], "table_b": k[2], "col_b": k[3]} for k in only_in_run2],
        "only_in_run1": [{"table_a": k[0], "col_a": k[1], "table_b": k[2], "col_b": k[3]} for k in only_in_run1],
        "warnings": [{"pair": f"{w['table_a']}.{w['col_a']} -> {w['table_b']}.{w['col_b']}", "warning": w["warning"]} for w in warnings],
    }

    comp_path = os.path.join(OUTPUT_DIR, "comparison.json")
    with open(comp_path, "w") as f:
        json.dump(comparison, f, indent=2)
    print(f"\n  Comparison -> {comp_path}")

    return comparison


def validate(run1, run2, comparison):
    """Validate outputs meet success criteria."""
    print("\n--- Validation ---")
    errors = []

    # Run 2 contains at least 1 relationship not in Run 1
    if len(comparison["only_in_run2"]) < 1:
        errors.append("Expected at least 1 relationship only in Run 2")
    else:
        print(f"  Relationships only in Run 2: {len(comparison['only_in_run2'])}")

    # At least 1 warning present
    if len(comparison["warnings"]) < 1:
        errors.append("Expected at least 1 warning in Run 2")
    else:
        print(f"  Warnings in Run 2: {len(comparison['warnings'])}")

    # At least 3 HIGH confidence in Run 2
    high_conf = [r for r in run2 if r.get("confidence") == "HIGH"]
    if len(high_conf) < 3:
        errors.append(f"Expected at least 3 HIGH confidence in Run 2, got {len(high_conf)}")
    else:
        print(f"  HIGH confidence in Run 2: {len(high_conf)}")

    if errors:
        print(f"\nFAILED with {len(errors)} error(s):")
        for e in errors:
            print(f"  - {e}")
    else:
        print("\nAll validations passed.")


def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "all"

    if mode == "ddl_only":
        run_ddl_only()
    elif mode == "combined":
        run_combined()
    elif mode == "all":
        run1 = run_ddl_only()
        run2 = run_combined()
        comp = compare(run1, run2)
        validate(run1, run2, comp)
    else:
        print(f"Unknown mode: {mode}. Use 'ddl_only', 'combined', or 'all'.")
        sys.exit(1)


if __name__ == "__main__":
    main()
