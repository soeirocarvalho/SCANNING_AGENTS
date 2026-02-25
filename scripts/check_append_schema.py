#!/usr/bin/env python3
import csv
import sys
from pathlib import Path

ORION_COLUMNS = [
    "id",
    "project_id",
    "title",
    "type",
    "steep",
    "dimension",
    "scope",
    "impact",
    "ttm",
    "sentiment",
    "source",
    "tags",
    "text",
    "magnitude",
    "distance",
    "color_hex",
    "feasibility",
    "urgency",
    "created_at",
    "updated_at",
]


def check_schema(csv_path: Path) -> bool:
    if not csv_path.exists():
        print(f"ERROR: File not found: {csv_path}")
        return False

    with csv_path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames or []

        if list(headers) != ORION_COLUMNS:
            print(f"ERROR: Column mismatch")
            print(f"Expected: {ORION_COLUMNS}")
            print(f"Got:      {list(headers)}")
            return False

        row_count = sum(1 for _ in reader)
        print(f"OK: Schema matches. {row_count} data rows.")
        return True


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python check_append_schema.py <path_to_orion_daily_append.csv>")
        sys.exit(1)

    path = Path(sys.argv[1])
    success = check_schema(path)
    sys.exit(0 if success else 1)
