import csv
from pathlib import Path
from typing import List, Dict, Any, Tuple, Optional, Set

from .config import ORION_COLUMNS, OUTPUT_ROOT


MASTER_FILE = OUTPUT_ROOT / "orion_master.csv"


def validate_orion_schema(rows: List[Dict[str, Any]]) -> Tuple[bool, List[str]]:
    errors = []
    for i, row in enumerate(rows):
        keys = list(row.keys())
        if keys != ORION_COLUMNS:
            errors.append(f"Row {i} columns mismatch: {keys}")
    return (len(errors) == 0, errors)


def write_csv(path: Path, rows: List[Dict[str, Any]], fieldnames: Optional[List[str]] = None):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        if fieldnames is None:
            fieldnames = list(rows[0].keys()) if rows else []
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if writer.fieldnames:
            writer.writeheader()
        for row in rows:
            writer.writerow(row)


def write_exports(output_dir: Path, staging_rows: List[Dict[str, Any]], append_rows: List[Dict[str, Any]], review_rows: List[Dict[str, Any]], reject_rows: List[Dict[str, Any]]):
    output_dir.mkdir(parents=True, exist_ok=True)

    extra_keys: set = set()
    for row in staging_rows:
        extra_keys.update(set(row.keys()) - set(ORION_COLUMNS))
    staging_fieldnames = ORION_COLUMNS + sorted(extra_keys)

    write_csv(output_dir / "orion_daily_all_candidates.csv", staging_rows, fieldnames=staging_fieldnames)
    write_csv(output_dir / "orion_daily_accepted.csv", append_rows, fieldnames=ORION_COLUMNS)
    write_csv(output_dir / "orion_daily_pending_review.csv", review_rows, fieldnames=staging_fieldnames)
    write_csv(output_dir / "orion_daily_rejected.csv", reject_rows, fieldnames=staging_fieldnames)


def _load_existing_ids(path: Path) -> Set[str]:
    if not path.exists():
        return set()
    ids: Set[str] = set()
    with path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get("id"):
                ids.add(row["id"])
    return ids


def append_to_master(append_rows: List[Dict[str, Any]], master_path: Path = MASTER_FILE) -> int:
    if not append_rows:
        return 0

    master_path.parent.mkdir(parents=True, exist_ok=True)
    existing_ids = _load_existing_ids(master_path)

    new_rows = [row for row in append_rows if row.get("id") not in existing_ids]
    if not new_rows:
        return 0

    file_exists = master_path.exists() and master_path.stat().st_size > 0
    with master_path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=ORION_COLUMNS)
        if not file_exists:
            writer.writeheader()
        for row in new_rows:
            writer.writerow({k: row.get(k, "") for k in ORION_COLUMNS})

    return len(new_rows)
