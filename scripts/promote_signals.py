#!/usr/bin/env python3
"""
Signal Promotion Tool

Review pending signals and promote selected ones to the master file.

Usage:
    python scripts/promote_signals.py [--date YYYY-MM-DD]
    python scripts/promote_signals.py --list           # List pending signals
    python scripts/promote_signals.py --promote 1,2,5  # Promote by row number
    python scripts/promote_signals.py --promote-all    # Promote all pending signals
"""
import argparse
import csv
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.config import ORION_COLUMNS, OUTPUT_ROOT
from src.export import append_to_master

MASTER_FILE = OUTPUT_ROOT / "orion_master.csv"


def get_pending_file(date_str: str) -> Path:
    new_path = OUTPUT_ROOT / date_str / "orion_daily_pending_review.csv"
    if new_path.exists():
        return new_path
    old_path = OUTPUT_ROOT / date_str / "orion_daily_review.csv"
    if old_path.exists():
        return old_path
    return new_path


def load_pending_signals(date_str: str) -> list:
    pending_file = get_pending_file(date_str)
    if not pending_file.exists():
        print(f"No pending review file found for {date_str}")
        print(f"Expected: {pending_file}")
        return []
    
    signals = []
    with pending_file.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            signals.append(row)
    return signals


def display_signals(signals: list):
    if not signals:
        print("No pending signals to review.")
        return
    
    print(f"\n{'='*80}")
    print(f"PENDING SIGNALS FOR REVIEW ({len(signals)} total)")
    print(f"{'='*80}\n")
    
    for i, sig in enumerate(signals, 1):
        title = sig.get("title", "Untitled")[:60]
        steep = sig.get("steep", "?")[:12]
        dimension = sig.get("dimension", "?")[:15]
        source = sig.get("source", "")[:30]
        priority = sig.get("priority_index", "?")
        credibility = sig.get("credibility_score", "?")
        
        print(f"[{i:3}] {title}")
        print(f"      STEEP: {steep} | Dimension: {dimension}")
        print(f"      Priority: {priority} | Credibility: {credibility}")
        print(f"      Source: {source}")
        print()


def promote_signals(signals: list, indices: list) -> int:
    to_promote = []
    for idx in indices:
        if 1 <= idx <= len(signals):
            row = signals[idx - 1]
            orion_row = {k: row.get(k, "") for k in ORION_COLUMNS}
            to_promote.append(orion_row)
        else:
            print(f"Warning: Index {idx} out of range, skipping")
    
    if not to_promote:
        print("No valid signals to promote.")
        return 0
    
    added = append_to_master(to_promote, MASTER_FILE)
    return added


def interactive_mode(signals: list):
    display_signals(signals)
    
    if not signals:
        return
    
    print("Enter signal numbers to promote (comma-separated), 'all' for all, or 'q' to quit:")
    print("Example: 1,3,5 or 1-10 or all")
    
    while True:
        try:
            user_input = input("\n> ").strip().lower()
        except (EOFError, KeyboardInterrupt):
            print("\nExiting.")
            break
        
        if user_input in ("q", "quit", "exit"):
            print("Exiting without changes.")
            break
        
        if user_input == "all":
            indices = list(range(1, len(signals) + 1))
        elif "-" in user_input and "," not in user_input:
            try:
                start, end = user_input.split("-")
                indices = list(range(int(start), int(end) + 1))
            except ValueError:
                print("Invalid range. Use format: 1-10")
                continue
        else:
            try:
                indices = [int(x.strip()) for x in user_input.split(",") if x.strip()]
            except ValueError:
                print("Invalid input. Enter numbers separated by commas.")
                continue
        
        if not indices:
            print("No signals selected.")
            continue
        
        print(f"\nPromoting {len(indices)} signal(s)...")
        added = promote_signals(signals, indices)
        print(f"Added {added} new signal(s) to master file.")
        print(f"Master file: {MASTER_FILE}")
        break


def main():
    parser = argparse.ArgumentParser(description="Review and promote pending signals")
    parser.add_argument("--date", default=None, help="Date folder to review (YYYY-MM-DD)")
    parser.add_argument("--list", action="store_true", help="List pending signals and exit")
    parser.add_argument("--promote", type=str, help="Comma-separated indices to promote (e.g., 1,2,5)")
    parser.add_argument("--promote-all", action="store_true", help="Promote all pending signals")
    args = parser.parse_args()
    
    date_str = args.date or date.today().strftime("%Y-%m-%d")
    signals = load_pending_signals(date_str)
    
    if args.list:
        display_signals(signals)
        return
    
    if args.promote_all:
        if signals:
            indices = list(range(1, len(signals) + 1))
            added = promote_signals(signals, indices)
            print(f"Promoted {added} signal(s) to master file.")
        else:
            print("No signals to promote.")
        return
    
    if args.promote:
        try:
            indices = [int(x.strip()) for x in args.promote.split(",")]
            added = promote_signals(signals, indices)
            print(f"Promoted {added} signal(s) to master file.")
        except ValueError:
            print("Invalid indices. Use comma-separated numbers.")
        return
    
    interactive_mode(signals)


if __name__ == "__main__":
    main()
