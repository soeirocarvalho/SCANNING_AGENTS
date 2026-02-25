import argparse
from datetime import datetime, date

from src.pipeline import run_pipeline
from src.sources import load_sources
from src.rotation import get_rotated_sources, get_rotation_info


def main():
    parser = argparse.ArgumentParser(description="ORION External Agents Daily Pipeline")
    parser.add_argument("--date", default=None, help="YYYY-MM-DD (defaults to today)")
    parser.add_argument("--max_sources", type=int, default=100, help="Max sources (ignored if --rotate)")
    parser.add_argument("--max_docs_per_source", type=int, default=2)
    parser.add_argument("--max_docs_total", type=int, default=50, help="Limit total docs processed (0 = no limit)")
    parser.add_argument("--max_candidates_per_doc", type=int, default=0, help="Limit candidates per doc (0 = default)")
    parser.add_argument("--max_runtime_seconds", type=int, default=0, help="Stop if runtime exceeds this many seconds")
    parser.add_argument("--rotate", action="store_true", help="Use source rotation (100 sources/day, 5-day cycle)")
    parser.add_argument("--full-sweep", action="store_true", help="Process all 500 sources (weekly full scan)")
    parser.add_argument("--synthesize", action="store_true", help="Run synthesis phase to generate curated forces from accepted signals")
    args = parser.parse_args()

    if args.date:
        date_str = args.date
        datetime.strptime(date_str, "%Y-%m-%d")
    else:
        date_str = date.today().strftime("%Y-%m-%d")
    
    all_sources = load_sources()
    
    if args.full_sweep:
        sources = all_sources
        print(f"Full sweep mode: processing all {len(sources)} sources")
    elif args.rotate:
        sources = get_rotated_sources(all_sources, date_str)
        info = get_rotation_info(all_sources, date_str)
        print(f"Rotation mode: Day {info['day_in_cycle']}/{info['total_days_in_cycle']} - processing sources {info['current_offset']+1} to {info['current_offset']+len(sources)}")
    else:
        sources = all_sources[:args.max_sources]
        print(f"Standard mode: processing top {len(sources)} sources")
    
    if args.synthesize:
        print("Synthesis mode: will generate curated forces from accepted signals")
    
    summary = run_pipeline(
        date_str,
        len(sources),
        args.max_docs_per_source,
        args.max_docs_total or None,
        args.max_candidates_per_doc or None,
        args.max_runtime_seconds or None,
        sources_override=sources,
        run_synthesis=args.synthesize,
    )

    print("\nRun summary")
    print(f"Date: {date_str}")
    print(f"Docs fetched: {summary.docs_fetched} | Failed: {summary.docs_failed}")
    print(f"Candidates: {summary.candidates}")
    print(f"Accept: {summary.accept} | Review: {summary.review} | Reject: {summary.reject}")
    if args.synthesize:
        print(f"Forces created: {summary.forces_created}")
    print(f"Importance distribution: {summary.importance_distribution}")


if __name__ == "__main__":
    main()
