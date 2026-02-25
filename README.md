# ORION External Agents Pipeline

## Overview
A standalone Python CLI pipeline that crawls active sources, extracts candidate signals using 6 sequential OpenAI agents, and produces schema-locked CSV outputs matching the ORION driving_forces.xlsx format. Includes an optional synthesis phase to generate curated forces (Megatrends, Trends, Weak Signals, Wildcards) from accepted signals.

## Project Structure
```
├── src/               # Core Python modules
│   ├── config.py      # Configuration constants
│   ├── load_corpus.py # ORION corpus loader
│   ├── embeddings.py  # Text embedding utilities
│   ├── vector_index.py # Lexical similarity index
│   ├── sources.py     # Source list loader
│   ├── rotation.py    # Source rotation logic
│   ├── collector.py   # Web crawler/RSS fetcher
│   ├── export.py      # CSV export utilities
│   ├── synthesis.py   # Force synthesis from signals
│   └── pipeline.py    # Main 6-agent orchestration
├── agents/            # Agent definitions
│   └── prompts.json   # 6 agent prompts + schemas
├── inputs/            # Input Excel files
│   ├── driving_forces.xlsx
│   └── ORION_active_sources_top500.xlsx
├── out/               # Daily output directories
│   └── YYYY-MM-DD/    # Per-date outputs
├── Logs/              # Run logs (JSONL)
├── scripts/           # Utility scripts
│   └── check_append_schema.py
├── run_daily.py       # CLI entry point
└── requirements.txt   # Python dependencies
```

## Pipeline Agents
### Signal Extraction (5 agents)
1. **PATHFINDER** - Extracts 0-5 candidates per document
2. **COMPARATOR** - Compares against ORION corpus for duplicates
3. **SCORER** - Computes novelty/credibility/relevance scores
4. **CURATOR** - Normalizes taxonomy and produces ORION rows
5. **EXPORTER** - Prepares export manifests

### Force Synthesis (1 agent, optional)
6. **SYNTHESIZER** - Clusters accepted signals and generates curated forces (MT/T/WS/WC)

## Running the Pipeline
```bash
# Daily rotation (50 sources, rotates through all 500 over 10 days)
python run_daily.py --rotate

# Full sweep of all 500 sources (weekly)
python run_daily.py --full-sweep

# Standard mode with specific source count
python run_daily.py --max_sources 50

# With synthesis - generate curated forces from accepted signals
python run_daily.py --rotate --synthesize
```

## Reviewing & Promoting Signals

### Web Dashboard (Recommended)
Use the Streamlit dashboard for a visual interface:
- Select date from dropdown
- View pending signals in a table
- Check boxes to select signals
- Click "Promote Selected" or "Promote All" to add to master file
- See master file stats and breakdown
- View curated forces grouped by type (Megatrends, Trends, Weak Signals, Wildcards)
- Trace each force back to its source signals

The dashboard runs automatically on port 5000.

### CLI Alternative
For command-line review, use the promotion script:

```bash
# Interactive mode - review and select signals to promote
python scripts/promote_signals.py

# List pending signals without promoting
python scripts/promote_signals.py --list

# Promote specific signals by row number
python scripts/promote_signals.py --promote 1,3,5

# Promote a range of signals
python scripts/promote_signals.py --promote 1-10

# Promote all pending signals
python scripts/promote_signals.py --promote-all

# Review a specific date's pending signals
python scripts/promote_signals.py --date 2026-02-05
```

### CLI Arguments
- `--date`: Date string YYYY-MM-DD (defaults to today)
- `--rotate`: Enable source rotation (50 sources/day, 10-day cycle; configurable via ORION_BATCH_SIZE)
- `--full-sweep`: Process all 500 sources (for weekly complete scans)
- `--max_sources`: Limit sources to crawl when not using rotation (default: 100)
- `--max_docs_per_source`: Max documents per source (default: 2)
- `--max_docs_total`: Total document limit (default: 50)
- `--max_candidates_per_doc`: Max candidates extracted per doc (0 = default 5)
- `--max_runtime_seconds`: Stop if runtime exceeds this (0 = no limit)
- `--synthesize`: Run synthesis phase to generate curated forces from accepted signals

## Output Files (per date)
### Signal Files
- `orion_daily_all_candidates.csv` - All candidates with full scoring metadata
- `orion_daily_accepted.csv` - Accepted signals (20-column schema-locked)
- `orion_daily_pending_review.csv` - Signals needing human review
- `orion_daily_rejected.csv` - Rejected/duplicate signals
- `collector_report.json` - Crawl statistics

### Force Files (when --synthesize is used)
- `orion_forces_all_candidates.csv` - Forces with synthesis metadata
- `orion_forces_accepted.csv` - Curated forces (20-column schema-locked)

## Cumulative Master Files
- `out/orion_master.csv` - All accepted signals from all runs, appended automatically
- `out/orion_forces_master.csv` - All curated forces from all runs
- Duplicates are prevented by checking existing IDs
- No need to manually merge daily files

## Force Types
- **MT (Megatrend)**: Large-scale, long-term transformative change
- **T (Trend)**: Observable pattern of change with clear momentum
- **WS (Weak Signal)**: Early indicator of potential change, emerging and uncertain
- **WC (Wildcard)**: Low-probability, high-impact event or discontinuity

## Schema Lock (20 columns)
id, project_id, title, type, steep, dimension, scope, impact, ttm, sentiment, source, tags, text, magnitude, distance, color_hex, feasibility, urgency, created_at, updated_at

## Decision Thresholds
- **Accept**: priority_index ≥ 60 AND credibility ≥ 45
- **Review**: priority_index 45-59 OR credibility 25-44
- **Reject**: priority_index < 45 OR duplicate (similarity ≥ 0.92)

## Deployment Configuration
- **Scheduled Deployment**: `python start.py` runs on schedule (cron: `0 3 * * *`)
  - Streamlit dashboard on port 5000
  - Built-in APScheduler also available for VM deployment mode
  - Default schedule: 03:00 GMT (configurable via Replit deployment schedule)
  - Processes 50 sources per day, cycling through all 500 over 10 days (configurable via ORION_BATCH_SIZE env var)
  - Safety timeout: 2.5 hours (configurable via ORION_MAX_RUNTIME_SECONDS env var, default 9000s)
  - Platform job timeout: 3 hours (Replit scheduled deployment limit)
  - Generates curated forces from accepted signals

## User Preferences
- Uses Replit AI Integrations for OpenAI (no personal API key required)
- Default model: gpt-4o-mini (agents 1-5)
- Synthesizer model: gpt-4o (configurable via OPENAI_SYNTHESIZER_MODEL env var)
- Rate limiting: 2 seconds per domain

## Recent Changes
- 2026-02-13: Reduced batch size from 100 to 50 sources/day (10-day rotation) to fit within 3-hour scheduled deployment timeout
- 2026-02-13: Added ORION_BATCH_SIZE env var for configurable batch size (default: 50)
- 2026-02-13: Added ORION_MAX_RUNTIME_SECONDS safety timeout (default: 9000s / 2.5h) to prevent platform kill
- 2026-02-06: Upgraded SYNTHESIZER agent to GPT-4o for higher-quality force generation
- 2026-02-06: Reduced deployment size by ~217 MB (removed unused ORION_Scanning_DB and attached_assets)
- 2026-02-06: Configured scheduled deployment for daily rotation with synthesis
- 2026-02-06: Removed manual ORION Pipeline workflow (now handled by scheduled deployment)
- 2026-02-06: Switched to VM deployment with combined dashboard + APScheduler for always-on access
- 2026-02-05: Added Streamlit dashboard (dashboard.py) for visual signal review and promotion
- 2026-02-05: Added promote_signals.py script for reviewing and accepting pending signals
- 2026-02-05: Renamed output files for clarity (staging→all_candidates, append→accepted, review→pending_review, reject→rejected)
- 2026-02-05: Fixed feedparser timeout issue by pre-fetching with requests
- 2026-02-05: Added progress logging to collector
- 2026-02-05: Added ORIONSYNTHESIZER agent for curated force generation
- 2026-02-05: Added --synthesize CLI flag and synthesis module
- 2026-02-05: Added source rotation system (100 sources/day, 5-day cycle)
- 2026-02-05: Made --date optional (defaults to today)
- 2026-02-05: Fixed OpenAI model to gpt-4o-mini
- 2026-02-04: Initial project setup with 5 core agents
- 2026-02-04: Added cumulative master file export (out/orion_master.csv)


## Large Files (Not in Repo)

The following files are too large for GitHub and must be obtained separately:

- `inputs/driving_forces.xlsx` (64 MB) — ORION corpus containing 3000+ curated forces. Place in `inputs/` directory before running the pipeline.
