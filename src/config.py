from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
INPUT_DIR = ROOT / "inputs"
OUTPUT_ROOT = ROOT / "out"
LOG_DIR = ROOT / "Logs"

DRIVING_FORCES_PATH = INPUT_DIR / "driving_forces.xlsx"
SOURCES_PATH = INPUT_DIR / "ORION_active_sources_top500.xlsx"
SOURCES_SHEET = "Active_500"

AGENTS_PATHS = [ROOT / "agents" / "prompts.json", ROOT / "agents" / "Prompts.json"]

OPENAI_MODEL = "gpt-4o-mini"
OPENAI_SYNTHESIZER_MODEL = "gpt-4o"
OPENAI_TIMEOUT_SECONDS = 30
OPENAI_MAX_RETRIES = 2
OPENAI_FORCE_RESPONSES = False
MAX_JSON_REPAIR_ATTEMPTS = 1

MAX_CANDIDATES_PER_DOC = 5
DUPLICATE_SIMILARITY = 0.92

ACCEPT_PRIORITY = 60
REVIEW_MIN_PRIORITY = 45
MIN_CREDIBILITY_ACCEPT = 45
MIN_CREDIBILITY_REVIEW = 25

RATE_LIMIT_SECONDS = 2.0
REQUEST_TIMEOUT_SECONDS = 12
MIN_DOC_TEXT_LENGTH = 400
MAX_FEED_DISCOVERY = 5
MAX_RUNTIME_SECONDS = 0

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

IMPORTANCE_BINS = [
    (0, 14, 1),
    (15, 24, 2),
    (25, 34, 3),
    (35, 44, 4),
    (45, 54, 5),
    (55, 64, 6),
    (65, 74, 7),
    (75, 84, 8),
    (85, 92, 9),
    (93, 100, 10),
]
