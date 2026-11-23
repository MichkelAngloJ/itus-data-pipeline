from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = ROOT / "raw"
CONFIG_DIR = ROOT / "config"
OUTPUT_DIR = ROOT / "output"
DB_PATH = ROOT / "itus_pipeline.db"

# open-ended upper bound for SCD2 intervals
OPEN_END = "9999-12-31"
# lower bound for identifiers that were valid "since before our data starts"
BEGIN = "1900-01-01"

# canonical column layout for the monthly operations feed
OPS_COLUMNS = [
    "vendor_id", "company_name", "isin", "period",
    "capacity_units", "production_units", "employees", "published_at",
]
# known vendor renames -> canonical name
COLUMN_ALIASES = {"units_produced": "production_units"}

# a monthly file is "late" if it lands more than this many days after period end
LATE_AFTER_DAYS = 20

# soft thresholds used for warnings (not quarantine)
CAPACITY_STEP_PCT = 0.10
PRODUCTION_JUMP_PCT = 0.30
EMPLOYEES_JUMP_PCT = 0.15
# hard threshold: capacity moving by more than 10x month on month is a unit/scale error
CAPACITY_SCALE_RATIO = 10.0
