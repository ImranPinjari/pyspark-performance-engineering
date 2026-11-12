from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent

CUSTOMERS_PATH = PROJECT_ROOT / "data" / "input" / "customers.csv"
ORDERS_PATH = PROJECT_ROOT / "data" / "input" / "orders.csv"
OUTPUT_DIR = PROJECT_ROOT / "data" / "output"
REPORT_DIR = OUTPUT_DIR / "reports"

APP_NAME = "PySparkPerformanceEngineering"
SHUFFLE_PARTITIONS = 8
