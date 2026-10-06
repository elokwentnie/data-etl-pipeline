from pathlib import Path

# Paths
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data"

RAW_DIR = DATA_DIR / "raw"
BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
GOLD_DIR = DATA_DIR / "gold"

# Sources and the columns each raw file must have
SOURCES = {
    "customers": ["CustomerID", "Country"],
    "products": ["StockCode", "Description", "UnitPrice"],
    "orders": ["InvoiceNo", "StockCode", "Quantity", "InvoiceDate", "CustomerID"],
}

CSV_OPTIONS = {
    "header": "true",
    "encoding": "utf-8",
}

OUTPUT_FORMAT = "parquet"

# Spark
APP_NAME = "retail-data-etl"
MASTER = "local[*]"
LOG_LEVEL = "WARN"

# Spark 4.2 needs Java 17. Used only when JAVA_HOME is not set.
DEFAULT_JAVA_HOME = "/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home"

SPARK_CONF = {
    "spark.sql.shuffle.partitions": "8",
    "spark.sql.ansi.enabled": "true",
    "spark.sql.session.timeZone": "UTC",
}
