from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ETL import cleaning, config

def read_bronze_data(spark: SparkSession, table: str) -> DataFrame:
    "Read one bronze table into a dataframe"
    path = str(config.BRONZE_PATH / f"{table}.{config.OUTPUT_FORMAT}")
    return spark.read.format(config.OUTPUT_FORMAT).load(path)

def clean_customers(df: DataFrame) -> DataFrame:
    "Custoemrs with clean country names, rows without an id are dropped"
    return df.filter(F.col("CustomerID").isNotNull()).select(
        F.col("CustomerID").cast("int").alias("customer_id"),
        cleaning.clean_country(F.col("Country")).alias("country"),
    )

def clean_products(df: DataFrame) -> DataFrame:
    "Products with normalized codes. Notes are removed from the description."
    return df.select(
        cleaning.normalize_stock_code(F.col("StockCode")).alias("stock_code"),
        cleaning.clean_description(F.col("Description")).alias("description"),
        F.col("UnitPrice").cast("decimal(10, 2)").alias("unit_price"),
    )

def clean_orders(df: DataFrame) -> DataFrame:
    "Orders, without duplicates, with a fixed timestamp and a line type"
    orders = df.dropDuplicates(config.SOURCES["orders"]).select(
        F.trim(F.col("InvoiceNo")).alias("invoice_no"),
        cleaning.normalize_stock_code(F.col("StockCode")).alias("stock_code"),
        F.col("Quantity").cast("int").alias("quantity"),
        cleaning.parse_invoice_ts(F.col("InvoiceDate")).alias("invoice_ts"),
        F.col("CustomerID").cast("int").alias("customer_id"),
    )
    return cleaning.fill_invoice_ts(orders).withColumn(
        "line_type", cleaning.line_type(F.col("invoice_no"), F.col("quantity"))
    )


CLEANERS = {
    "customers": clean_customers,
    "products": clean_products,
}

REQUIRED_COLUMNS = {
    "customers": ["customer_id", "country"],
    "products": ["stock_code", "unit_price"], # we accept null as a description, it will be resolved in gold layer
}