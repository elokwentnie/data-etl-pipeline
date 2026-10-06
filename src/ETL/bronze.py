from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ETL import config


def check_columns(df: DataFrame, source: str) -> None:
    "Check that the dataframe has the required columns."
    required_columns = config.SOURCES[source]
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Source {source} is missing required columns: {missing_columns}")


def read_source(spark: SparkSession, source: str) -> DataFrame:
    "Read one raw csv as strings and add lineage columns."
    path = str(config.RAW_DIR / f"{source}.csv")
    df = spark.read.options(**config.CSV_OPTIONS).csv(path)
    check_columns(df, source)
    return df.select(
        "*",
        F.current_timestamp().alias("_ingested_at"),
        F.col("_metadata.file_path").alias("_source_file"),
    )


def write_bronze(df: DataFrame, source: str) -> None:
    "Write the dataframe to the bronze directory."
    path = str(config.BRONZE_DIR / f"{source}.{config.OUTPUT_FORMAT}")
    df.write.mode("overwrite").format(config.OUTPUT_FORMAT).save(path)


def run(spark: SparkSession) -> None:
    "Read the raw data and write the bronze data."
    for source in config.SOURCES:
        df = read_source(spark, source)
        write_bronze(df, source)

if __name__ == "__main__":
    from ETL.spark import get_spark

    run(get_spark())