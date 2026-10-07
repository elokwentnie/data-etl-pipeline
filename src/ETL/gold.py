from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from ETL import config


def read_silver_data(spark: SparkSession, table: str) -> DataFrame:
    "Read one silver table into a dataframe"
    path = str(config.SILVER_DIR / f"{table}.{config.OUTPUT_FORMAT}")
    return spark.read.format(config.OUTPUT_FORMAT).load(path)

def write_gold_data(df: DataFrame, table: str) -> None:
    "Write a dataframe to the gold layer"
    path = str(config.GOLD_DIR / f"{table}.{config.OUTPUT_FORMAT}")
    df.write.format(config.OUTPUT_FORMAT).mode("overwrite").save(path)

if __name__ == "__main__":
    from ETL.spark import get_spark

    run(get_spark())