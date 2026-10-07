from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from ETL import config


def read_silver_data(spark: SparkSession, table: str) -> DataFrame:
    "Read one silver table into a dataframe"
    path = str(config.SILVER_DIR / f"{table}.{config.OUTPUT_FORMAT}")
    return spark.read.format(config.OUTPUT_FORMAT).load(path)


def add_key(df: DataFrame, key: str, natural_key: str) -> DataFrame:
    "Add a surrogate key 1..n in natural key order, reruns will give the same keys."
    window = Window.orderBy(natural_key)
    return df.select(F.row_number().over(window).alias(key), "*")

def build_dim_customer(customers: DataFrame) -> DataFrame:
    "One row per customer. Customers listed with two countries get Unknown."
    dim = customers.groupBy("customer_id").agg(
        F.count_distinct("country").alias("countries"),
        F.min("country").alias("country"),
    ).select(
        "customer_id",
        F.when(F.col("countries") > 1, config.UNKNOWN)
        .otherwise(F.col("country"))
        .alias("country"),
        (F.col("countries") > 1).alias("has_multiple_countries"),
    )
    dim = add_key(dim, "customer_key", "customer_id")
    unknown = dim.sparkSession.createDataFrame(
        [(config.UNKNOWN_KEY, None, config.UNKNOWN, False)], dim.schema
    )
    return dim.union(unknown)

def write_gold_data(df: DataFrame, table: str) -> None:
    "Write a dataframe to the gold layer"
    path = str(config.GOLD_DIR / f"{table}.{config.OUTPUT_FORMAT}")
    df.write.format(config.OUTPUT_FORMAT).mode("overwrite").save(path)

def run(spark: SparkSession) -> None:
    "Transform the data from silver to gold"
    ...
    #write_gold_data(df, table)

if __name__ == "__main__":
    from ETL.spark import get_spark

    run(get_spark())