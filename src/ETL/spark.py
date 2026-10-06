import os

from pyspark.sql import SparkSession

from ETL import config


def get_spark() -> SparkSession:
    """Return the shared local Spark session, creating it on first call."""
    os.environ.setdefault("JAVA_HOME", config.DEFAULT_JAVA_HOME)

    builder = SparkSession.builder.appName(config.APP_NAME).master(config.MASTER)
    for key, value in config.SPARK_CONF.items():
        builder = builder.config(key, value)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(config.LOG_LEVEL)
    return spark
