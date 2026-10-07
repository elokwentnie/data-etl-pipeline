from ETL import bronze, gold, silver
from ETL.spark import get_spark


def main() -> None:
    """Run the whole pipeline: raw to bronze to silver to gold."""
    spark = get_spark()
    try:
        bronze.run(spark)
        silver.run(spark)
        gold.run(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
