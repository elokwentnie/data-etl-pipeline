from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from ETL import config


def read_silver_data(spark: SparkSession, table: str) -> DataFrame:
    "Read one silver table into a dataframe"
    path = str(config.SILVER_DIR / f"{table}.{config.OUTPUT_FORMAT}")
    return spark.read.format(config.OUTPUT_FORMAT).load(path)


def add_key(df: DataFrame, key: str, natural_key: str | list[str]) -> DataFrame:
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

def build_dim_product(products: DataFrame) -> DataFrame:
    "One row per stock code, with one description and one median price"
    non_zero_price = F.when(F.col("unit_price") > 0, F.col("unit_price"))
    dim = products.groupBy("stock_code").agg(
        F.coalesce(
            F.mode("description", deterministic=True),
            F.lit(config.UNKNOWN),
        ).alias("description"),
        F.round(F.median(non_zero_price), 2).cast("decimal(10,2)").alias("unit_price"),
        F.min(non_zero_price).alias("price_min"),
        F.max(non_zero_price).alias("price_max"),
    )
    dim = add_key(dim, "product_key", "stock_code")
    unknown = dim.sparkSession.createDataFrame(
         [(config.UNKNOWN_KEY, config.UNKNOWN, config.UNKNOWN, None, None, None)], dim.schema
    )
    return dim.union(unknown)

def build_dim_date(orders: DataFrame) -> DataFrame:
    "One row per day, covering every month that has orders."
    date_range = orders.select(
        F.trunc(F.to_date(F.min("invoice_ts")), "month").alias("start"),
        F.last_day(F.to_date(F.max("invoice_ts"))).alias("end"),
    )
    dates = date_range.select(F.explode(F.sequence("start", "end")).alias("date"))
    return dates.select(
        F.date_format("date", "yyyyMMdd").cast("int").alias("date_key"),
        "date",
        F.year("date").alias("year"),
        F.quarter("date").alias("quarter"),
        F.month("date").alias("month"),
        F.date_format("date", "MMMM").alias("month_name"),
        F.dayofmonth("date").alias("day_of_month"),
        (F.weekday("date") + 1).alias("day_of_week"),
        F.date_format("date", "EEEE").alias("day_name"),
        (F.weekday("date") >= 5).alias("is_weekend"),
    )

def build_fact_sales(
    orders: DataFrame, dim_customer: DataFrame, dim_product: DataFrame) -> DataFrame:
    "One row per order line, with dimension keys and the chosen price"

    customers = dim_customer.select("customer_id", "customer_key")
    products = dim_product.select("stock_code", "product_key", "unit_price")

    fact = (
        orders.join(customers, "customer_id", "left")
        .join(products, "stock_code", "left")
        .select(
            "invoice_no",
            "line_type",
            F.date_format("invoice_ts", "yyyyMMdd").cast("int").alias("date_key"),
            F.coalesce("customer_key", F.lit(config.UNKNOWN_KEY)).alias("customer_key"),
            F.coalesce("product_key", F.lit(config.UNKNOWN_KEY)).alias("product_key"),
            "invoice_ts",
            "quantity",
            "unit_price",
            (F.col("quantity") * F.col("unit_price")).cast("decimal(12,2)").alias("line_amount"),
        )
    )
    line_order = ["invoice_no", "product_key", "invoice_ts", "quantity", "customer_key"]
    return add_key(fact, "sales_key", line_order)

def write_gold_data(df: DataFrame, table: str) -> None:
    "Write a dataframe to the gold layer"
    path = str(config.GOLD_DIR / f"{table}.{config.OUTPUT_FORMAT}")
    df.write.format(config.OUTPUT_FORMAT).mode("overwrite").save(path)

def run(spark: SparkSession) -> None:
    "Build the gold dimensions from silver and the sales fact table."

    orders = read_silver_data(spark, "orders")

    dim_customer = build_dim_customer(read_silver_data(spark, "customers"))
    dim_product = build_dim_product(read_silver_data(spark, "products"))
    dim_date = build_dim_date(read_silver_data(spark, "orders"))
    fact_sales = build_fact_sales(orders, dim_customer, dim_product)

    tables = {
        "dim_customer": dim_customer,
        "dim_product": dim_product,
        "dim_date": dim_date,
        "fact_sales": fact_sales,
    }
    
    for table, df in tables.items():
        write_gold_data(df, table)

if __name__ == "__main__":
    from ETL.spark import get_spark

    run(get_spark())