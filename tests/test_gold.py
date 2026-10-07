from decimal import Decimal

from pyspark.sql import functions as F

from ETL import gold


def test_dim_customer_flags_two_countries_and_adds_unknown(spark):
    customers = spark.createDataFrame(
        [(12346, "United Kingdom"), (12370, "Austria"), (12370, "Cyprus")],
        "customer_id int, country string",
    )
    result = gold.build_dim_customer(customers).orderBy("customer_key").collect()
    assert [tuple(row) for row in result] == [
        (-1, None, "Unknown", False),
        (1, 12346, "United Kingdom", False),
        (2, 12370, "Unknown", True),
    ]


def test_dim_product_picks_one_price_and_description(spark):
    products = spark.createDataFrame(
        [
            ("10002", "GLOBE", Decimal("0.00")),
            ("10002", "GLOBE", Decimal("2.00")),
            ("10002", None, Decimal("3.00")),
            ("10002", "INFLATABLE GLOBE", Decimal("10.00")),
            ("22196", None, Decimal("0.00")),
        ],
        "stock_code string, description string, unit_price decimal(10,2)",
    )
    result = gold.build_dim_product(products).orderBy("product_key").collect()
    assert [tuple(row) for row in result] == [
        (-1, "Unknown", "Unknown", None, None, None),
        (1, "10002", "GLOBE", Decimal("3.00"), Decimal("2.00"), Decimal("10.00")),
        (2, "22196", "Unknown", None, None, None),
    ]


def test_dim_product_breaks_description_ties_alphabetically(spark):
    products = spark.createDataFrame(
        [("10080", "ZEBRA", Decimal("1.00")), ("10080", "APPLE", Decimal("1.00"))],
        "stock_code string, description string, unit_price decimal(10,2)",
    )
    result = gold.build_dim_product(products).filter("product_key = 1").first()
    assert result.description == "APPLE"


def test_dim_date_covers_whole_months(spark):
    orders = spark.createDataFrame(
        [("2010-12-15 10:00",), ("2011-01-03 09:00",)], "ts string"
    ).select(F.to_timestamp("ts").alias("invoice_ts"))
    dim = gold.build_dim_date(orders)

    keys = [row.date_key for row in dim.orderBy("date_key").collect()]
    assert len(keys) == 62
    assert (keys[0], keys[-1]) == (20101201, 20110131)

    new_year = dim.filter("date_key = 20110101").first()
    assert (new_year.day_name, new_year.day_of_week, new_year.is_weekend) == ("Saturday", 6, True)


def test_fact_sales_maps_keys_and_amounts(spark):
    orders = spark.createDataFrame(
        [
            ("536365", "85123A", 6, "2010-12-01 08:26", 12346, "sale"),
            ("536366", "85123A", 2, "2010-12-01 08:28", None, "sale"),
            ("C536379", "85123A", -1, "2010-12-01 09:41", 12346, "cancellation"),
            ("536380", "99999", 3, "2010-12-01 09:45", 12346, "sale"),
        ],
        "invoice_no string, stock_code string, quantity int, ts string, customer_id int, line_type string",
    ).select("*", F.to_timestamp("ts").alias("invoice_ts")).drop("ts")
    dim_customer = spark.createDataFrame(
        [(1, 12346), (-1, None)], "customer_key int, customer_id int"
    )
    dim_product = spark.createDataFrame(
        [(1, "85123A", Decimal("2.55")), (-1, "Unknown", None)],
        "product_key int, stock_code string, unit_price decimal(10,2)",
    )

    fact = gold.build_fact_sales(orders, dim_customer, dim_product)
    result = fact.select(
        "sales_key", "invoice_no", "date_key", "customer_key", "product_key", "line_amount"
    ).orderBy("sales_key").collect()

    assert [tuple(row) for row in result] == [
        (1, "536365", 20101201, 1, 1, Decimal("15.30")),
        (2, "536366", 20101201, -1, 1, Decimal("5.10")),
        (3, "536380", 20101201, 1, -1, None),
        (4, "C536379", 20101201, 1, 1, Decimal("-2.55")),
    ]