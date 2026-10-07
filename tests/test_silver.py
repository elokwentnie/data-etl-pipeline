from decimal import Decimal

import pytest
from pyspark.sql import functions as F

from ETL import silver


def test_clean_customers_drops_missing_ids(spark):
    df = spark.createDataFrame(
        [("12346", "United Kingdom"), (None, "France"), ("12347", "EIRE")],
        "CustomerID string, Country string",
    )
    result = silver.clean_customers(df).collect()
    assert [tuple(row) for row in result] == [(12346, "United Kingdom"), (12347, "Ireland")]


def test_clean_products_types_and_normalizes(spark):
    df = spark.createDataFrame(
        [(" 85123a ", "WHITE HANGING HEART", "2.55"), ("85123A", "damaged", "0")],
        "StockCode string, Description string, UnitPrice string",
    )
    result = silver.clean_products(df).collect()
    assert [tuple(row) for row in result] == [
        ("85123A", "WHITE HANGING HEART", Decimal("2.55")),
        ("85123A", None, Decimal("0.00")),
    ]


def test_clean_orders_dedups_and_fixes_timestamp(spark):
    df = spark.createDataFrame(
        [
            ("540239", "22423", "2", "1/5/2011 14:48", "12345"),
            ("540239", "22423", "2", "1/5/2011 14:48", "12345"),
            ("540239", "85123a", "1", "15/5/2011 14:48", "12345"),
            ("C540240", "22423", "-1", "1/5/2011 15:00", None),
        ],
        "InvoiceNo string, StockCode string, Quantity string, InvoiceDate string, CustomerID string",
    )
    result = (
        silver.clean_orders(df)
        .select(
            "invoice_no", "stock_code", "quantity", "customer_id", "line_type",
            F.date_format("invoice_ts", "yyyy-MM-dd HH:mm").alias("ts"),
        )
        .orderBy("invoice_no", "stock_code")
        .collect()
    )
    assert [tuple(row) for row in result] == [
        ("540239", "22423", 2, 12345, "sale", "2011-01-05 14:48"),
        ("540239", "85123A", 1, 12345, "sale", "2011-01-05 14:48"),
        ("C540240", "22423", -1, None, "cancellation", "2011-01-05 15:00"),
    ]


def test_check_not_null_raises_on_nulls(spark):
    df = spark.createDataFrame([("a", None), ("b", 1)], "invoice_no string, quantity int")
    with pytest.raises(ValueError, match="quantity"):
        silver.check_not_null(df, "orders", ["invoice_no", "quantity"])