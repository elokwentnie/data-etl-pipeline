from pyspark.sql import functions as F

from ETL import cleaning


def apply_to(spark, helper, values):
    """Run a column helper over a list of strings and return the results."""
    df = spark.createDataFrame([(v,) for v in values], "value string")
    return [row[0] for row in df.select(helper(F.col("value"))).collect()]


def test_normalize_stock_code(spark):
    result = apply_to(spark, cleaning.normalize_stock_code, [" 85123a ", "85123A", "post"])
    assert result == ["85123A", "85123A", "POST"]


def test_clean_country(spark):
    raw = ["EIRE", "RSA", "USA", "Unspecified", "European Community", "Channel Islands", " France "]
    result = apply_to(spark, cleaning.clean_country, raw)
    assert result == [
        "Ireland", "South Africa", "United States", "Unknown", "Unknown", "Channel Islands", "France",
    ]


def test_clean_description_keeps_names_and_drops_notes(spark):
    raw = [" WHITE HANGING HEART ", "Bank Charges", "damaged", "ebay", None, "  "]
    result = apply_to(spark, cleaning.clean_description, raw)
    assert result == ["WHITE HANGING HEART", "Bank Charges", None, None, None, None]


def test_parse_invoice_ts_returns_null_for_bad_values(spark):
    def parse_as_text(col):
        return F.date_format(cleaning.parse_invoice_ts(col), "yyyy-MM-dd HH:mm")

    result = apply_to(spark, parse_as_text, ["12/1/2010 8:45", "15/5/2011 14:48", "1/11/2011 12:90"])
    assert result == ["2010-12-01 08:45", None, None]


def test_fill_invoice_ts_uses_same_invoice(spark):
    df = spark.createDataFrame(
        [("540239", "1/5/2011 14:48"), ("540239", "15/5/2011 14:48"), ("999999", "bad")],
        "invoice_no string, raw string",
    ).withColumn("invoice_ts", cleaning.parse_invoice_ts(F.col("raw")))

    result = (
        cleaning.fill_invoice_ts(df)
        .select("raw", F.date_format("invoice_ts", "yyyy-MM-dd HH:mm").alias("ts"))
        .collect()
    )
    assert {row.raw: row.ts for row in result} == {
        "1/5/2011 14:48": "2011-01-05 14:48",
        "15/5/2011 14:48": "2011-01-05 14:48",
        "bad": None,
    }


def test_line_type(spark):
    df = spark.createDataFrame(
        [("536365", 6), ("C536379", -1), ("536380", -5), ("A563185", 1)],
        "invoice_no string, quantity int",
    )
    result = df.select(cleaning.line_type(F.col("invoice_no"), F.col("quantity"))).collect()
    assert [row[0] for row in result] == ["sale", "cancellation", "other", "other"]