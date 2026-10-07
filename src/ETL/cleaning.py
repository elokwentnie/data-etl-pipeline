from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from ETL import config


def normalize_stock_code(code: Column) -> Column:
    "Uppercase and trip, so case varants will join to the same product"
    return F.upper(F.trim(code))


def clean_country(country: Column) -> Column:
    "Map short and placeholder names to the full country name"
    name = F.trim(country)
    mapping = F.map_from_arrays(
        F.lit(list(config.COUNTRY_MAP.keys())),
        F.lit(list(config.COUNTRY_MAP.values())),
    )
    return F.coalesce(F.try_element_at(mapping, name), name)


def clean_description(description: Column) -> Column:
    "Trim description, or null when it's a warehouse note and not a name"
    text = F.trim(description)
    return F.when(text.rlike("[A-Z]"), text)


def parse_invoice_ts(value: Column) -> Column:
    "Parse timestamp. Bad values should become null instead of failing the run."
    return F.try_to_timestamp(value, F.lit(config.INVOICE_TS_FORMAT))

# based on data exploration, this is just for 2 cases, but if more data is ahead, we should be prepared for it
def fill_invoice_ts(df: DataFrame) -> DataFrame:
    "Fill a nullk invoice_ts from theoter lines of the same invoice."
    invoice = Window.partitionBy("invoice_no")
    return df.withColumn(
        "invoice_ts", F.coalesce("invoice_ts", F.min("invoice_ts").over(invoice))
    )


def line_type(invoice_no: Column, quantity: Column) -> Column:
    "Sale, cancellation or other, bassed on the invoice prefix and its quantity"
    return (
        F.when(invoice_no.startswith("C"), "cancellation")
        .when(invoice_no.rlike("^[0-9]") & (quantity > 0), "sale")
        .otherwise("other")
    )
