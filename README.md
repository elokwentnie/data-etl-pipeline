# data-etl-pipeline

PySpark ETL for retail order data (customers, products, orders) from 1 Dec 2010 to 9 Dec 2011. Raw CSV files go through bronze, silver and gold layers and end up in a star schema, which is then used to answer the analytics questions.

## Layout

```
.github/workflows/
  pipeline.yml            CI/CD: tests, then the pipeline on main
analytics_questions/
  questions.ipynb         analytics questions with answers and charts
data/
  raw/                    source CSV files (customers, products, orders)
  bronze/ silver/ gold/   pipeline output, Parquet (created on run, not in git)
docs/
  data_exploration.md     findings from the exploration
  data_model.md           star schema, diagram, grain and design decisions
notebooks/
  analytics.ipynb         initial data exploration (Spark)
src/ETL/
  config.py               paths, Spark settings, cleaning rules
  spark.py                shared Spark session
  bronze.py               raw CSV to bronze
  cleaning.py             cleaning helpers used by silver
  silver.py               bronze to silver
  gold.py                 silver to gold (dimensions and fact)
  main.py                 runs bronze, silver and gold
tests/
  conftest.py             shared Spark fixture
  test_cleaning.py        cleaning helpers
  test_silver.py          silver tables
  test_gold.py            dimensions and fact
pytest.ini                lets pytest find src/
requirements.txt          pyspark, pytest, matplotlib
```

## How to run

You need Python 3.14 and Java 17 (Spark 4.2 does not work with Java 8).

```bash
python -m venv env
source env/bin/activate
pip install -r requirements.txt
```

If `JAVA_HOME` is not set, `spark.py` falls back to the Homebrew path `/opt/homebrew/opt/openjdk@17`. On other machines set `JAVA_HOME` to a Java 17 install.

Run the whole pipeline:

```bash
PYTHONPATH=src python -m ETL.main
```

Output is written as Parquet to `data/bronze`, `data/silver` and `data/gold`. Each layer can also be run on its own, e.g. `PYTHONPATH=src python -m ETL.silver`.

Run the tests:

```bash
pytest
```

For the analytics, run the pipeline first and then open `analytics_questions/questions.ipynb`.

## Data analysis

The exploration is in `notebooks/analytics.ipynb` and the findings are written up in `docs/data_exploration.md`. Main things I found:

- `CustomerID` is almost unique: 9 rows have no id and 8 ids have two countries.
- Country names need cleaning: `EIRE`, `RSA`, `USA`, plus `Unspecified` and `European Community`.
- `StockCode` only joins between orders and products after `upper(trim())`.
- Products have several prices per stock code (0 to 5) and no dates, so there is no single price and no price history.
- Some descriptions are warehouse notes (`damaged`, `check`, `ebay`) and not product names.
- About 25% of order lines have no customer. It is always the whole invoice, never single lines.
- `C` invoices are cancellations. There are also 1,336 negative quantities on normal invoices.
- 5,429 exact duplicate order rows.
- Two invoice dates do not parse. Both are typos, the other lines on the same invoice have the right timestamp.

## Assumptions

- One price per product: the median of the non-zero listed prices. Revenue depends a lot on this choice (12.5M with min price, 16.1M with max).
- Zero prices are not real prices. Three products only have a zero price, so their price and line amount are null.
- Order lines without a customer are kept and point to an "Unknown" customer (`customer_key = -1`). The reason the customer is missing is not in the data.
- Customers with two countries get country `Unknown`, because orders do not say which one is right.
- Exact duplicate order rows are removed. Same invoice and stock code with a different quantity are kept as separate lines.
- Lines are split into `sale`, `cancellation` and `other`.
- Broken timestamps take the timestamp from the rest of the invoice.

## Data model

Star schema with one fact and three dimensions: `fact_sales`, `dim_customer`, `dim_product`, `dim_date`. The mermaid diagram, the grain of each table and the design decisions are in `docs/data_model.md`.

## ETL

- **Bronze**: reads the CSVs as strings, checks required columns and adds `_ingested_at` and `_source_file`.
- **Silver**: same grain as the source. Types, normalized stock codes, cleaned countries and descriptions, deduplicated orders, fixed timestamps and `line_type`.
- **Gold**: builds the dimensions (one row per customer, product and day, plus the `-1` unknown rows) and `fact_sales` with surrogate keys and the line amount.

## Data quality rules

Implemented:

- Bronze fails if a source file is missing a required column.
- Silver fails if a required column has nulls after cleaning (e.g. `invoice_ts`, `stock_code`).
- Casts run with ANSI mode on, so a non-numeric quantity or id fails the run instead of becoming null.

Proposed, not implemented:

- Surrogate keys are unique in every dimension.
- Every key in `fact_sales` exists in its dimension.
- `fact_sales` has the same number of rows as silver orders.
- Quantity is never 0, and `cancellation` lines always have a negative quantity.
- Prices are between 0 and an agreed maximum.
- `invoice_ts` is inside the expected date range.
- Alert if the share of lines without a customer moves a lot from the usual ~25%.

## Analytics

All four questions are answered in `analytics_questions/questions.ipynb`, with Spark code, tables and charts.

1. **Customers by country**: United Kingdom 3,950, then Germany 95 and France 87. About 90% of customers are in the UK.
2. **Revenue by country**: net revenue is 12.92M. UK brings 77.5%, international 17.3%, and 5.2% can't be placed. The Netherlands, Ireland, Germany, France and Australia are the biggest markets abroad.
3. **Price vs sales volume**: no relationship (correlation about 0). Every price band sells about the same. Most likely because the prices in the source are not tied to the sales.
4. **Price drop in the last month**: can't be answered with this data. Prices have no dates, so there is only one price per product. 

## CI/CD

`.github/workflows/pipeline.yml` runs the tests on every pull request and push. On `main`, after the tests pass, it runs the full pipeline and uploads the gold tables as an artifact.

## Not done yet

Some smaller things are not fixed yet:

- No linting or formatting check. The code has a few style issues (quote style in docstrings, blank lines between functions).
- A few small mismatches between `docs/data_model.md` and the code (e.g. `sales_key` is `int`, not `bigint`).
- Surrogate keys use `row_number()` over the whole table, so Spark logs a single partition warning. Fine for this size, for big data I would switch to a hash of the natural key.
- No quarantine table for rows that are dropped (null customer ids in the customers file).
- The proposed data quality rules above are not implemented.

## Use of AI

I used Claude to speed up writing the unit tests and to prepare the READMEs and docs from my own notes and findings from the initial analysis. I reviewed and validated everything myself. I use it also to fix small issues when I was writing a code to not spend too much time on that. 
