# data-etl-pipeline

Retail line data (customers, products, orders), 1 Dec 2010 – 9 Dec 2011. Exploration is done. The ETL pipeline is not built yet.

## Layout

```
data/raw/          customers.csv, products.csv, orders.csv
notebooks/         analytics.ipynb — Spark exploration of the raw files
docs/              data_exploration.md — what the notebook found
src/ETL/           bronze.py, silver.py, gold.py, config.py, main.py
tests/             conftest.py, test_silver.py, test_gold.py
requirements.txt   pyspark, pytest
```

`src/ETL` and `tests` are empty. That is the pipeline.

## Analytics

Three files. One row per customer, product price, or invoice line. Counts and the join checks are in `docs/data_exploration.md`.

- **customers** (`CustomerID`, `Country`). `CustomerID` is almost unique: 9 null ids, 8 ids with two countries. UK is most of the file. `EIRE`, `RSA`, `USA` are short names. `Unspecified`, `European Community`, and `Channel Islands` need a decision.
- **products** (`StockCode`, `Description`, `UnitPrice`). `StockCode` is not unique, and the same code can have several prices (0.00–5.00). Some descriptions are warehouse notes (`damaged`, `check`), not product names. Some codes are fees (`POST`, `DOT`, `M`, gift vouchers), not products.
- **orders** (`InvoiceNo`, `StockCode`, `Quantity`, `InvoiceDate`, `CustomerID`). Grain is an invoice line. About 25% of lines have no customer. `C` invoices are cancellations and always have a negative quantity. Another ~1,300 negative quantities sit on normal invoices. Two dates do not parse as `M/d/yyyy H:mm`. No Saturday orders. Raw `StockCode` does not join to products; `upper(trim(StockCode))` does.

A line amount is `Quantity * UnitPrice`, but price is not one value per product. On ordinary invoices with quantity > 0, revenue is about 12.5M at the min price and 16.1M at the max.

## ETL plan

PySpark, three layers, same modules as `src/ETL`.

1. **Bronze** — load the three CSVs with the header and keep the raw values.
2. **Silver** — clean, from the exploration:
   - drop or quarantine null customer ids, and decide the 8 dual-country ids
   - normalize country names
   - `upper(trim(StockCode))` before joins, and split fee / postage / voucher codes out of products
   - do not use null or lowercase operational descriptions as the product name
   - pick one price per stock code (this is what makes revenue one number)
   - keep cancellations (`C…`) separate from the other negative quantities, and dedupe exact duplicate order rows
   - parse `InvoiceDate`, and handle the two bad values
3. **Gold** — customer and product dimensions, and a sales fact that uses the chosen price. `main.py` runs bronze → silver → gold.

`tests/` covers the silver cleaning and the gold fact.
