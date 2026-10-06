# Data exploration

Exploration of the three raw CSV files in `data/raw` before building the ETL pipeline. Figures come from `analytics.ipynb` (Spark, schema inferred from the files).

The files are line-level retail data: one customer dimension, one product dimension, and one order-line fact. They cover **1 December 2010 08:26** through **9 December 2011 12:50**.

## Sources

| File | Rows | Columns |
| --- | ---: | --- |
| `customers.csv` | 4,389 | `CustomerID`, `Country` |
| `products.csv` | 5,635 | `StockCode`, `Description`, `UnitPrice` |
| `orders.csv` | 541,909 | `InvoiceNo`, `StockCode`, `Quantity`, `InvoiceDate`, `CustomerID` |

Inferred types:

| Column | Type | Notes |
| --- | --- | --- |
| `CustomerID` | integer | Nullable in both `customers` and `orders` |
| `Country` | string | No nulls or blanks |
| `StockCode` | string | Mixed case; not a stable key as stored |
| `Description` | string | Often null, and sometimes an operations note rather than a product name |
| `UnitPrice` | double | Always numeric; several prices per stock code |
| `InvoiceNo` | string | Prefix encodes invoice type |
| `Quantity` | integer | Negative values are returns, cancellations, or adjustments |
| `InvoiceDate` | string | Almost all values match `M/d/yyyy H:mm` |

No column contains blank strings. Problems are nulls, duplicate keys, inconsistent codes, and a few bad dates.

## Customers

`CustomerID` is almost a primary key, but not quite.

| Check | Result |
| --- | --- |
| Null `CustomerID` | 9 rows (0.21%) |
| Distinct `CustomerID` (null counted once) | 4,373 |
| `CustomerID` values appearing more than once | 9 |
| Exact duplicate rows | 0 |
| Distinct countries | 38 |

The 9 repeated keys are the null id (9 rows, one per country) plus **8 real ids that each map to two countries**:

| CustomerID | Countries |
| ---: | --- |
| 12370 | Austria, Cyprus |
| 12394 | Belgium, Denmark |
| 12417 | Spain, Belgium |
| 12422 | Switzerland, Australia |
| 12429 | Austria, Denmark |
| 12431 | Belgium, Australia |
| 12455 | Spain, Cyprus |
| 12457 | Switzerland, Cyprus |

Null-id rows are not usable as customers. Their countries are United Kingdom, Unspecified, Hong Kong, EIRE, Israel, Portugal, France, Switzerland, and Bahrain.

Country values need cleaning before they can be used as a dimension:

- **United Kingdom dominates**: 3,951 of 4,389 rows. Germany (95) and France (88) are next.
- Short names that are real places: `EIRE` (Ireland), `RSA` (South Africa), `USA`.
- `European Community` is not a country (1 row).
- `Unspecified` is a placeholder (5 rows).
- `Channel Islands` is a territory, not a country (9 rows).

Every non-null customer id appears on at least one order line. The 9 customer rows with no matching order are exactly the null-id rows.

## Products

`StockCode` is not unique, and `UnitPrice` is not a single attribute of a product.

| Check | Result |
| --- | --- |
| Null `Description` | 960 rows (17.04%) |
| Null `StockCode` or `UnitPrice` | 0 |
| Distinct `StockCode` | 3,965 |
| Stock codes appearing more than once | 1,315 |
| Exact duplicate rows | 0 |
| Distinct prices | 501 |

How many rows each stock code has:

| Rows per code | Codes |
| ---: | ---: |
| 1 | 2,650 |
| 2 | 1,061 |
| 3 | 185 |
| 4 | 49 |
| 5 | 14 |
| 6 | 2 |
| 7 | 2 |
| 8 | 2 |

Many of those repeats are different prices, not repeated descriptions. The busiest codes (`20713`, `23084`) have 8 rows and 8 different prices. Across the catalog, price runs from **0.00 to 5.00** (mean 2.52, median 2.55). Every `UnitPrice` casts to a number. **10 rows have price 0.**

Descriptions written in lowercase look like warehouse notes, not product names. The most common are `check` (146), `damaged` (57), `damages` (44), `found` (32), `sold as set on dotcom` (20), `adjustment` (17), `amazon` (12), `unsaleable, destroyed.` (9), and `thrown away` (9).

Most stock codes look like five digits plus an optional letter suffix (`85123A`). **41 rows do not.** They are fees, postage, discounts, samples, manual adjustments, and gift vouchers, for example `POST`, `DOT`, `C2`, `D`, `M`, `S`, `AMAZONFEE`, `BANK CHARGES`, `CRUK`, `PADS`, `DCGS*`, and `gift_0001_*`.

Stock codes also differ only by case and surrounding space. Treating the raw code as a join key leaves order lines unmatched; `upper(trim(StockCode))` does not.

## Orders

Grain is one invoice line. There are **25,900** distinct invoices and **4,070** distinct raw stock codes (more than the 3,965 product codes, because of case variants).

| Check | Result |
| --- | --- |
| Null `CustomerID` | 135,080 rows (24.93%) |
| Nulls in other columns | 0 |
| Distinct `CustomerID` | 4,373 |
| (`InvoiceNo`, `StockCode`) pairs appearing more than once | 9,694 |
| Exact duplicate rows | 5,429 |

### Invoice type

`InvoiceNo` prefix splits the lines into three groups:

| Prefix | Lines | Meaning in this file |
| --- | ---: | --- |
| (none) | 532,618 | Ordinary invoice |
| `C` | 9,288 | Most likely cancellation? Quantity is always negative. |
| `A` | 3 | Rare third type. Not investigated further. |

### Quantity

Quantity is an integer on every row. Distribution is heavily skewed: min **-80,995**, 25th percentile 1, median 3, 75th percentile 10, mean 9.55, max **80,995**.

The extremes are a matched pair, not two unrelated outliers:

| InvoiceNo | StockCode | Quantity | InvoiceDate | CustomerID |
| --- | --- | ---: | --- | ---: |
| 581483 | 23843 | 80,995 | 12/9/2011 9:15 | 16446 |
| C581484 | 23843 | -80,995 | 12/9/2011 9:27 | 16446 |

The same pattern shows up elsewhere. Customer 12346 has 74,215 and -74,215 on stock `23166` (invoices `541431` and `C541433`, 16 minutes apart). Customer 14533 has 1,200 on `569214` (2 Oct 2011) and -1,200 on `C569552` (4 Oct 2011), both stock `15034`.

Cancellation prefix and sign of quantity:

| Starts with `C` | Quantity &lt; 0 | Lines |
| --- | --- | ---: |
| no | no | 531,285 |
| no | yes | 1,336 |
| yes | yes | 9,288 |

Every `C` invoice has a negative quantity. The 1,336 negative lines on ordinary invoices are a separate case (adjustments or returns that were not coded as cancellations). Several of the largest of those have a null customer, for example -9,600 and -9,058 on 14 June 2011.

### Dates

`InvoiceDate` parses with format `M/d/yyyy H:mm` except **2 lines**:

| InvoiceNo | InvoiceDate | Why it fails |
| --- | --- | --- |
| 540239 | `15/5/2011 14:48` | Day is written first (`15/5` is not a US month/day) |
| 540798 | `1/11/2011 12:90` | Minute 90 is not a valid time |

Parsed timestamps run from 2010-12-01 08:26 to 2011-12-09 12:50. Volume rises through autumn 2011 and drops in December because the extract stops on 9 December, not because December is a short trading month.

| Month | Lines | Invoices |
| --- | ---: | ---: |
| 2010-12 | 42,481 | 2,025 |
| 2011-01 | 35,145 | 1,476 |
| 2011-02 | 27,707 | 1,393 |
| 2011-03 | 36,748 | 1,983 |
| 2011-04 | 29,916 | 1,744 |
| 2011-05 | 37,030 | 2,162 |
| 2011-06 | 36,874 | 2,012 |
| 2011-07 | 39,518 | 1,927 |
| 2011-08 | 35,284 | 1,737 |
| 2011-09 | 50,226 | 2,327 |
| 2011-10 | 60,742 | 2,637 |
| 2011-11 | 84,711 | 3,462 |
| 2011-12 | 25,525 | 1,015 |

There are **no Saturday orders**. Sunday is a normal trading day (64,375 lines). Hours run from **6 through 20** only, with the bulk between 10 and 16.

## How the tables join

| Check | Result |
| --- | --- |
| Order lines whose non-null `CustomerID` is missing from `customers` | 0 |
| Customer rows with no order | 9 (all null `CustomerID`) |
| Order lines whose raw `StockCode` is missing from `products` | 2,041 |
| Same check after `upper(trim(StockCode))` | 0 |
| Normalized product codes with no order line | 0 |

The 2,041 raw orphans are case variants of real codes (`72349B` vs `72349b`, `47566b`, `15056n`, `85123a`, and similar). After the code is uppercased and trimmed, every order line matches a product and every product matches an order.

## What this means for revenue

A line amount is `Quantity * UnitPrice`, but price is not unique per product. Taking the min and max price per normalized stock code, and keeping only ordinary invoices (not starting with `C` or `A`) with quantity &gt; 0:

| Metric | Value |
| --- | ---: |
| Revenue if every line uses the minimum price | 12,519,705.45 |
| Revenue if every line uses the maximum price | 16,060,015.59 |
| Share of those lines whose min and max price differ | 31.0% |

The gap is about 3.5 million. Any sales fact needs an explicit price rule (which row wins per stock code, or whether price must be carried on the order line) before the number is trustworthy.

## Cleaning the pipeline has to decide

1. **Customer id.** Drop or quarantine the 9 null-id customer rows. For the 8 ids with two countries, pick one country or keep both and stop treating `CustomerID` as a unique key.
2. **Country.** Map `EIRE`, `RSA`, and `USA` to full names if the model uses standard country names. Decide what to do with `Unspecified`, `European Community`, and `Channel Islands`.
3. **Stock code.** Normalize with `upper(trim(...))` before any join. Split non-product codes (postage, fees, discounts, vouchers, samples, manual entries) out of the product dimension.
4. **Description.** Do not treat nulls or lowercase operational notes (`damaged`, `check`, `thrown away`, …) as the product name.
5. **Unit price.** Resolve multiple prices per code, including the 10 zero prices. Until that rule exists, revenue can swing from 12.5 million to 16.1 million on the same lines.
6. **Invoice lines.** Keep cancellations (`C…`, always negative quantity) separate from sales. Also separate the 1,336 negative quantities on invoices that are not cancellations. The 5,429 exact duplicate rows need a dedup rule.
7. **Invoice date.** Parse `M/d/yyyy H:mm`. Handle the two bad values explicitly: `15/5/2011 14:48` is day-first, and `1/11/2011 12:90` is not a valid time. December 2011 is a partial month.
