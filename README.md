# Nifty 50 Stock ETL Pipeline

An end-to-end data engineering pipeline built on **Azure Databricks** that ingests, transforms, and analyzes 1 year of stock market data for 25 Nifty 50 companies using PySpark and Delta Lake.

---

## Architecture

```
yfinance API
     ↓
[EXTRACT] Pull 1 year OHLCV data for 25 Nifty stocks
     ↓
[VALIDATE] Manual StructType schema enforcement + null checks
     ↓
[TRANSFORM] Daily returns | 7-day moving average | Sector mapping | GroupBy aggregations
     ↓
[LOAD] Parquet (partitionBy Ticker) → ADLS Gen2
       Delta Table → Unity Catalog (with Time Travel)
     ↓
[ANALYZE] SparkSQL — Top gainers | Best sector | Highest volume stocks
```

---

## Tech Stack

| Tool | Purpose |
|------|---------|
| Azure Databricks | Compute + notebook environment |
| Apache Spark / PySpark | Distributed data processing |
| Delta Lake | ACID transactions + Time Travel |
| ADLS Gen2 | Cloud storage (partitioned Parquet) |
| yfinance | Live market data ingestion |
| Python / Pandas | Data extraction + preprocessing |
| Unity Catalog | Storage credentials + external location |
| GitHub | Version control |

---

## Pipeline Steps

### 1. Extract
- Pulls 1 year of daily OHLCV (Open, High, Low, Close, Volume) data for 25 Nifty 50 stocks using the `yfinance` API
- Downloads each ticker individually to avoid multi-level column issues
- Converts Pandas DataFrame to PySpark DataFrame
- Result: **6,250 rows × 7 columns**

### 2. Validate
- Enforces manual `StructType` schema — no inferSchema
- Ensures correct data types: `DateType`, `DoubleType`, `LongType`, `StringType`
- Null check across all columns — **0 nulls found**

### 3. Transform
- **Daily Return %** — `((Close - Open) / Open) * 100`
- **7-Day Moving Average** — rolling avg of Close price using Window function (`rowsBetween(-6, 0)`)
- **Sector Mapping** — hardcoded ticker → sector dictionary, joined as lookup table
- **Sector Aggregation** — average daily return per sector using `groupBy`

### 4. Load
- Writes full transformed dataset to **ADLS Gen2** in Parquet format, partitioned by `Ticker`
- Writes to **Delta table** (`nifty50_delta`) in Unity Catalog for ACID compliance and versioning

### 5. Analyze (SparkSQL)
- Creates temp view on transformed DataFrame
- **Top 10 stocks** by average daily return
- **Best performing sector** by average daily return
- **Top 5 stocks** by average trading volume

### 6. Time Travel
- Queries Delta table version history using `DESCRIBE HISTORY`
- Demonstrates querying a previous version: `VERSION AS OF 0`

---

## Stocks Covered (25 Nifty 50 Companies)

| Sector | Tickers |
|--------|---------|
| IT | TCS, Infosys, HCL Tech, Wipro |
| Banking | HDFC Bank, ICICI Bank, SBI, Axis Bank, Bajaj Finance |
| Energy | Reliance, ONGC, NTPC, Power Grid |
| FMCG | HUL, ITC, Nestle |
| Auto | Maruti, Mahindra, Bajaj Auto |
| Metals | Tata Steel, Hindalco |
| Pharma | Sun Pharma, Cipla |
| Infrastructure | L&T |
| Telecom | Bharti Airtel |

---

## Key PySpark Concepts Demonstrated

- Manual schema definition with `StructType` / `StructField`
- Window functions — `rowsBetween`, `partitionBy`, `orderBy`
- Broadcast join for lookup table optimization
- `partitionBy` on write for query optimization
- Delta Lake — ACID writes, Time Travel, version history
- SparkSQL on temp views
- ADLS Gen2 integration via Unity Catalog External Location

---

## Setup

### Prerequisites
- Azure Databricks workspace (Unity Catalog enabled)
- ADLS Gen2 storage account with container `nifty50`
- Storage Credential + External Location configured in Unity Catalog
- Cluster with `yfinance` installed (`%pip install yfinance`)

### Run
1. Open `nifty50_etl.ipynb` in Azure Databricks
2. Attach to cluster
3. Run All cells top to bottom

---

## Project Structure

```
pyspark-etl-project/
├── nifty50_etl.ipynb    # Main pipeline notebook
└── README.md
```

---

## Author

**Varanasi V G Alekhya**
Senior Systems Engineer → Data Engineer
[LinkedIn](https://www.linkedin.com/in/varanasi-v-g-alekhya/) | [GitHub](https://github.com/Alekhya-Varanasi)
