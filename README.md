# 🛒 Sales ETL Data Pipeline

A production-quality **Extract → Transform → Load** pipeline built in Python that processes raw e-commerce sales data, cleans and enriches it, loads it into PostgreSQL, and enables analytical SQL reporting.

---

## 📐 Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                        ETL PIPELINE                              │
│                                                                  │
│  ┌─────────────┐    ┌──────────────────┐    ┌────────────────┐  │
│  │   EXTRACT   │───▶│    TRANSFORM     │───▶│     LOAD       │  │
│  │             │    │                  │    │                │  │
│  │ • Read CSV  │    │ • Drop dupes     │    │ • Create       │  │
│  │ • Validate  │    │ • Clean text     │    │   schema       │  │
│  │   schema    │    │ • Parse dates    │    │ • Batch insert │  │
│  │ • Log stats │    │ • Fix numerics   │    │   (5k rows)    │  │
│  └─────────────┘    │ • Handle nulls   │    │ • Retry logic  │  │
│         │           │ • Validate rules │    └────────────────┘  │
│         ▼           │ • Add derived    │           │            │
│  data/raw/          │   columns        │           ▼            │
│  sales_data.csv     └──────────────────┘    PostgreSQL          │
│                              │              sales_fact           │
│                              ▼                                   │
│                     data/processed/                              │
│                     sales_data_clean.csv                         │
└──────────────────────────────────────────────────────────────────┘

Project layout
──────────────
etl-data-pipeline/
├── data/
│   ├── raw/                   ← source CSV files
│   └── processed/             ← cleaned CSV (post-transform)
├── src/
│   ├── extract.py             ← extraction layer
│   ├── transform.py           ← transformation layer
│   ├── load.py                ← loading layer (PostgreSQL)
│   └── pipeline.py            ← orchestrator (entry point)
├── sql/
│   ├── schema.sql             ← DDL: tables + indexes
│   └── queries.sql            ← analytical queries
├── config/
│   └── config.py              ← DB credentials & settings
├── logs/                      ← auto-created pipeline log files
├── generate_data.py           ← synthetic dataset generator
├── requirements.txt
├── docker-compose.yml
└── README.md
```

---

## 🛠 Tech Stack

| Layer        | Technology                |
|--------------|---------------------------|
| Language     | Python 3.10+              |
| Data wrangling | pandas, NumPy           |
| Database     | PostgreSQL 15             |
| ORM / Driver | SQLAlchemy 2 + psycopg2   |
| Container    | Docker + Docker Compose   |
| Logging      | Python `logging` module   |
| Dev tools    | black, flake8, pytest     |

---

## 📦 Dataset

The pipeline ships with a **synthetic e-commerce sales dataset** (`generate_data.py`) that simulates real-world data quality problems:

| Property                  | Detail                                      |
|---------------------------|---------------------------------------------|
| Rows                      | ~15,150 (including intentional duplicates)  |
| Columns                   | 13 raw → 20 clean                           |
| Date range                | Jan 2022 – Jun 2024                         |
| Categories                | 8 product categories, 40 sub-categories     |
| Dirty data injected       | ~4% nulls per column, ~1% duplicate rows, negative prices, mixed date formats, inconsistent casing |

---

## ⚙️ Setup Instructions

### Prerequisites

- Python 3.10+
- Docker Desktop (for PostgreSQL)
- Git

### 1 — Clone & install dependencies

```bash
git clone https://github.com/your-username/etl-data-pipeline.git
cd etl-data-pipeline

python -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate

pip install -r requirements.txt
```

### 2 — Start PostgreSQL via Docker

```bash
docker compose up -d

# Verify the database is healthy
docker compose ps
```

The database will be available at `localhost:5432` with:
- **Database:** `sales_db`
- **User:** `etl_user`
- **Password:** `etl_password`

To use different credentials, set environment variables before running:

```bash
export DB_HOST=localhost
export DB_PORT=5432
export DB_NAME=sales_db
export DB_USER=etl_user
export DB_PASSWORD=etl_password
```

### 3 — Generate the dataset

```bash
python generate_data.py
# → data/raw/sales_data.csv  (15,150 rows)
```

---

## ▶️ How to Run the Pipeline

### Full pipeline (Extract + Transform + Load)

```bash
python src/pipeline.py
```

### Dry run — skip the database load

Useful for testing the extract/transform logic without PostgreSQL running:

```bash
python src/pipeline.py --skip-db
```

The cleaned dataset is always written to `data/processed/sales_data_clean.csv` regardless of the `--skip-db` flag.

### Pipeline log

Every run appends a timestamped log file to `logs/`:

```
logs/pipeline_20240615_143022.log
```

---

## 📋 What the Pipeline Does

### Extract
- Reads `data/raw/sales_data.csv` with full schema validation
- Logs row count, file size, and per-column null summary
- Raises descriptive errors for missing files or corrupt CSVs

### Transform (15,150 rows in → 15,000 rows out)

| Step | Action |
|------|--------|
| Deduplication | Removes 150 exact duplicate rows |
| Text cleaning | Strips whitespace, applies Title Case to all categorical columns |
| Date parsing | Handles 3 mixed formats (`YYYY-MM-DD`, `MM/DD/YYYY`, `DD-Mon-YYYY`) |
| Numeric casting | Enforces `float64` / nullable `Int64` dtypes |
| Null imputation | Median fill for prices/quantities; `'Unknown'` for categoricals; drop rows with missing primary keys |
| Business rules | Corrects negative prices (abs), floors quantity at 1, clamps discounts to [0, 100] |
| Derived columns | `discount_amount`, `line_total`, `order_year/month/quarter`, `order_year_month`, `is_returned` |

### Load
- Creates schema idempotently from `sql/schema.sql`
- Inserts data in batches of 5,000 rows (`CHUNK_SIZE` in config)
- Retries database connection up to 3 times on failure
- Supports `replace` (default) or `append` mode

---

## 🗄️ Database Schema

```sql
sales_fact          ← primary fact table (one row per order line)
dim_customers       ← customer dimension
dim_products        ← product / category dimension
monthly_revenue_summary  ← pre-aggregated summary table
```

Key indexes on `sales_fact`:
- `order_date`, `customer_id`, `category`, `order_year_month`, `region`, `shipping_status`

---

## 📊 Sample Queries & Outputs

Connect to the database:

```bash
docker exec -it sales_etl_db psql -U etl_user -d sales_db
```

Or run the full query file:

```bash
docker exec -i sales_etl_db psql -U etl_user -d sales_db < sql/queries.sql
```

### Revenue by Category

```sql
SELECT category,
       COUNT(DISTINCT order_id)   AS total_orders,
       ROUND(SUM(line_total), 2)  AS total_revenue
FROM sales_fact
GROUP BY category
ORDER BY total_revenue DESC;
```

```
category         total_orders  total_revenue
───────────────  ────────────  ─────────────
Clothing                1,917  6,878,385.57
Books                   1,888  6,678,230.01
Beauty                  1,896  6,626,909.64
Food & Beverage         1,896  6,622,387.79
Electronics             1,899  6,605,627.17
Home & Garden           1,805  6,569,770.01
Toys                    1,874  6,536,693.73
Sports                  1,825  6,520,051.75
```

### Monthly Revenue Trend

```sql
SELECT order_year_month,
       COUNT(DISTINCT order_id) AS total_orders,
       ROUND(SUM(line_total), 2) AS monthly_revenue
FROM sales_fact
GROUP BY order_year_month
ORDER BY order_year_month DESC
LIMIT 6;
```

```
order_year_month  total_orders  monthly_revenue
────────────────  ────────────  ───────────────
2024-06                    504    1,826,140.77
2024-05                    498    1,714,172.89
2024-04                    488    1,603,573.94
2024-03                    521    1,776,396.74
2024-02                    495    1,731,808.89
2024-01                    523    1,818,034.02
```

### Top 10 Customers by Lifetime Value

```sql
SELECT customer_id, customer_name,
       COUNT(DISTINCT order_id) AS orders,
       ROUND(SUM(line_total), 2) AS lifetime_value
FROM sales_fact
GROUP BY customer_id, customer_name
ORDER BY lifetime_value DESC
LIMIT 10;
```

```
customer_id   customer_name        orders  lifetime_value
───────────── ──────────────────── ──────  ──────────────
CUST-4429     Matthew Robinson          2      16,208.22
CUST-5268     Thomas Thompson           2      15,257.21
CUST-2991     Richard Brown             2      14,589.84
CUST-3317     John Martin               2      14,404.46
CUST-3864     Barbara Young             2      14,236.67
CUST-5617     Lisa Lewis                1      13,477.77
CUST-5111     Charles King              1      13,466.52
CUST-4001     Ashley Wilson             1      13,465.35
CUST-4821     Christopher Wright        1      13,459.50
CUST-1120     Sandra Smith              1      13,438.44
```

### Month-over-Month Growth (window function)

```sql
WITH monthly AS (
    SELECT order_year_month, SUM(line_total) AS revenue
    FROM sales_fact GROUP BY order_year_month
)
SELECT order_year_month,
       ROUND(revenue, 2) AS revenue,
       ROUND(
           (revenue - LAG(revenue) OVER (ORDER BY order_year_month))
           * 100.0 / NULLIF(LAG(revenue) OVER (ORDER BY order_year_month), 0),
       2) AS mom_growth_pct
FROM monthly ORDER BY order_year_month;
```

---

## 🔮 Future Improvements

| Area | Improvement |
|------|-------------|
| Orchestration | Replace `pipeline.py` CLI with **Apache Airflow** DAG for scheduled runs |
| Scalability | Swap pandas for **PySpark** or **DuckDB** for 100M+ row datasets |
| Monitoring | Add **Great Expectations** data quality checks at each stage |
| CI/CD | GitHub Actions workflow to lint, test, and validate on every push |
| Incremental loads | Add `watermark` table to support delta / incremental ingestion |
| API source | Replace CSV with a REST API or streaming source (Kafka, Kinesis) |
| Visualisation | Connect **Metabase** or **Grafana** to the PostgreSQL instance |
| Cloud | Deploy to AWS (S3 → Glue → RDS) or GCP (GCS → Dataflow → BigQuery) |

---

## 🧪 Running Tests

```bash
pytest tests/ -v --cov=src
```

---

## 📁 Environment Variables Reference

| Variable | Default | Description |
|----------|---------|-------------|
| `DB_HOST` | `localhost` | PostgreSQL host |
| `DB_PORT` | `5432` | PostgreSQL port |
| `DB_NAME` | `sales_db` | Database name |


---

## 👤 Author

Built as a portfolio project to demonstrate end-to-end data engineering skills:
ETL design, Python data processing, SQL schema design, and containerised infrastructure.
