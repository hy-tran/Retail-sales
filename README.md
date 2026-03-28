# Retail Sales Data Warehouse

An Oracle data warehouse project that implements a **star schema** for retail sales analytics. The pipeline ingests a CSV dataset into a staging table, then transforms and loads it into dimension and fact tables for reporting.

---

## Project Structure

```
retail-sale/
├── data/
│   └── retail_sales_dataset.csv   # 1,000 retail transactions (2023)
├── docs/
│   ├── MyNotes.txt                # Setup and configuration notes
│   └── star_schema.png            # Star schema diagram
├── etl/
│   └── etl_pipeline.py            # Python ETL: drop → create → load → transform
└── plsql/
    ├── 01_staging.sql             # Create staging table
    ├── 02_dimension.sql           # Create & populate dimension tables
    ├── 03_fact.sql                # Create & populate fact table
    ├── 99_validation.sql          # Row count, revenue reconciliation, FK checks
    └── load_staging.ctl           # SQL*Loader control file (staging only)
```

---

## Star Schema

```
                          ┌─────────────────┐
                          │   DIM_CUSTOMER   │
                          │─────────────────│
                          │ CUSTOMER_KEY PK  │
                          │ CUSTOMER_ID      │
                          │ GENDER           │
                          │ AGE              │
                          └────────┬────────┘
                                   │
┌──────────────┐          ┌────────▼────────┐          ┌──────────────────┐
│   DIM_DATE   │          │   FACT_SALES    │          │   DIM_PRODUCT    │
│──────────────│          │─────────────────│          │──────────────────│
│ DATE_KEY  PK │◄─────────│ SALES_KEY    PK │─────────►│ PRODUCT_KEY   PK │
│ FULL_DATE    │          │ DATE_KEY     FK │          │ PRODUCT_CATEGORY │
│ YEAR         │          │ CUSTOMER_KEY FK │          └──────────────────┘
│ MONTH        │          │ PRODUCT_KEY  FK │
│ DAY          │          │ QUANTITY        │
└──────────────┘          │ PRICE_PER_UNIT  │
                          │ TOTAL_AMOUNT    │
                          └─────────────────┘
```

**Staging table** (`STG_RETAIL_TRANSACTIONS`) mirrors the raw CSV and is the source for all dimension and fact inserts.

### ETL Flow

```
CSV file
  └─► STG_RETAIL_TRANSACTIONS  (raw load)
        ├─► DIM_CUSTOMER        (SELECT DISTINCT CUSTOMER_ID, GENDER, AGE)
        ├─► DIM_PRODUCT         (SELECT DISTINCT PRODUCT_CATEGORY)
        ├─► DIM_DATE            (SELECT DISTINCT TRANSACTION_DATE + date parts)
        └─► FACT_SALES          (3-way JOIN back to staging)
```

---

## Dataset

`data/retail_sales_dataset.csv` — 1,000 transactions across 2023.

| Column           | Type    | Description                      |
|------------------|---------|----------------------------------|
| Transaction ID   | String  | Unique transaction identifier    |
| Date             | Date    | Transaction date (YYYY-MM-DD)    |
| Customer ID      | String  | Customer identifier              |
| Gender           | String  | Male / Female                    |
| Age              | Integer | Customer age                     |
| Product Category | String  | Beauty / Clothing / Electronics  |
| Quantity         | Integer | Units purchased                  |
| Price per Unit   | Decimal | Unit price                       |
| Total Amount     | Decimal | Quantity × Price per Unit        |

---

## Prerequisites

- Docker (Oracle Enterprise 21.3 image)
- Python 3 + `oracledb` library (via `venv/`)

---

## Setup

### 1. Start the Oracle container

```bash
docker run -d --name oracle21c \
  -p 1521:1521 -p 5500:5500 \
  -e ORACLE_PWD=secret \
  -v ~/repo/retail-sale:/opt/retail \
  container-registry.oracle.com/database/enterprise:21.3.0.0
```

The repo is mounted inside the container at `/opt/retail`.

### 2. Create the database user (run once)

```bash
docker exec -it oracle21c bash
sqlplus SYSTEM/secret@ORCLPDB1
```

```sql
ALTER SESSION SET CONTAINER=ORCLPDB1;
CREATE USER retail_dwh IDENTIFIED BY secret;
GRANT CONNECT, RESOURCE TO retail_dwh;
ALTER USER retail_dwh DEFAULT TABLESPACE USERS;
ALTER USER retail_dwh QUOTA UNLIMITED ON USERS;
```

---

## Running the ETL

### Option A — Python (fully automated)

Drops and recreates all tables, loads CSV, populates dimensions and fact table.

```bash
cd ~/repo/retail-sale
source venv/bin/activate
python etl/etl_pipeline.py
```

### Option B — Manual SQL scripts (inside SQLPlus as `retail_dwh`)

```sql
@/opt/retail/plsql/01_staging.sql
@/opt/retail/plsql/02_dimension.sql
@/opt/retail/plsql/03_fact.sql
```

### Option C — SQL\*Loader (staging table only)

```bash
# Ensure /tmp is writable inside the container (chmod 777 /tmp)
sqlldr retail_dwh/secret@ORCLPDB1 \
  control=/opt/retail/plsql/load_staging.ctl \
  log=/tmp/load_staging.log \
  bad=/tmp/load_staging.bad
```

---

## Validation

Run inside SQLPlus to verify row counts, revenue reconciliation, and foreign key integrity:

```sql
@/opt/retail/plsql/99_validation.sql
```

Checks performed:
- Row counts for all 5 tables
- Distinct customer and product counts vs. dimension table rows
- `SUM(TOTAL_AMOUNT)` reconciliation between staging and fact
- NULL foreign key check on `FACT_SALES`
- Sample 5-row star join across all dimensions

---

## Connection Details

| Setting  | Value              |
|----------|--------------------|
| Host     | localhost:1521     |
| Service  | ORCLPDB1           |
| User     | retail_dwh         |
| Password | secret             |
