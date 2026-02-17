import oracledb
import csv
from datetime import datetime

# CONNECTION
conn = oracledb.connect(
    user="retail_dwh",
    password="secret",
    dsn="localhost:1521/ORCLPDB1"
)
cur = conn.cursor()

print("Connected to Oracle")

# DROP TABLES IF EXIST
tables = [
    "FACT_SALES",
    "DIM_DATE",
    "DIM_PRODUCT",
    "DIM_CUSTOMER",
    "STG_RETAIL_TRANSACTIONS"
]

for table in tables:
    try:
        cur.execute(f"DROP TABLE {table} CASCADE CONSTRAINTS")
        print(f"Dropped {table}")
    except:
        pass

conn.commit()

# CREATE STAGING TABLE
cur.execute("""
CREATE TABLE STG_RETAIL_TRANSACTIONS (
    TRANSACTION_ID      VARCHAR2(50),
    TRANSACTION_DATE    DATE,
    CUSTOMER_ID         VARCHAR2(50),
    GENDER              VARCHAR2(10),
    AGE                 NUMBER,
    PRODUCT_CATEGORY    VARCHAR2(50),
    QUANTITY            NUMBER,
    PRICE_PER_UNIT      NUMBER(10,2),
    TOTAL_AMOUNT        NUMBER(10,2)
)
""")

print("Staging table created")

# CREATE DIMENSIONS

# DIM_CUSTOMER
cur.execute("""
CREATE TABLE DIM_CUSTOMER (
    CUSTOMER_KEY NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    CUSTOMER_ID  VARCHAR2(50),
    GENDER       VARCHAR2(10),
    AGE          NUMBER
)
""")

# DIM_PRODUCT
cur.execute("""
CREATE TABLE DIM_PRODUCT (
    PRODUCT_KEY      NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    PRODUCT_CATEGORY VARCHAR2(50)
)
""")

# DIM_DATE
cur.execute("""
CREATE TABLE DIM_DATE (
    DATE_KEY   NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    FULL_DATE  DATE,
    YEAR       NUMBER,
    MONTH      NUMBER,
    DAY        NUMBER
)
""")

print("Dimension tables created")

# ==============================
# CREATE FACT TABLE
# ==============================
cur.execute("""
CREATE TABLE FACT_SALES (
    SALES_KEY      NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    DATE_KEY       NUMBER,
    CUSTOMER_KEY   NUMBER,
    PRODUCT_KEY    NUMBER,
    QUANTITY       NUMBER,
    PRICE_PER_UNIT NUMBER(10,2),
    TOTAL_AMOUNT   NUMBER(10,2),
    CONSTRAINT fk_date FOREIGN KEY (DATE_KEY)
        REFERENCES DIM_DATE(DATE_KEY),
    CONSTRAINT fk_customer FOREIGN KEY (CUSTOMER_KEY)
        REFERENCES DIM_CUSTOMER(CUSTOMER_KEY),
    CONSTRAINT fk_product FOREIGN KEY (PRODUCT_KEY)
        REFERENCES DIM_PRODUCT(PRODUCT_KEY)
)
""")

print("Fact table created")

conn.commit()

# LOAD CSV INTO STAGING
csv_path = "data/retail_sales_dataset.csv"

with open(csv_path, mode="r", encoding="utf-8-sig") as file:
    reader = csv.DictReader(file)

    for row in reader:
        txn_date = datetime.strptime(row["Date"], "%Y-%m-%d")

        cur.execute("""
        INSERT INTO STG_RETAIL_TRANSACTIONS
        VALUES (:1,:2,:3,:4,:5,:6,:7,:8,:9)
        """, (
            row["Transaction ID"],
            txn_date,
            row["Customer ID"],
            row["Gender"],
            int(row["Age"]),
            row["Product Category"],
            int(row["Quantity"]),
            float(row["Price per Unit"]),
            float(row["Total Amount"])
        ))

conn.commit()
print("CSV loaded into staging")

# LOAD DIMENSIONS

# CUSTOMER
cur.execute("""
INSERT INTO DIM_CUSTOMER (CUSTOMER_ID, GENDER, AGE)
SELECT DISTINCT CUSTOMER_ID, GENDER, AGE
FROM STG_RETAIL_TRANSACTIONS
""")

# PRODUCT
cur.execute("""
INSERT INTO DIM_PRODUCT (PRODUCT_CATEGORY)
SELECT DISTINCT PRODUCT_CATEGORY
FROM STG_RETAIL_TRANSACTIONS
""")

# DATE
cur.execute("""
INSERT INTO DIM_DATE (FULL_DATE, YEAR, MONTH, DAY)
SELECT DISTINCT
    TRANSACTION_DATE,
    EXTRACT(YEAR FROM TRANSACTION_DATE),
    EXTRACT(MONTH FROM TRANSACTION_DATE),
    EXTRACT(DAY FROM TRANSACTION_DATE)
FROM STG_RETAIL_TRANSACTIONS
""")

conn.commit()
print("Dimensions populated")

# LOAD FACT TABLE
cur.execute("""
INSERT INTO FACT_SALES
(DATE_KEY, CUSTOMER_KEY, PRODUCT_KEY,
 QUANTITY, PRICE_PER_UNIT, TOTAL_AMOUNT)
SELECT
    d.DATE_KEY,
    c.CUSTOMER_KEY,
    p.PRODUCT_KEY,
    s.QUANTITY,
    s.PRICE_PER_UNIT,
    s.TOTAL_AMOUNT
FROM STG_RETAIL_TRANSACTIONS s
JOIN DIM_CUSTOMER c
    ON s.CUSTOMER_ID = c.CUSTOMER_ID
JOIN DIM_PRODUCT p
    ON s.PRODUCT_CATEGORY = p.PRODUCT_CATEGORY
JOIN DIM_DATE d
    ON s.TRANSACTION_DATE = d.FULL_DATE
""")

conn.commit()
print("Fact table populated")

cur.close()
conn.close()

print("ETL PIPELINE COMPLETED SUCCESSFULLY")
