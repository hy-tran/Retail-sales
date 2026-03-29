"""
DAG 1 — Full ETL Load
Drops all tables, recreates schema, loads the full CSV into staging,
populates dimensions (in parallel), then populates the fact table.
Ends with a validation task that fails the run on row-count or revenue mismatch.
"""

import csv
import os
from datetime import datetime

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator

from db import get_connection, get_csv_path

CSV_DEFAULT = os.path.join(os.path.dirname(__file__), "..", "data", "retail_sales_dataset.csv")


# ---------------------------------------------------------------------------
# Task functions
# ---------------------------------------------------------------------------

def drop_tables():
    conn = get_connection()
    cur = conn.cursor()
    # Drop in FK-safe order (fact first, then dimensions, then staging)
    for table in ["FACT_SALES", "DIM_DATE", "DIM_PRODUCT", "DIM_CUSTOMER", "STG_RETAIL_TRANSACTIONS"]:
        try:
            cur.execute(f"DROP TABLE {table} CASCADE CONSTRAINTS")
            print(f"Dropped {table}")
        except Exception:
            pass
    conn.commit()
    cur.close()
    conn.close()


def create_tables():
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE STG_RETAIL_TRANSACTIONS (
            TRANSACTION_ID   VARCHAR2(50),
            TRANSACTION_DATE DATE,
            CUSTOMER_ID      VARCHAR2(50),
            GENDER           VARCHAR2(10),
            AGE              NUMBER,
            PRODUCT_CATEGORY VARCHAR2(50),
            QUANTITY         NUMBER,
            PRICE_PER_UNIT   NUMBER(10,2),
            TOTAL_AMOUNT     NUMBER(10,2),
            LOAD_TIMESTAMP   TIMESTAMP DEFAULT SYSTIMESTAMP
        )
    """)

    cur.execute("""
        CREATE TABLE DIM_CUSTOMER (
            CUSTOMER_KEY NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            CUSTOMER_ID  VARCHAR2(50),
            GENDER       VARCHAR2(10),
            AGE          NUMBER
        )
    """)

    cur.execute("""
        CREATE TABLE DIM_PRODUCT (
            PRODUCT_KEY      NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            PRODUCT_CATEGORY VARCHAR2(50)
        )
    """)

    cur.execute("""
        CREATE TABLE DIM_DATE (
            DATE_KEY  NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            FULL_DATE DATE,
            YEAR      NUMBER,
            MONTH     NUMBER,
            DAY       NUMBER
        )
    """)

    cur.execute("""
        CREATE TABLE FACT_SALES (
            SALES_KEY      NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            DATE_KEY       NUMBER,
            CUSTOMER_KEY   NUMBER,
            PRODUCT_KEY    NUMBER,
            QUANTITY       NUMBER,
            PRICE_PER_UNIT NUMBER(10,2),
            TOTAL_AMOUNT   NUMBER(10,2),
            CONSTRAINT fk_fs_date     FOREIGN KEY (DATE_KEY)     REFERENCES DIM_DATE(DATE_KEY),
            CONSTRAINT fk_fs_customer FOREIGN KEY (CUSTOMER_KEY) REFERENCES DIM_CUSTOMER(CUSTOMER_KEY),
            CONSTRAINT fk_fs_product  FOREIGN KEY (PRODUCT_KEY)  REFERENCES DIM_PRODUCT(PRODUCT_KEY)
        )
    """)

    conn.commit()
    cur.close()
    conn.close()
    print("All tables created")


def load_csv_to_staging():
    csv_path = get_csv_path(CSV_DEFAULT)
    rows = []
    with open(csv_path, encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            rows.append((
                row["Transaction ID"],
                datetime.strptime(row["Date"], "%Y-%m-%d"),
                row["Customer ID"],
                row["Gender"],
                int(row["Age"]),
                row["Product Category"],
                int(row["Quantity"]),
                float(row["Price per Unit"]),
                float(row["Total Amount"]),
            ))

    conn = get_connection()
    cur = conn.cursor()
    cur.executemany("""
        INSERT INTO STG_RETAIL_TRANSACTIONS
            (TRANSACTION_ID, TRANSACTION_DATE, CUSTOMER_ID, GENDER, AGE,
             PRODUCT_CATEGORY, QUANTITY, PRICE_PER_UNIT, TOTAL_AMOUNT)
        VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9)
    """, rows)
    conn.commit()
    cur.close()
    conn.close()
    print(f"Loaded {len(rows)} rows into staging")


def load_dim_customer():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO DIM_CUSTOMER (CUSTOMER_ID, GENDER, AGE)
        SELECT DISTINCT CUSTOMER_ID, GENDER, AGE
        FROM STG_RETAIL_TRANSACTIONS
    """)
    conn.commit()
    print(f"DIM_CUSTOMER: {cur.rowcount} rows inserted")
    cur.close()
    conn.close()


def load_dim_product():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO DIM_PRODUCT (PRODUCT_CATEGORY)
        SELECT DISTINCT PRODUCT_CATEGORY
        FROM STG_RETAIL_TRANSACTIONS
    """)
    conn.commit()
    print(f"DIM_PRODUCT: {cur.rowcount} rows inserted")
    cur.close()
    conn.close()


def load_dim_date():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO DIM_DATE (FULL_DATE, YEAR, MONTH, DAY)
        SELECT DISTINCT
            TRANSACTION_DATE,
            EXTRACT(YEAR  FROM TRANSACTION_DATE),
            EXTRACT(MONTH FROM TRANSACTION_DATE),
            EXTRACT(DAY   FROM TRANSACTION_DATE)
        FROM STG_RETAIL_TRANSACTIONS
    """)
    conn.commit()
    print(f"DIM_DATE: {cur.rowcount} rows inserted")
    cur.close()
    conn.close()


def load_fact_sales():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO FACT_SALES
            (DATE_KEY, CUSTOMER_KEY, PRODUCT_KEY, QUANTITY, PRICE_PER_UNIT, TOTAL_AMOUNT)
        SELECT
            d.DATE_KEY,
            c.CUSTOMER_KEY,
            p.PRODUCT_KEY,
            s.QUANTITY,
            s.PRICE_PER_UNIT,
            s.TOTAL_AMOUNT
        FROM STG_RETAIL_TRANSACTIONS s
        JOIN DIM_CUSTOMER c
            ON  s.CUSTOMER_ID       = c.CUSTOMER_ID
            AND s.GENDER            = c.GENDER
            AND s.AGE               = c.AGE
        JOIN DIM_PRODUCT p
            ON s.PRODUCT_CATEGORY   = p.PRODUCT_CATEGORY
        JOIN DIM_DATE d
            ON s.TRANSACTION_DATE   = d.FULL_DATE
    """)
    conn.commit()
    print(f"FACT_SALES: {cur.rowcount} rows inserted")
    cur.close()
    conn.close()


def validate_load():
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM STG_RETAIL_TRANSACTIONS")
    stg_count = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM FACT_SALES")
    fact_count = cur.fetchone()[0]

    cur.execute("SELECT SUM(TOTAL_AMOUNT) FROM STG_RETAIL_TRANSACTIONS")
    stg_total = float(cur.fetchone()[0])

    cur.execute("SELECT SUM(TOTAL_AMOUNT) FROM FACT_SALES")
    fact_total = float(cur.fetchone()[0])

    cur.close()
    conn.close()

    print(f"STG rows={stg_count}, FACT rows={fact_count}")
    print(f"STG revenue={stg_total}, FACT revenue={fact_total}")

    if stg_count != fact_count:
        raise AirflowException(f"Row count mismatch: STG={stg_count}, FACT={fact_count}")

    if abs(stg_total - fact_total) > 0.01:
        raise AirflowException(f"Revenue mismatch: STG={stg_total}, FACT={fact_total}")

    print("Validation passed")


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id="retail_full_etl",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["retail", "etl", "full-load"],
) as dag:

    t_drop   = PythonOperator(task_id="drop_tables",         python_callable=drop_tables)
    t_create = PythonOperator(task_id="create_tables",       python_callable=create_tables)
    t_stg    = PythonOperator(task_id="load_csv_to_staging", python_callable=load_csv_to_staging)
    t_dcust  = PythonOperator(task_id="load_dim_customer",   python_callable=load_dim_customer)
    t_dprod  = PythonOperator(task_id="load_dim_product",    python_callable=load_dim_product)
    t_ddate  = PythonOperator(task_id="load_dim_date",       python_callable=load_dim_date)
    t_fact   = PythonOperator(task_id="load_fact_sales",     python_callable=load_fact_sales)
    t_valid  = PythonOperator(task_id="validate_load",       python_callable=validate_load)

    t_drop >> t_create >> t_stg >> [t_dcust, t_dprod, t_ddate] >> t_fact >> t_valid
