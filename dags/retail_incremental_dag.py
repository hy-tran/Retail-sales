"""
DAG 2 — Incremental Daily Load
Assumes schema already exists (run retail_full_etl first).
Loads only rows matching the Airflow execution date from the CSV,
MERGEs new dimension values, and appends to FACT_SALES.
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

def check_schema_exists():
    conn = get_connection()
    cur = conn.cursor()
    required = {"STG_RETAIL_TRANSACTIONS", "DIM_CUSTOMER", "DIM_PRODUCT", "DIM_DATE", "FACT_SALES"}
    cur.execute(
        "SELECT TABLE_NAME FROM USER_TABLES WHERE TABLE_NAME IN "
        "(:1, :2, :3, :4, :5)",
        list(required),
    )
    found = {row[0] for row in cur.fetchall()}
    cur.close()
    conn.close()

    missing = required - found
    if missing:
        raise AirflowException(
            f"Missing tables: {missing}. Trigger the retail_full_etl DAG first."
        )
    print("Schema check passed — all tables present")


def extract_daily_slice(execution_date: str):
    csv_path = get_csv_path(CSV_DEFAULT)
    rows = []
    with open(csv_path, encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            if row["Date"] == execution_date:
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

    if not rows:
        print(f"WARNING: No rows found for {execution_date} — downstream tasks will be no-ops")
        return

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
    print(f"Inserted {len(rows)} staging rows for {execution_date}")


def merge_dim_customer(execution_date: str):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        MERGE INTO DIM_CUSTOMER tgt
        USING (
            SELECT DISTINCT CUSTOMER_ID, GENDER, AGE
            FROM STG_RETAIL_TRANSACTIONS
            WHERE TRANSACTION_DATE = TO_DATE(:exec_date, 'YYYY-MM-DD')
        ) src
        ON (    tgt.CUSTOMER_ID = src.CUSTOMER_ID
            AND tgt.GENDER      = src.GENDER
            AND tgt.AGE         = src.AGE)
        WHEN NOT MATCHED THEN
            INSERT (CUSTOMER_ID, GENDER, AGE)
            VALUES (src.CUSTOMER_ID, src.GENDER, src.AGE)
    """, exec_date=execution_date)
    conn.commit()
    print(f"DIM_CUSTOMER: {cur.rowcount} rows merged for {execution_date}")
    cur.close()
    conn.close()


def merge_dim_product(execution_date: str):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        MERGE INTO DIM_PRODUCT tgt
        USING (
            SELECT DISTINCT PRODUCT_CATEGORY
            FROM STG_RETAIL_TRANSACTIONS
            WHERE TRANSACTION_DATE = TO_DATE(:exec_date, 'YYYY-MM-DD')
        ) src
        ON (tgt.PRODUCT_CATEGORY = src.PRODUCT_CATEGORY)
        WHEN NOT MATCHED THEN
            INSERT (PRODUCT_CATEGORY)
            VALUES (src.PRODUCT_CATEGORY)
    """, exec_date=execution_date)
    conn.commit()
    print(f"DIM_PRODUCT: {cur.rowcount} rows merged for {execution_date}")
    cur.close()
    conn.close()


def merge_dim_date(execution_date: str):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        MERGE INTO DIM_DATE tgt
        USING (
            SELECT DISTINCT
                TRANSACTION_DATE                           AS FULL_DATE,
                EXTRACT(YEAR  FROM TRANSACTION_DATE)       AS YEAR,
                EXTRACT(MONTH FROM TRANSACTION_DATE)       AS MONTH,
                EXTRACT(DAY   FROM TRANSACTION_DATE)       AS DAY
            FROM STG_RETAIL_TRANSACTIONS
            WHERE TRANSACTION_DATE = TO_DATE(:exec_date, 'YYYY-MM-DD')
        ) src
        ON (tgt.FULL_DATE = src.FULL_DATE)
        WHEN NOT MATCHED THEN
            INSERT (FULL_DATE, YEAR, MONTH, DAY)
            VALUES (src.FULL_DATE, src.YEAR, src.MONTH, src.DAY)
    """, exec_date=execution_date)
    conn.commit()
    print(f"DIM_DATE: {cur.rowcount} rows merged for {execution_date}")
    cur.close()
    conn.close()


def append_fact_sales(execution_date: str):
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
        WHERE s.TRANSACTION_DATE = TO_DATE(:exec_date, 'YYYY-MM-DD')
    """, exec_date=execution_date)
    conn.commit()
    print(f"FACT_SALES: {cur.rowcount} rows appended for {execution_date}")
    cur.close()
    conn.close()


def validate_daily_load(execution_date: str):
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT COUNT(*) FROM STG_RETAIL_TRANSACTIONS
        WHERE TRANSACTION_DATE = TO_DATE(:1, 'YYYY-MM-DD')
    """, [execution_date])
    stg_count = cur.fetchone()[0]

    cur.execute("""
        SELECT COUNT(*) FROM FACT_SALES f
        JOIN DIM_DATE d ON f.DATE_KEY = d.DATE_KEY
        WHERE d.FULL_DATE = TO_DATE(:1, 'YYYY-MM-DD')
    """, [execution_date])
    fact_count = cur.fetchone()[0]

    cur.close()
    conn.close()

    print(f"[{execution_date}] STG rows={stg_count}, FACT rows={fact_count}")

    if stg_count != fact_count:
        raise AirflowException(
            f"[{execution_date}] Row mismatch: STG={stg_count}, FACT={fact_count}"
        )
    print(f"[{execution_date}] Validation passed")


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id="retail_incremental_load",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["retail", "etl", "incremental"],
) as dag:

    t_check   = PythonOperator(
        task_id="check_schema_exists",
        python_callable=check_schema_exists,
    )
    t_extract = PythonOperator(
        task_id="extract_daily_slice",
        python_callable=extract_daily_slice,
        op_kwargs={"execution_date": "{{ ds }}"},
    )
    t_mcust   = PythonOperator(
        task_id="merge_dim_customer",
        python_callable=merge_dim_customer,
        op_kwargs={"execution_date": "{{ ds }}"},
    )
    t_mprod   = PythonOperator(
        task_id="merge_dim_product",
        python_callable=merge_dim_product,
        op_kwargs={"execution_date": "{{ ds }}"},
    )
    t_mdate   = PythonOperator(
        task_id="merge_dim_date",
        python_callable=merge_dim_date,
        op_kwargs={"execution_date": "{{ ds }}"},
    )
    t_fact    = PythonOperator(
        task_id="append_fact_sales",
        python_callable=append_fact_sales,
        op_kwargs={"execution_date": "{{ ds }}"},
    )
    t_valid   = PythonOperator(
        task_id="validate_daily_load",
        python_callable=validate_daily_load,
        op_kwargs={"execution_date": "{{ ds }}"},
    )

    t_check >> t_extract >> [t_mcust, t_mprod, t_mdate] >> t_fact >> t_valid
