"""Sari-sari store sales ETL.
landing CSV -> data lake raw zone -> data lake staged zone (Parquet)
-> PostgreSQL staging -> star schema (dw) -> quality checks
"""
from __future__ import annotations

import shutil
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

LANDING = Path("/opt/airflow/data/landing")
RAW = Path("/opt/airflow/datalake/raw/sales")
STAGED = Path("/opt/airflow/datalake/staged/sales")
DWH_CONN_ID = "dwh_postgres"  # comes from AIRFLOW_CONN_DWH_POSTGRES in docker-compose

STG_COLUMNS = [
    "order_id", "order_date", "store_id", "store_name", "barangay", "city",
    "product_id", "product_name", "category", "quantity", "unit_price",
    "line_total", "batch_id",
]

# Staging -> star schema. Every statement is an upsert, so re-running is safe.
LOAD_DW_SQL = """
INSERT INTO dw.dim_date (
    date_key, full_date, day_of_week, is_weekend,
    month, month_name, quarter, year
)
SELECT DISTINCT
    TO_CHAR(order_date, 'YYYYMMDD')::INT,
    order_date,
    TRIM(TO_CHAR(order_date, 'Day')),
    EXTRACT(ISODOW FROM order_date) IN (6, 7),
    EXTRACT(MONTH FROM order_date)::INT,
    TRIM(TO_CHAR(order_date, 'Month')),
    EXTRACT(QUARTER FROM order_date)::INT,
    EXTRACT(YEAR FROM order_date)::INT
FROM staging.stg_sales
ON CONFLICT (date_key) DO NOTHING;

INSERT INTO dw.dim_store (store_id, store_name, barangay, city)
SELECT DISTINCT ON (store_id) store_id, store_name, barangay, city
FROM staging.stg_sales
ORDER BY store_id
ON CONFLICT (store_id) DO UPDATE
SET store_name = EXCLUDED.store_name,
    barangay = EXCLUDED.barangay,
    city = EXCLUDED.city;

INSERT INTO dw.dim_product (product_id, product_name, category)
SELECT DISTINCT ON (product_id) product_id, product_name, category
FROM staging.stg_sales
ORDER BY product_id
ON CONFLICT (product_id) DO UPDATE
SET product_name = EXCLUDED.product_name,
    category = EXCLUDED.category;

INSERT INTO dw.fact_sales (
    order_id, date_key, store_key, product_key,
    quantity, unit_price, line_total, batch_id
)
SELECT
    s.order_id,
    TO_CHAR(s.order_date, 'YYYYMMDD')::INT,
    ds.store_key,
    dp.product_key,
    s.quantity,
    s.unit_price,
    s.line_total,
    s.batch_id
FROM staging.stg_sales s
JOIN dw.dim_store ds ON ds.store_id = s.store_id
JOIN dw.dim_product dp ON dp.product_id = s.product_id
ON CONFLICT (order_id, product_key) DO UPDATE
SET date_key = EXCLUDED.date_key,
    store_key = EXCLUDED.store_key,
    quantity = EXCLUDED.quantity,
    unit_price = EXCLUDED.unit_price,
    line_total = EXCLUDED.line_total,
    batch_id = EXCLUDED.batch_id;
"""


@dag(
    dag_id="sarisari_sales_etl",
    schedule="@daily",
    start_date=datetime(2026, 10, 1),
    catchup=False,
    default_args={"retries": 1},
    tags=["cselec1c", "etl"],
)
def sarisari_sales_etl():
    @task
    def extract() -> dict:
        """E: copy landing files, untouched, into a new batch folder in the raw zone."""
        files = sorted(LANDING.glob("sales_*.csv"))
        if not files:
            raise FileNotFoundError(f"No sales_*.csv files found in {LANDING}")

        batch_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        target = RAW / f"ingest_date={batch_id[:8]}" / f"batch={batch_id}"
        target.mkdir(parents=True, exist_ok=True)
        copied = []
        for f in files:
            shutil.copy2(f, target / f.name)
            copied.append(str(target / f.name))
        print(f"Copied {len(copied)} file(s) to {target}")
        return {"batch_id": batch_id, "files": copied}

    @task
    def transform(raw: dict) -> dict:
        """T: clean and type raw rows via DuckDB, write Parquet to the staged zone."""
        import time
        import duckdb

        start_time = time.perf_counter()
        raw_files = raw["files"]
        batch_id = raw["batch_id"]

        out = STAGED / f"batch={batch_id}" / "sales_clean.parquet"
        out.parent.mkdir(parents=True, exist_ok=True)

        # Format Python list into DuckDB SQL array: ['path1','path2']
        paths_sql = "[" + ", ".join(f"'{p}'" for p in raw_files) + "]"

        con = duckdb.connect()
        
        # Count incoming raw rows from CSVs in data lake
        total_raw = con.execute(
            f"SELECT COUNT(*) FROM read_csv({paths_sql}, header=true, all_varchar=true)"
        ).fetchone()[0]

        # Vectorized SQL transformation, cleaning, validation, and Parquet export
        transform_query = f"""
        COPY (
            WITH cleaned AS (
                SELECT 
                    TRIM(order_id) AS order_id,
                    TRY_CAST(TRIM(order_date) AS DATE) AS order_date,
                    TRIM(store_id) AS store_id,
                    TRIM(store_name) AS store_name,
                    TRIM(barangay) AS barangay,
                    TRIM(city) AS city,
                    TRIM(product_id) AS product_id,
                    TRIM(product_name) AS product_name,
                    TRIM(category) AS category,
                    TRY_CAST(TRIM(quantity) AS DOUBLE) AS raw_qty,
                    TRY_CAST(TRIM(unit_price) AS NUMERIC(10,2)) AS unit_price
                FROM read_csv({paths_sql}, header=true, all_varchar=true)
            ),
            filtered AS (
                SELECT 
                    order_id, order_date, store_id, store_name, barangay, city,
                    product_id, product_name, category,
                    CAST(raw_qty AS INTEGER) AS quantity,
                    unit_price,
                    ROUND(CAST(raw_qty AS INTEGER) * unit_price, 2) AS line_total,
                    '{batch_id}' AS batch_id,
                    ROW_NUMBER() OVER (
                        PARTITION BY order_id, product_id 
                        ORDER BY order_id
                    ) AS dedup_rank
                FROM cleaned
                WHERE order_id IS NOT NULL 
                  AND order_date IS NOT NULL 
                  AND store_id IS NOT NULL 
                  AND product_id IS NOT NULL 
                  AND raw_qty IS NOT NULL 
                  AND unit_price IS NOT NULL
                  AND raw_qty > 0 
                  AND unit_price >= 0
            )
            SELECT 
                order_id, order_date, store_id, store_name, barangay, city,
                product_id, product_name, category, quantity, unit_price,
                line_total, batch_id
            FROM filtered
            WHERE dedup_rank = 1
        ) TO '{str(out)}' (FORMAT PARQUET);
        """

        con.execute(transform_query)

        # Verify staged Parquet row count, then calculate benchmark
        kept_rows = con.execute(f"SELECT COUNT(*) FROM read_parquet('{str(out)}')").fetchone()[0]
        elapsed = time.perf_counter() - start_time
        rejected = total_raw - kept_rows

        print(f"[DuckDB Transform] Rows in: {total_raw}, kept: {kept_rows}, rejected: {rejected}")
        print(f"[DuckDB Benchmark] Execution time: {elapsed:.4f} seconds")

        return {
            "batch_id": batch_id,
            "path": str(out),
            "rows_in": total_raw,
            "rows_out": kept_rows,
            "execution_time_sec": elapsed,
        }

    @task
    def load(staged: dict) -> dict:
        """L: staged Parquet -> staging table -> dimensions and fact."""
        df = pd.read_parquet(staged["path"])
        hook = PostgresHook(postgres_conn_id=DWH_CONN_ID)
        hook.run("TRUNCATE staging.stg_sales;")
        # astype(object) turns numpy numbers into plain Python values that psycopg2 understands
        rows = df[STG_COLUMNS].astype(object).itertuples(index=False, name=None)

        hook.insert_rows(
            table="staging.stg_sales",
            rows=rows,
            target_fields=STG_COLUMNS,
            commit_every=1000,
        )

        hook.run(LOAD_DW_SQL)
        fact_rows = hook.get_first("SELECT COUNT(*) FROM dw.fact_sales")[0]
        print(f"Loaded {len(df)} staged rows; fact_sales now has {fact_rows} rows")

        return {"batch_id": staged["batch_id"], "rows_loaded": len(df)}

    @task
    def quality_check(loaded: dict) -> None:
        """Fail the run loudly if the warehouse breaks a business rule."""
        hook = PostgresHook(postgres_conn_id=DWH_CONN_ID)
        checks = {
            "fact table is not empty": "SELECT COUNT(*) > 0 FROM dw.fact_sales",
            "every staged row reached the fact table": (
                "SELECT (SELECT COUNT(*) FROM staging.stg_sales) = "
                "(SELECT COUNT(*) FROM dw.fact_sales WHERE batch_id = %(b)s)"
            ),
            "no non-positive quantities": "SELECT COUNT(*) = 0 FROM dw.fact_sales WHERE quantity <= 0",
            "line_total equals quantity x unit_price": (
                "SELECT COUNT(*) = 0 FROM dw.fact_sales "
                "WHERE line_total <> ROUND(quantity * unit_price, 2)"
            ),
            # Exercise 2
            "noodle products under 35 PHP": (
                "SELECT COUNT(*) = 0 "
                "FROM dw.fact_sales f "
                "JOIN dw.dim_product p USING (product_key) "
                "WHERE p.category ILIKE 'Noodles' AND f.unit_price > 35.00"
            ),
        }
        failed = [
            name
            for name, sql in checks.items()
            if not hook.get_first(sql, parameters={"b": loaded["batch_id"]})[0]
        ]
        if failed:
            raise ValueError(f"Data quality checks failed: {failed}")
        print(f"All {len(checks)} checks passed for batch {loaded['batch_id']}")

    quality_check(load(transform(extract())))


sarisari_sales_etl()