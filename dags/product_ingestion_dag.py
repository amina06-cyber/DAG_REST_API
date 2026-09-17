from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime
import requests
import psycopg2


def fetch_and_upsert_products():
    """Fetch products from DummyJSON API and upsert into staging.api_products."""
    response = requests.get("https://dummyjson.com/products")
    data = response.json()
    products = data["products"]

    conn = psycopg2.connect(
        host="host.docker.internal",  # lets the container reach your local PostgreSQL
        port=5432,
        dbname="postgres",
        user="postgres",
        password="9087"
    )
    cur = conn.cursor()

    for p in products:
        cur.execute("""
            INSERT INTO staging.api_products (product_id, title, category, price, stock)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (product_id) DO UPDATE SET
                title = EXCLUDED.title,
                category = EXCLUDED.category,
                price = EXCLUDED.price,
                stock = EXCLUDED.stock;
        """, (p["id"], p["title"], p["category"], p["price"], p["stock"]))

    conn.commit()
    cur.close()
    conn.close()
    print(f"Inserted/updated {len(products)} products.")


with DAG(
    dag_id="products_api_ingestion",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["week2", "dag_rest_api"],
) as dag:

    fetch_and_upsert_products_task = PythonOperator(
        task_id="fetch_and_upsert_products",
        python_callable=fetch_and_upsert_products,
    )

    transform_products_to_warehouse = SQLExecuteQueryOperator(
        task_id="transform_products_to_warehouse",
        conn_id="postgres_default",
        sql="""
            CREATE SCHEMA IF NOT EXISTS warehouse;

            DROP TABLE IF EXISTS warehouse.dim_products;

            CREATE TABLE warehouse.dim_products AS
            SELECT product_id, title, category, price, stock
            FROM staging.api_products;
        """,
    )

    fetch_and_upsert_products_task >> transform_products_to_warehouse