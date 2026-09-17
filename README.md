# DAG_REST_API
A small pipeline that pulls live product data from a public API and loads it into 
PostgreSQL - automated end-to-end using Apache Airflow instead of running scripts by hand.

## What this project does
1. Pulls live product data from a public API (DummyJSON: https://dummyjson.com/products)
2. Loads it into a PostgreSQL staging table
3. Transforms it into a clean warehouse table

All of this runs automatically through an Airflow DAG called products_api_ingestion, 
instead of running Python and SQL by hand.

The pipeline is idempotent - running it again (daily, or triggered twice by accident) 
updates existing rows instead of creating duplicates.

## Tools used
- Apache Airflow 3.x - schedules and runs the pipeline automatically
- Docker Compose - runs Airflow locally without a manual install
- PostgreSQL - where the data lives
- Python (requests, psycopg2) - fetches the API data and inserts it into Postgres
- DummyJSON API - free public API used as the data source

## How to run this

### 1. Prerequisites
- Docker Desktop installed (WSL2 enabled if on Windows)
- PostgreSQL running locally
- Python with requests and psycopg2-binary installed

Create the staging table first:
```sql
CREATE SCHEMA IF NOT EXISTS staging;

CREATE TABLE IF NOT EXISTS staging.api_products (
    product_id INT PRIMARY KEY,
    title VARCHAR(200),
    category VARCHAR(100),
    price NUMERIC(10,2),
    stock INT
);
```

### 2. Start Airflow
```
docker compose up airflow-init
docker compose up -d
```

### 3. Open the Airflow dashboard
Go to http://localhost:8080 and log in with airflow / airflow.

### 4. Connect Airflow to PostgreSQL
Airflow runs inside Docker, so it can't reach the database with "localhost" - that has 
to be set manually.

Go to Admin, then Connections, then click the plus button, and add:

| Field | Value |
|---|---|
| Connection Id | postgres_default |
| Connection Type | Postgres |
| Host | host.docker.internal |
| Schema | postgres |
| Login | postgres |
| Password | (Postgres password) |
| Port | 5432 |

### 5. Run it
New DAGs start paused by default. Find products_api_ingestion in the DAGs list, 
unpause it, and hit the play button to trigger a run.

## Issues hit along the way

- DAG wasn't showing up in the UI - it was there, just paused, and the UI was 
  filtering by "Active" instead of "All"
- Transform task kept failing - error: conn_id postgres_default isn't defined. 
  The DAG code referenced that connection name, but it was never created inside 
  Airflow's UI - fixed by adding it under Admin, Connections
- localhost didn't work for the database connection - since Airflow runs inside 
  a Docker container, localhost means "inside the container," not the host machine. 
  Fixed by using host.docker.internal instead.