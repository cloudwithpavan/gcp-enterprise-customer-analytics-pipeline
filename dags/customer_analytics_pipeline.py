"""
Customer Analytics pipeline for Cloud Composer 3 / Apache Airflow 2.

Configuration is supplied by Terraform as Composer environment variables.
"""
from datetime import timedelta
import os

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from google.cloud import bigquery, storage


def get_config():
    required = ["GCP_PROJECT_ID", "GCS_BUCKET_NAME", "BQ_DATASET_ID", "BQ_LOCATION"]
    missing = [name for name in required if not os.environ.get(name)]
    if missing:
        raise ValueError(f"Missing Composer environment variables: {', '.join(missing)}")

    return {
        "project_id": os.environ["GCP_PROJECT_ID"],
        "bucket_name": os.environ["GCS_BUCKET_NAME"],
        "gcs_file": os.environ.get("GCS_FILE", "customers.csv"),
        "dataset_id": os.environ["BQ_DATASET_ID"],
        "location": os.environ["BQ_LOCATION"],
    }


def table_ids(config):
    prefix = f"{config['project_id']}.{config['dataset_id']}"
    return {
        "staging": f"{prefix}.customers_staging",
        "clean": f"{prefix}.customers_cleaned",
        "report": f"{prefix}.customer_city_report",
    }


def detect_file(**context):
    config = get_config()
    client = storage.Client(project=config["project_id"])
    blob = client.bucket(config["bucket_name"]).blob(config["gcs_file"])

    if not blob.exists(client):
        raise FileNotFoundError(
            f"Input file not found: gs://{config['bucket_name']}/{config['gcs_file']}"
        )

    print(f"Input file found: gs://{config['bucket_name']}/{config['gcs_file']}")
    print(f"Size in bytes: {blob.size}")


def ingest_data(**context):
    config = get_config()
    tables = table_ids(config)
    client = bigquery.Client(project=config["project_id"])
    uri = f"gs://{config['bucket_name']}/{config['gcs_file']}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=False,
        schema=[
            bigquery.SchemaField("customer_id", "INT64"),
            bigquery.SchemaField("full_name", "STRING"),
            bigquery.SchemaField("email", "STRING"),
            bigquery.SchemaField("city", "STRING"),
            bigquery.SchemaField("salary", "NUMERIC"),
            bigquery.SchemaField("created_at", "TIMESTAMP"),
        ],
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        allow_quoted_newlines=True,
    )

    job = client.load_table_from_uri(
        uri, tables["staging"], job_config=job_config, location=config["location"]
    )
    job.result()
    table = client.get_table(tables["staging"])
    print(f"Loaded {table.num_rows} rows into {tables['staging']}")


def validate_data(**context):
    config = get_config()
    tables = table_ids(config)
    client = bigquery.Client(project=config["project_id"])

    sql = f"""
    WITH duplicate_ids AS (
      SELECT customer_id
      FROM `{tables['staging']}`
      WHERE customer_id IS NOT NULL
      GROUP BY customer_id
      HAVING COUNT(*) > 1
    )
    SELECT
      COUNTIF(
        customer_id IS NULL
        OR full_name IS NULL OR TRIM(full_name) = ''
        OR email IS NULL OR TRIM(email) = ''
        OR city IS NULL OR TRIM(city) = ''
        OR salary IS NULL OR salary < 0
        OR created_at IS NULL
      ) AS invalid_rows,
      (SELECT COUNT(*) FROM duplicate_ids) AS duplicate_customer_ids,
      COUNT(*) AS total_rows
    FROM `{tables['staging']}`
    """
    row = next(iter(client.query(sql, location=config["location"]).result()))
    print(
        f"Rows checked: {row.total_rows}; invalid rows: {row.invalid_rows}; "
        f"duplicate customer IDs: {row.duplicate_customer_ids}"
    )
    if row.invalid_rows > 0 or row.duplicate_customer_ids > 0:
        raise ValueError("Validation failed. Correct the source CSV and rerun the DAG.")


def transform_data(**context):
    config = get_config()
    tables = table_ids(config)
    client = bigquery.Client(project=config["project_id"])

    sql = f"""
    CREATE OR REPLACE TABLE `{tables['clean']}` AS
    SELECT
      customer_id,
      TRIM(full_name) AS full_name,
      LOWER(TRIM(email)) AS email,
      TRIM(city) AS city,
      salary,
      created_at
    FROM `{tables['staging']}`
    """
    client.query(sql, location=config["location"]).result()
    print(f"Created cleaned table: {tables['clean']}")


def create_report(**context):
    config = get_config()
    tables = table_ids(config)
    client = bigquery.Client(project=config["project_id"])

    sql = f"""
    CREATE OR REPLACE TABLE `{tables['report']}` AS
    SELECT
      city,
      COUNT(*) AS total_customers,
      AVG(salary) AS average_salary,
      SUM(salary) AS total_salary,
      CURRENT_TIMESTAMP() AS report_generated_at
    FROM `{tables['clean']}`
    GROUP BY city
    """
    client.query(sql, location=config["location"]).result()

    for row in client.query(
        f"SELECT * FROM `{tables['report']}` ORDER BY city",
        location=config["location"],
    ).result():
        print(dict(row))


default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="customer_analytics_pipeline",
    description="GCS to BigQuery customer analytics pipeline with validation and reporting",
    default_args=default_args,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule="0 2 * * *",
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=1),
    tags=["gcp", "bigquery", "terraform", "training"],
) as dag:
    file_detection = PythonOperator(task_id="file_detection", python_callable=detect_file)
    ingestion = PythonOperator(task_id="ingestion", python_callable=ingest_data)
    validation = PythonOperator(task_id="validation", python_callable=validate_data)
    transformation = PythonOperator(task_id="transformation", python_callable=transform_data)
    final_reporting = PythonOperator(task_id="final_reporting", python_callable=create_report)

    file_detection >> ingestion >> validation >> transformation >> final_reporting
