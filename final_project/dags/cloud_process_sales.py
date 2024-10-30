from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator

BUCKET_NAME = "de-07-kondratiuk-final-bucket"
PROJECT_NAME = "de-07-denys-kondratiuk"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
}

with DAG(
    'cloud_process_sales',
    default_args=default_args,
    description='Load data from GCS to BigQuery',
    schedule_interval=None,
) as dag:
    list_files = GCSListObjectsOperator(
        task_id='list_files',
        bucket=BUCKET_NAME,
        prefix='data/sales/',
        delimiter=None,
    )

    gcs_to_bigquery = GCSToBigQueryOperator(
        task_id='gcs_to_bigquery',
        bucket=BUCKET_NAME,
        source_objects=list_files.output,
        destination_project_dataset_table=f'{PROJECT_NAME}.bronze.sales',
        project_id=PROJECT_NAME,
        schema_fields=[
            {'name': 'CustomerId', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'PurchaseDate', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'Product', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'Price', 'type': 'STRING', 'mode': 'NULLABLE'},
        ],
        source_format='CSV',
        write_disposition='WRITE_TRUNCATE',
        create_disposition='CREATE_IF_NEEDED',
        dag=dag,
    )

    transform_query = """
      CREATE OR REPLACE TABLE `silver.sales`
      PARTITION BY purchase_date AS
        SELECT
          CAST(CustomerId AS INT64) AS client_id,
          CASE
            WHEN REGEXP_CONTAINS(PurchaseDate, r'^[0-9]{4}-[A-Za-z]{3}-[0-9]{1,2}$') THEN PARSE_DATE('%Y-%b-%d', PurchaseDate)
            WHEN REGEXP_CONTAINS(PurchaseDate, r'^[0-9]{4}-[0-9]{1,2}-[0-9]{1,2}$') THEN PARSE_DATE('%Y-%m-%d', PurchaseDate)
            WHEN REGEXP_CONTAINS(PurchaseDate, r'^[0-9]{4}/[0-9]{1,2}/[0-9]{1,2}$') THEN PARSE_DATE('%Y/%m/%d', PurchaseDate)
            ELSE NULL
          END AS purchase_date,
          CAST(Product AS STRING) AS product_name,
          CAST(REGEXP_REPLACE(Price, r'[^0-9.]', '') AS FLOAT64) AS price
      FROM
        `bronze.sales`
    """

    bronze_to_silver = BigQueryExecuteQueryOperator(
        task_id='bronze_to_silver',
        sql=transform_query,
        use_legacy_sql=False,
    )

    list_files >> gcs_to_bigquery >> bronze_to_silver
