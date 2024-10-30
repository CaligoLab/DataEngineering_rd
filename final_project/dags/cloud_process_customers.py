import datetime
from typing import List

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.operators.python import PythonOperator

BUCKET_NAME = "de-07-kondratiuk-final-bucket"
PROJECT_NAME = "de-07-denys-kondratiuk"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
}


def find_latest(path_list: List[str]) -> str:
    latest_date = datetime.date(year=1961, month=1, day=1)
    for dir in path_list:
        date = datetime.datetime.strptime(dir.split("/")[-2], "%Y-%m-%d").date()
        if date > latest_date:
            latest_date = date
    return f"{latest_date.year}-{latest_date.month:02}-{latest_date.day}"


with DAG(
        'cloud_process_customers',
        default_args=default_args,
        description='Load data from GCS to BigQuery',
        schedule_interval=None,
) as dag:
    list_dirs = GCSListObjectsOperator(
        task_id='list_dirs',
        bucket=BUCKET_NAME,
        prefix='data/customers/',
        delimiter="/",
    )

    find_latest_dir = PythonOperator(
        task_id="find_latest_dir",
        python_callable=find_latest,
        op_kwargs={'path_list': list_dirs.output}
    )

    list_files = GCSListObjectsOperator(
        task_id='list_files',
        bucket=BUCKET_NAME,
        prefix=f'data/customers/{find_latest_dir.output}',
        delimiter=None,
    )

    gcs_to_bigquery = GCSToBigQueryOperator(
        task_id='gcs_to_bigquery',
        bucket=BUCKET_NAME,
        source_objects=list_files.output,
        destination_project_dataset_table=f'{PROJECT_NAME}.bronze.customers',
        project_id=PROJECT_NAME,
        schema_fields=[
            {'name': 'Id', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'FirstName', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'LastName', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'Email', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'RegistrationDate', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'State', 'type': 'STRING', 'mode': 'NULLABLE'},
        ],
        source_format='CSV',
        write_disposition='WRITE_TRUNCATE',
        create_disposition='CREATE_IF_NEEDED',
        dag=dag,
    )
    transform_query = """
      CREATE OR REPLACE TABLE `silver.customers` AS
        SELECT
          CAST(Id AS INT64) AS client_id,
          FirstName AS first_name,
          LastName AS last_name,
          Email AS email,
          CASE
            WHEN REGEXP_CONTAINS(RegistrationDate, r'^[0-9]{4}-[A-Za-z]{3}-[0-9]{1,2}$') THEN PARSE_DATE('%Y-%b-%d', RegistrationDate)
            WHEN REGEXP_CONTAINS(RegistrationDate, r'^[0-9]{4}-[0-9]{1,2}-[0-9]{1,2}$') THEN PARSE_DATE('%Y-%m-%d', RegistrationDate)
            WHEN REGEXP_CONTAINS(RegistrationDate, r'^[0-9]{4}/[0-9]{1,2}/[0-9]{1,2}$') THEN PARSE_DATE('%Y/%m/%d', RegistrationDate)
            ELSE NULL
          END AS registration_date,
          State AS state
      FROM
        `bronze.customers`
    """

    bronze_to_silver = BigQueryExecuteQueryOperator(
        task_id='bronze_to_silver',
        sql=transform_query,
        use_legacy_sql=False,
    )

    list_dirs >> find_latest_dir >> list_files >> gcs_to_bigquery >> bronze_to_silver
