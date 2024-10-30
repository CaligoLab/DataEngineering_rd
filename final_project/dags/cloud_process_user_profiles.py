from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.trigger_rule import TriggerRule

BUCKET_NAME = "de-07-kondratiuk-final-bucket"
PROJECT_NAME = "de-07-denys-kondratiuk"
USER_PROFILES_JSON_PATH = "data/user_profiles/user_profiles.json"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
}

with DAG(
        'cloud_process_user_profiles',
        default_args=default_args,
        description='Load data from GCS to BigQuery',
        schedule_interval=None,
) as dag:
    gcs_to_bigquery = GCSToBigQueryOperator(
        task_id="gcs_to_bigquery",
        bucket=BUCKET_NAME,
        source_objects=[USER_PROFILES_JSON_PATH],
        source_format="NEWLINE_DELIMITED_JSON",
        destination_project_dataset_table=f"{PROJECT_NAME}.bronze.user_profiles",
        write_disposition="WRITE_TRUNCATE",
    )

    transform_query = """
      CREATE OR REPLACE TABLE `silver.user_profiles` AS
        SELECT
          email,
          SPLIT(full_name, ' ')[OFFSET(0)] AS first_name,
          SPLIT(full_name, ' ')[OFFSET(1)] AS last_name,
          phone_number,
          birth_date,
          state
      FROM
        `bronze.user_profiles`
    """

    bronze_to_silver = BigQueryExecuteQueryOperator(
        task_id='bronze_to_silver',
        sql=transform_query,
        use_legacy_sql=False,
    )

    trigger_task = TriggerDagRunOperator(
        task_id='trigger_task',
        trigger_rule=TriggerRule.ALL_SUCCESS,
        trigger_dag_id='cloud_enrich_user_profiles'
    )

    gcs_to_bigquery >> bronze_to_silver >> trigger_task
