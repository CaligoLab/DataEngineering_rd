from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator

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
        'cloud_enrich_user_profiles',
        default_args=default_args,
        description='Enrich data and save into golden layer',
        schedule_interval=None,
) as dag:
    transform_query = """
    # 
      CREATE OR REPLACE TABLE `gold.enrich_user_profiles` AS
        SELECT 
          c.client_id,
          c.email, 
          c.registration_date, 
          up.first_name, 
          up.last_name, 
          up.birth_date, 
          up.state, 
          up.phone_number
        FROM silver.customers as c
        JOIN silver.user_profiles as up
          ON c.email = up.email
    """

    enrich_to_gold = BigQueryExecuteQueryOperator(
        task_id='enrich_to_gold',
        sql=transform_query,
        use_legacy_sql=False,
    )

    enrich_to_gold
