from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.hooks.http import HttpHook
from airflow.models.param import Param


def collect_raw_data(**task_context):
    hook = HttpHook(
        method='POST',
        http_conn_id='collect-job'
    )
    ds = task_context.get("ds")
    base_dir = task_context.get("params")["base_dir"]
    response = hook.run('/', json={
        "date": ds,
        "raw_dir": f"{base_dir}/raw/{ds}/"
    })
    assert response.status_code == 201, response.text


def transform_data(**task_context):
    hook = HttpHook(
        method='POST',
        http_conn_id='transform-job'
    )
    ds = task_context.get("ds")
    base_dir = task_context.get("params")["base_dir"]
    response = hook.run('/', json={
        "raw_dir": f"{base_dir}/raw/{ds}",
        "stg_dir": f"{base_dir}/stg/{ds}"
    })
    assert response.status_code == 201, response.text


with DAG(
        dag_id="process_sales",
        schedule="0 1 * * *",
        params={"base_dir": Param(default="/mnt/results")},
        start_date=datetime.strptime("2022-08-09", "%Y-%m-%d"),
        end_date=datetime.strptime("2022-08-12", "%Y-%m-%d"),
        catchup=True,
        max_active_runs=1
) as dag:
    extract_data_from_api = PythonOperator(
        task_id="extract_data_from_api",
        python_callable=collect_raw_data,
    )

    convert_to_avro = PythonOperator(
        task_id="convert_to_avro",
        python_callable=transform_data,
    )

    extract_data_from_api >> convert_to_avro
