from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.hooks.http import HttpHook
from airflow.models.param import Param
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator

BUCKET_NAME = "de-07-kondraiuk-csv-bucket"


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
    raw_dir = f"{base_dir}/raw/{ds}"
    stg_dir = f"{base_dir}/csv/{ds}"
    response = hook.run('/csv', json={
        "raw_dir": raw_dir,
        "stg_dir": stg_dir
    })
    assert response.status_code == 201, response.text

    task_context.get("ti").xcom_push(key="stg_dir", value=stg_dir)


with DAG(
        dag_id="cloud_process_sales",
        schedule="0 1 * * *",
        params={"base_dir": Param(default="/mnt/results")},
        start_date=datetime.strptime("2022-08-09", "%Y-%m-%d"),
        end_date=datetime.strptime("2022-08-11", "%Y-%m-%d"),
        catchup=True,
        max_active_runs=1,
        render_template_as_native_obj=True
) as dag:
    extract_data_from_api = PythonOperator(
        task_id="extract_data_from_api",
        python_callable=collect_raw_data,
    )

    convert_to_csv = PythonOperator(
        task_id="convert_to_csv",
        python_callable=transform_data,
        do_xcom_push=True,
    )

    upload_file = LocalFilesystemToGCSOperator(
        task_id="upload_file",
        src="{{ ti.xcom_pull(key='stg_dir', task_ids=['convert_to_csv'])[0] }}/*",
        dst="{{ ti.xcom_pull(key='stg_dir', task_ids=['convert_to_csv'])[0].replace('/mnt/', '') }}/",
        bucket=BUCKET_NAME,
    )

    extract_data_from_api >> convert_to_csv >> upload_file
