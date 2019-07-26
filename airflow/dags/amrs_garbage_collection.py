from builtins import range
from datetime import timedelta
from datetime import datetime
import logging
import math

from airflow.operators.http_operator import SimpleHttpOperator
import airflow
from airflow.models import DAG


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['fali@ampath.or.ke'],
    'email_on_failure': True,
    'email_on_retry': True,
    'email_on_success': False,
    'start_date': datetime(2019, 5, 31),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='amrs_session_and_garbage_collection',
    default_args=default_args,
    schedule_interval= '*/10 * * * *',
    dagrun_timeout=timedelta(minutes=60),
    catchup=False
)

invalidate_http_sessions = SimpleHttpOperator(
    task_id="invalidate_http_sessions",
    endpoint="/amrs/monitoring?action=invalidate_sessions",
    method="GET",
    log_response=True,
    http_conn_id='ngx',
    dag=dag)

garbage_collection = SimpleHttpOperator(
    task_id="garbage_collection",
    endpoint="/amrs/monitoring?action=gc",
    method="GET",
    log_response=True,
    http_conn_id='ngx',
    dag=dag)

invalidate_http_sessions >> garbage_collection
