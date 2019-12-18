from builtins import range
from datetime import timedelta
from datetime import datetime

import airflow
from airflow.models import DAG
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.hooks.mysql_hook import MySqlHook
### CONSTANTS: DO NOT EDIT ###


## MYSQL CONNECTION
MYSQL_CONN_ID = 'amrs_slave_conn'

## DAG ID
DAG_ID = 'surge_daily'
### END TRIGGER RULES ###

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['fali@ampath.or.ke'],
    'email_on_failure': True,
    'email_on_retry': True,
    'start_date': '2019-12-18',
    'retries': 3,
    'retry_delay': timedelta(seconds=30),
}



dag = DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    catchup=False,
    schedule_interval="0 20 * * *",
    dagrun_timeout=timedelta(minutes=60),
)


update_surge_data = MySqlOperator(
    task_id='update_surge_data',
    sql='CALL etl.generate_surge_weekly_report_dataset_v2("sync",1,15000,20);',
    mysql_conn_id=MYSQL_CONN_ID,
    database='etl',
    dag=dag
);

if __name__ == "__main__":
    dag.cli()
