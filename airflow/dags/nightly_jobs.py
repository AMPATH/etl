from builtins import range
from datetime import timedelta
from datetime import datetime
from pytz import timezone

import airflow
from airflow.models import DAG
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.hooks.mysql_hook import MySqlHook
from airflow.operators.python_operator import BranchPythonOperator
### CONSTANTS: DO NOT EDIT ###
## TRIGGER RULES
ONE_SUCCESS = 'one_success'

## MYSQL CONNECTION
MYSQL_CONN_ID = 'amrs_slave_conn'

## DAG ID
DAG_ID = 'nightly_jobs'
SLEEP_DAG_ID = 'check_dag'
### END TRIGGER RULES ###

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['jwangare@ampath.or.ke'],
    'email_on_failure': True,
    'email_on_retry': True,
    'start_date': '2020-01-01',
    'retries': 0,
    'retry_delay': timedelta(minutes=30),
    'max_active_runs':1
}

dag = DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    schedule_interval='0 2 * * *',
    dagrun_timeout=timedelta(minutes=60),
    catchup=False,
    template_searchpath=[
    '/usr/local/airflow/etl-scripts/flat_tables',
    '/usr/local/airflow/etl-scripts/calculated_tables',
    '/usr/local/airflow/etl-scripts/database_updates'
    ]
)

#update_surge_weekly
update_surge_weekly = MySqlOperator(
   task_id='update_surge_weekly',
   sql='generate_surge_weekly_report_dataset_v2.sql("sync",1,15000,20,true);',
   mysql_conn_id=MYSQL_CONN_ID,
   database='etl',
   dag=dag
)

finish = DummyOperator(
    task_id='finish',
    dag=dag
)

update_surge_weekly >> finish

if __name__ == "__main__":
    dag.cli()
