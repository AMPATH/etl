from datetime import datetime
from pytz import timezone
from datetime import timedelta
import airflow
from airflow.models import DAG
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.python_operator import BranchPythonOperator

### CONSTANTS: DO NOT EDIT ###
## MYSQL CONNECTION
MYSQL_CONN_ID = 'amrs_slave_conn'
## DAG ID
DAG_ID = 'check_dag'
ETL_REALTIME_DAG_ID = 'test_new_etl_jobs_realtime'
### END TRIGGER RULES ###

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['fali@ampath.or.ke'],
    'email_on_failure': True,
    'email_on_retry': True,
    'start_date': '2019-06-25',
    'retries': 0,
    'retry_delay': timedelta(minutes=30),
}



dag = DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=600)
)

def decide_which_path():
    now = datetime.now(timezone('Africa/Nairobi'))
    if now.hour >= 22 or now.hour <= 4:
        return "rerun_trigger"
    else:
        return "start_etl_realtime_jobs"

branch = BranchPythonOperator(
    task_id='branch',
    python_callable=decide_which_path,
    trigger_rule="all_done",
    dag=dag)


rerun_trigger = TriggerDagRunOperator(
    task_id='rerun_trigger',
    trigger_dag_id=DAG_ID,
    dag=dag
)

start_etl_realtime_jobs = TriggerDagRunOperator(
    task_id='start_etl_realtime_jobs',
    trigger_dag_id=ETL_REALTIME_DAG_ID,
    dag=dag
)

branch >> rerun_trigger
branch >> start_etl_realtime_jobs


if __name__ == "__main__":
    dag.cli()
