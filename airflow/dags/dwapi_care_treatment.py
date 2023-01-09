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
DAG_ID = 'dwapi_care_treatment'
SLEEP_DAG_ID = 'check_dag'
### END TRIGGER RULES ###

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['fali@ampath.or.ke'],
    'email_on_failure': True,
    'email_on_retry': True,
    'start_date': '2019-05-20',
    'retries': 0,
    'retry_delay': timedelta(minutes=30),
    'max_active_runs':1
}



dag = DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=60),
    catchup=False,
    template_searchpath=[
    '/usr/local/airflow/etl-scripts/flat_tables',
    '/usr/local/airflow/etl-scripts/calculated_tables',
    '/usr/local/airflow/etl-scripts/database_updates'
    ]
)


class CustomMySqlOperator(MySqlOperator):
    def execute(self, context):
        self.log.info('Executing: %s', self.sql)
        hook = MySqlHook(mysql_conn_id=self.mysql_conn_id,
                         schema=self.database)
        return hook.get_records(self.sql, parameters=self.parameters)









sync_patients_extracts = MySqlOperator(
    task_id='sync_patients_extracts',
    sql='CALL `ndwr`.`build_NDWR_all_patients_extract`("sync",1,15000, 1,"2013-01-01",true);',
    mysql_conn_id=MYSQL_CONN_ID,
    database='ndwr',
    dag=dag
)

sync_patients_art_extracts = MySqlOperator(
    task_id='sync_patients_art_extracts',
    sql='CALL `ndwr`.`build_NDWR_patient_art_extract`("sync",1,15000,1,true);',
    mysql_conn_id=MYSQL_CONN_ID,
    database='ndwr',
    dag=dag
)

sync_patients_status_extracts = MySqlOperator(
    task_id='sync_patients_status_extracts',
    sql='CALL `ndwr`.`build_NDWR_all_patient_status_extract`("sync",1,15000,1,true);',
    mysql_conn_id=MYSQL_CONN_ID,
    database='ndwr',
    dag=dag
)


sync_patients_adverse_events = MySqlOperator(
    task_id='sync_patients_adverse_events',
    sql='CALL `ndwr`.`build_NDWR_adverse_event`("sync",1,15000,1,true);',
    mysql_conn_id=MYSQL_CONN_ID,
    database='ndwr',
    dag=dag
)

sync_patient_labs_extracts = MySqlOperator(
    task_id='sync_patient_labs_extracts',
    sql='CALL `ndwr`.`build_NDWR_ndwr_patient_labs_extract`("sync",1,15000,1,true);',
    mysql_conn_id=MYSQL_CONN_ID,
    database='ndwr',
    dag=dag
)

sync_patient_visits_extracts = MySqlOperator(
    task_id='sync_patient_visits_extracts',
    sql='CALL `ndwr`.`build_NDWR_all_patient_visits_extract`("sync",1,1,1,true);',
    mysql_conn_id=MYSQL_CONN_ID,
    database='ndwr',
    dag=dag
)
sync_patient_baselines_extracts = MySqlOperator(
    task_id='sync_patient_baselines_extracts',
    sql='CALL `ndwr`.`build_ndwr_patient_baselines_extract`("sync",1,15000,true);',
    mysql_conn_id=MYSQL_CONN_ID,
    database='ndwr',
    dag=dag
)

sync_patient_pharmacy = MySqlOperator(
    task_id='sync_patient_pharmacy',
    sql='CALL `ndwr`.`build_NDWR_pharmacy`("sync",1,15000,1,true);',
    mysql_conn_id=MYSQL_CONN_ID,
    database='ndwr',
    dag=dag
)

def run_ndwr_scripts():
    now = datetime.now(timezone('Africa/Nairobi'))
    if now.hour >= 19:
        return "start_sync"
    else:
        return "finish"

rerun_trigger = TriggerDagRunOperator(
    task_id='rerun_trigger',
    trigger_dag_id=DAG_ID,
    dag=dag
)


sleep_trigger = TriggerDagRunOperator(
    task_id='sleep_trigger',
    trigger_dag_id=SLEEP_DAG_ID,
    dag=dag
)
start_sync = BashOperator(
    task_id='start_sync',
    bash_command="sleep 15s",
    dag=dag,
)

finish_sync = BashOperator(
    task_id='finish_sync',
    bash_command="sleep 15s",
    dag=dag,
)

start_sync >> sync_patients_extracts >> sync_patients_art_extracts >> sync_patients_status_extracts >> sync_patients_adverse_events >> finish_sync
start_sync >> sync_patient_labs_extracts >> finish
start_sync >> sync_patient_visits_extracts >> sync_patient_baselines_extracts >> sync_patient_pharmacy >> finish_sync

finish_sync >> rerun_trigger



if __name__ == "__main__":
    dag.cli()
