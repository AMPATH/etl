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
DAG_ID = 'test_new_etl_jobs_realtime'
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



resume_replication = MySqlOperator(
    task_id='resume_replication',
    sql='START SLAVE;',
    mysql_conn_id=MYSQL_CONN_ID,
    database='etl',
    dag=dag,
    trigger_rule='all_done'
)


update_flat_obs = MySqlOperator(
    task_id='update_flat_obs',
    sql='flat_obs_v1.3.sql',
    mysql_conn_id=MYSQL_CONN_ID,
    database='etl',
    dag=dag
)



update_flat_orders = MySqlOperator(
    task_id='update_flat_orders',
    sql='flat_orders_v1.1.sql',
    mysql_conn_id=MYSQL_CONN_ID,
    database='etl',
    dag=dag
)

update_flat_lab_obs = MySqlOperator(
    task_id='update_flat_lab_obs',
    sql='flat_lab_obs.sql',
    mysql_conn_id=MYSQL_CONN_ID,
    database='etl',
    dag=dag
)


wait_for_base_tables = BashOperator(
    task_id='wait_for_base_tables',
    bash_command="echo 'Finished all base table jobs' && sleep 5s",
    dag=dag,
)


wait_for_replication_catchup = BashOperator(
    task_id='wait_for_replication_catchup',
    bash_command="echo 'Replication resumed!' && sleep 2m",
    dag=dag,
)

update_hiv_summary = MySqlOperator(
    task_id='update_hiv_summary',
    sql='call generate_hiv_summary_v15_12("sync",1,15000,20);',
    mysql_conn_id=MYSQL_CONN_ID,
    database='etl',
    dag=dag
)

update_flat_covid = MySqlOperator(
    task_id='update_flat_covid',
    sql='CALL `etl`.`generate_flat_covid_screening_v1_0`();',
    mysql_conn_id=MYSQL_CONN_ID,
    database='etl',
    dag=dag
)


#update_hiv_summary = SSHOperator(
#    task_id="update_hiv_summary",
#    command="cd /opt/etl-sync-scripts && node hiv-summary-job.js",
#    ssh_conn_id='.115',
#    dag=dag)


update_vitals = MySqlOperator(
   task_id='update_vitals',
   sql='vitals_v2.2.sql',
   mysql_conn_id=MYSQL_CONN_ID,
   database='etl',
   dag=dag
)

update_flat_labs_and_imaging = MySqlOperator(
    task_id='update_flat_labs_and_imaging',
    sql='call generate_flat_labs_and_imaging("sync",1,15000,100);',
    mysql_conn_id=MYSQL_CONN_ID,
    database='etl',
    dag=dag
)

update_pep_summary = MySqlOperator(
    task_id='update_pep_summary',
    sql='call generate_pep_summary();',
    mysql_conn_id=MYSQL_CONN_ID,
    database='etl',
    dag=dag
)

#update_appointments = SSHOperator(
#    task_id="update_appointments",
#    command="cd /opt/etl-sync-scripts && node appointments-job.js",
#    ssh_conn_id='.115',
#    dag=dag)


update_appointments = MySqlOperator(
    task_id='update_appointments',
    sql='call generate_flat_appointment_v1_1("sync",1,15000,20);',
    mysql_conn_id=MYSQL_CONN_ID,
    database='etl',
    dag=dag
)

update_onc_tables =  MySqlOperator(
    task_id='update_onc_tables',
    sql='sync_onc_tables.sql',
    mysql_conn_id=MYSQL_CONN_ID,
    database='etl',
    dag=dag
)


update_cdm_summary = MySqlOperator(
   task_id='update_cdm_summary',
   sql='sync_cdm_summary_and_monthly_set.sql',
   mysql_conn_id=MYSQL_CONN_ID,
   database='etl',
   dag=dag
)

update_defaulters =  MySqlOperator(
   task_id='update_defaulters',
   sql='call generate_defaulters();',
   mysql_conn_id=MYSQL_CONN_ID,
   database='etl',
   dag=dag
)

update_case_management = MySqlOperator(
    task_id='update_case_management',
    sql='CALL `etl`.`generate_flat_case_manager`("sync",1,10000,10,1);',
    mysql_conn_id=MYSQL_CONN_ID,
    database='etl',
    dag=dag
)



finish = DummyOperator(
    task_id='finish',
    dag=dag
)


def decide_which_path():
    now = datetime.now(timezone('Africa/Nairobi'))
    if now.hour >= 5 and now.hour <= 21:
        return "rerun_trigger"
    else:
        return "sleep_trigger"


def decide_to_run_cdm_summary():
    now = datetime.now(timezone('Africa/Nairobi'))
    if now.hour >= 19:
        return "update_cdm_summary"
    else:
        return "finish"


branch = BranchPythonOperator(
    task_id='branch',
    python_callable=decide_which_path,
    trigger_rule="all_done",
    dag=dag)

cdm_branch = BranchPythonOperator(
    task_id='check_time',
    python_callable=decide_to_run_cdm_summary,
    dag=dag)



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

update_flat_obs >> wait_for_base_tables
update_flat_orders >> wait_for_base_tables
update_flat_lab_obs >> wait_for_base_tables

wait_for_base_tables >> update_hiv_summary
wait_for_base_tables >> update_flat_labs_and_imaging
#wait_for_base_tables >> update_pep_summary
#wait_for_base_tables >> update_vitals


update_hiv_summary >> update_appointments >> update_onc_tables >> update_pep_summary >> cdm_branch
update_hiv_summary >> update_flat_covid >> update_case_management >> cdm_branch
cdm_branch >> update_cdm_summary >> update_defaulters >> update_vitals >> finish
cdm_branch >> finish
update_flat_labs_and_imaging >> finish
#update_vitals >> finish


finish >> resume_replication
resume_replication >> wait_for_replication_catchup
wait_for_replication_catchup >> branch
branch >> rerun_trigger
branch >> sleep_trigger

if __name__ == "__main__":
    dag.cli()
