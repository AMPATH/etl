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
DAG_ID = 'build_hiv_summaries'
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



sleep1 = BashOperator(
    task_id='sleep1',
    bash_command="sleep 30",
    dag=dag,
)
sleep2 = BashOperator(
    task_id='sleep2',
    bash_command="sleep 30",
    dag=dag,
)

sleep3 = BashOperator(
    task_id='sleep3',
    bash_command="sleep 15s",
    dag=dag,
)
sleep4 = BashOperator(
    task_id='sleep4',
    bash_command="sleep 2m",
    dag=dag,
)
sleep5 = BashOperator(
    task_id='sleep5',
    bash_command="sleep 4m",
    dag=dag,
)

sleep6 = BashOperator(
    task_id='sleep6',
    bash_command="sleep 6m",
    dag=dag,
)
sleep7 = BashOperator(
    task_id='sleep7',
    bash_command="sleep 8m",
    dag=dag,
)

build_hiv_summary_1 = MySqlOperator(
    task_id='build_hiv_summary_1',
    sql='call generate_flat_hiv_summary_vitals("build",11,500,1);',
    mysql_conn_id=MYSQL_CONN_ID,
    database='etl',
    dag=dag
)

build_hiv_summary_2 = MySqlOperator(
    task_id='build_hiv_summary_2',
    sql='call generate_flat_hiv_summary_vitals("build",12,500,1);',
    mysql_conn_id=MYSQL_CONN_ID,
    database='etl',
    dag=dag
)

build_hiv_summary_3 = MySqlOperator(
    task_id='build_hiv_summary_3',
    sql='call generate_flat_hiv_summary_vitals("build",13,500,1);',
    mysql_conn_id=MYSQL_CONN_ID,
    database='etl',
    dag=dag
)

build_hiv_summary_4 = MySqlOperator(
    task_id='build_hiv_summary_4',
    sql='call generate_flat_hiv_summary_vitals("build",14,500,1);',
    mysql_conn_id=MYSQL_CONN_ID,
    database='etl',
    dag=dag
)

build_hiv_summary_5 = MySqlOperator(
    task_id='build_hiv_summary_5',
    sql='call generate_flat_hiv_summary_vitals("build",15,500,1);',
    mysql_conn_id=MYSQL_CONN_ID,
    database='etl',
    dag=dag
)

rerun_trigger = TriggerDagRunOperator(
    task_id='rerun_trigger',
    trigger_dag_id=DAG_ID,
    dag=dag
)

sleep1 >> sleep3 >> build_hiv_summary_1
sleep1 >> sleep4 >> build_hiv_summary_2
sleep1 >> sleep5 >> build_hiv_summary_3
sleep1 >> sleep6 >> build_hiv_summary_4
sleep1 >> sleep7 >> build_hiv_summary_5

build_hiv_summary_1 >> sleep2
build_hiv_summary_2 >> sleep2
build_hiv_summary_3 >> sleep2
build_hiv_summary_4 >> sleep2
build_hiv_summary_5 >> sleep2

sleep2 >> rerun_trigger

if __name__ == "__main__":
    dag.cli()