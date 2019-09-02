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
## TRIGGER RULES
ONE_SUCCESS = 'one_success'

## MYSQL CONNECTION
MYSQL_CONN_ID = 'amrs_slave_conn'

## DAG ID
DAG_ID = 'base_tables'
### END TRIGGER RULES ###

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['fali@ampath.or.ke'],
    'email_on_failure': True,
    'email_on_retry': True,
    'start_date': '2019-06-24',
    'retries': 3,
    'retry_delay': timedelta(seconds=30),
}



dag = DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    catchup=False,
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=60),
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
    sql='flat_lab_obs_v1.7.sql',
    mysql_conn_id=MYSQL_CONN_ID,
    database='etl',
    dag=dag
)


finito = DummyOperator(
    task_id='finito',
    dag=dag
)


rerun_trigger = TriggerDagRunOperator(
    task_id='rerun_trigger',
    trigger_dag_id=DAG_ID,
    dag=dag
)


update_flat_obs  >>  finito
update_flat_lab_obs >> finito
update_flat_orders  >> finito



finito >> rerun_trigger

if __name__ == "__main__":
    dag.cli()
