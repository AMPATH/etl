from builtins import range
from datetime import timedelta
from datetime import datetime
import logging
import math

import airflow
from airflow.models import DAG
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.hooks.mysql_hook import MySqlHook
from airflow.operators.dagrun_operator import TriggerDagRunOperator


from airflow.models import Connection
from airflow import settings


class CustomMySqlOperator(MySqlOperator):
    def execute(self, context):
        self.log.info('Executing: %s', self.sql)
        hook = MySqlHook(mysql_conn_id=self.mysql_conn_id,
                         schema=self.database)
        return hook.get_records(
            self.sql,
            parameters=self.parameters)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['fali@ampath.or.ke'],
    'email_on_failure': False,
    'email_on_retry': True,
    'email_on_success': True,
    'start_date': datetime.now(),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}



dag = DAG(
    dag_id='sync_only_hiv_summary',
    default_args=default_args,
    schedule_interval= None,
    dagrun_timeout=timedelta(minutes=60),
    template_searchpath=[
    '/usr/local/airflow/etl-scripts/flat_tables', 
    '/usr/local/airflow/etl-scripts/calculated_tables', 
    '/usr/local/airflow/etl-scripts/database_updates',
    '/usr/local/airflow/email-templates'
    ]
)


add_patients_to_queue = MySqlOperator(
    task_id='add_patients_to_queue',
    sql='add_patients_to_hiv_summary_build_queue.sql',
    mysql_conn_id='slave_conn',
    database='etl',
    dag=dag
)


sync_hiv_summary = CustomMySqlOperator(
     task_id='sync_hiv_summary',
     sql='CALL `generate_hiv_summary_v15_8`("build",1,1000,100);',
     mysql_conn_id='slave_conn',
     database='etl',
     dag=dag
)

add_patients_to_queue >> sync_hiv_summary
