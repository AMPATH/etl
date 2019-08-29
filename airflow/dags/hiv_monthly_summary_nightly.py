from datetime import datetime
from pytz import timezone

import airflow
from airflow.models import DAG
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.bash_operator import BashOperator

### CONSTANTS: DO NOT EDIT ###
## MYSQL CONNECTION
MYSQL_CONN_ID = 'amrs_slave_conn'

## DAG ID
DAG_ID = 'hiv_monthly_summary_daily_10pm'
### END TRIGGER RULES ###

nbo_timezone = timezone("Africa/Nairobi")
#start_date = nbo_timezone.localize(datetime.strptime('2019-06-25 20:00', '%Y-%m-%d %H:%M'))
start_date = datetime.strptime('2019-06-25', '%Y-%m-%d')

# Dag is returned by a factory method
dag = DAG(
    dag_id=DAG_ID,
    schedule_interval='30 22 * * *',
    start_date=start_date,
    catchup=False
)

update_hiv_monthly_report_dataset = MySqlOperator(
    task_id='update_hiv_monthly_report_dataset',
    sql='call generate_hiv_monthly_report_dataset_v1_4("sync",1,100000,100,"2013-01-01");',
    mysql_conn_id=MYSQL_CONN_ID,
    database='etl',
    dag=dag
)
