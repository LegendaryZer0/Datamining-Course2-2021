default_args = {
'owner': 'admin',
'depends_on_past': False,
'email': ['airflow@example.com'],
'email_on_failure': False,
'email_on_retry': False,
'retries': 1,
'retry_delay': timedelta(minutes=1),

}
dag = DAG(
'cleandb_dag',
default_args=default_args,
description='A simple tutorial DAG',
schedule_interval=None,
start_date=days_ago(2),
tags=['vk_api'],
)


BashOperator(
task_id='cleandb_task',
bash_command='python3 CleanDbScript.py mydatabase.cdwb7v1ldkf1.us-east-1.rds.amazonaws.com postgres qwerty016 postgres 5432 ',
dag=dag)