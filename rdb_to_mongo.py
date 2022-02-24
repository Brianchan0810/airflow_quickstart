from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.operators.python import PythonOperator

def connection_to_rdb():
    query = 'SELECT * FROM EASY_CALL_RESULT'
    mysql_hook = MySqlHook(schema='HTHK', connection='mysql_connection')
    cursor = mysql_hook.get_conn()
    cursor.execute(query)
    result = cursor.fetchall()
    for row in result:
        print(row)
    for row in result:
        print('col1: {0}, col2: {1}'.format(row[0], row[1]))

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}
with DAG(
    'rdb_mongdb_testing',
    default_args=default_args,
    description='',
    schedule_interval='@once',
    start_date=datetime(2022, 2, 22),
    catchup=False,
) as dag:

    task1 = PythonOperator(task_id='task1', python_callable=connection_to_rdb)

    task1
