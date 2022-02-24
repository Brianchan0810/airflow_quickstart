from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.operators.python import PythonOperator

def connection_to_rdb():
    print('currently inside connection_to_rdb task')
    query = 'SELECT * FROM EASY_CALL_RESULT'
    mysql_hook = MySqlHook(schema='HTHK', mysql_conn_id='mysql_connection')
    connection = mysql_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(query)
    result = cursor.fetchall()
    for row in result:
        print(row)

def connection_to_mongo():
    print('currently inside connection_to_mongo task')
    mongo_hook = MongoHook("mongo_connection")
    connection = mongo_hook.get_conn()
    db = connection['HTHK_test']
    collection = db['test']
    result = collection.find({})
    for item in result:
        print(item)


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
    task2 = PythonOperator(task_id='task2', python_callable=connection_to_mongo)

    task1 >> task2
