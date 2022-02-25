from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.operators.python import PythonOperator
from airflow.operators.python import PythonVirtualenvOperator

def extract(**kwargs):
    print('currently inside extract task')

    import json

    query = "Select * from MSP"
    mysql_hook = MySqlHook(schema='HTHK', mysql_conn_id='mysql_connection')
    connection = mysql_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(query)

    raw_data = cursor.fetchall()
    raw_columns_name = [item[0] for item in cursor.description]

    data = json.dumps(raw_data)
    columns_name = json.dumps(raw_columns_name)

    ti = kwargs['ti']
    ti.xcom_push('rdb_data', data)
    ti.xcom_push('columns_name', columns_name)


def transform(xcom_data, xcom_columns_name, **kwargs):
    print('currently inside etl task')

    import pandas as pd
    import json

    data = json.loads(xcom_data)
    columns_name = json.loads(xcom_columns_name)

    df = pd.DataFrame(data, columns=columns_name)

    basic_info = df[['AC', 'LAST_NAME', 'FIRST_NAME', 'HKID', 'SEX', 'AGE_BAND']]
    basic_info['AGE_GRP'] = basic_info['AGE_BAND'].map({
        'band_01': '18-22', 'band_02': '22-30', 'band_03': '30-45', 'band_04': '45-60', 'band_05': '60 or above'})
    basic_info.drop(columns=['AGE_BAND'], inplace=True)
    basic_info_json = json.dumps(basic_info.to_dict('records'))

    ti = kwargs['ti']
    ti.xcom_push('basic_info', basic_info_json)

def load(**kwargs):
    print('currently inside insert_data task')

    import json

    ti = kwargs['ti']
    raw_basic_info = ti.xcom_pull(task_ids='transform', key='basic_info')
    basic_info = json.loads(raw_basic_info)

    mongo_hook = MongoHook("mongo_connection")
    connection = mongo_hook.get_conn()
    db = connection['HTHK_test']
    collection = db['test']

    collection.insert_many(basic_info)

    result = collection.find({}).limit(2)
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
    'etl_testing',
    default_args=default_args,
    description='',
    schedule_interval='@once',
    start_date=datetime(2022, 2, 22),
    catchup=False,
) as dag:

    task1 = PythonOperator(task_id='extract', python_callable=extract)
    task2 = PythonVirtualenvOperator(
        task_id='transform',
        python_callable=transform,
        requirements=['pandas'],
        op_kwargs={"xcom_data": "{{ti.xcom_pull(task_ids='extract', key='rdb_data')}}",
                   "xcom_columns_name": "{{ ti.xcom_pull(task_ids='extract', key='columns_name') }}"},
        do_xcom_push=True,
    )
    task3 = PythonOperator(task_id='load', python_callable=load)

    task1 >> task2 >> task3
