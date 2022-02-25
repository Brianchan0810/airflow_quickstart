from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.operators.python import PythonOperator
from airflow.operators.python import PythonVirtualenvOperator

def etl():
    import pandas as pd
    import pprint

    print('currently inside etl task')

    #read data from RDB
    query = "Select * from MSP"
    mysql_hook = MySqlHook(schema='HTHK', mysql_conn_id='mysql_connection')
    connection = mysql_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(query)

    data = cursor.fetchall()
    columns_name = [item[0] for item in cursor.description]

    #ETL
    df = pd.DataFrame(data, columns=columns_name)

    basic_info = df[['AC', 'LAST_NAME', 'FIRST_NAME', 'HKID', 'SEX', 'AGE_BAND']]
    basic_info['AGE_GRP'] = basic_info['AGE_BAND'].map({
        'band_01': '18-22', 'band_02': '22-30', 'band_03': '30-45', 'band_04': '45-60', 'band_05': '60 or above'})
    basic_info.drop(columns=['AGE_BAND'], inplace=True)
    basic_info_dict = basic_info.to_dict('records')

    #Insert to MongoDB
    mongo_hook = MongoHook("mongo_connection")
    connection = mongo_hook.get_conn()
    db = connection['HTHK_test']
    collection = db['test']
    collection.insert_many(basic_info_dict)

    result = collection.find({})
    for item in result:
        pprint.pprint(item)


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
    'rdb_mongdb_testing2',
    default_args=default_args,
    description='',
    schedule_interval='@once',
    start_date=datetime(2022, 2, 22),
    catchup=False,
) as dag:

    task1 = PythonVirtualenvOperator(task_id='my_etl', python_callable=etl, requirements=['pandas'])

    task1
