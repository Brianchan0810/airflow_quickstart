from datetime import datetime, timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
#from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonVirtualenvOperator

def pandas_test():
    import pandas as pd
    from datetime import datetime

    name = ['Tom', 'May', 'Sam', 'John']
    id = [i for i in range(4)]
    spend = [200, 400, 100, 600]

    df = pd.DataFrame({'id':id, 'name':name, 'expenditure':spend})
    df['date'] = datetime.now().date()
    print(df)

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
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
    'brian_test_PyVirOperator',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 2, 18),
    catchup=False,
    tags=['test_for_PyVirOperator'],
) as dag:

    # t1, t2 and t3 are examples of tasks created by instantiating operators
    t1 = PythonVirtualenvOperator(
        task_id='task1',
        python_callable=pandas_test,
        requirements=[
            "pandas",
            "datetime"
        ],
        system_site_packages=False,
    )

    t1

