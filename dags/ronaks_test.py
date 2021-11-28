from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
default_args = {'start_date': datetime(2021,1,1)}
def test():
    print('Success')

with DAG('test_dag', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:
    task_1 = PythonOperator(task_id='task_1', python_callable=test)
