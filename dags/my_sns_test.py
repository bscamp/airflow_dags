import datetime

from airflow import DAG
from utils.publish_sns_message import *

default_args = {
  'start_date': datetime.datetime(2021,1,1)
}

msg="{{ dag_run.conf['message'] }}"

with DAG('call_sns_test',
  schedule_interval=None,
  default_args=default_args,
  catchup=False) as dag:
    task_1 = PublishSnsMsg('task_1', msg,'Testing SNS')
