from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from airflow.exceptions import AirflowFailException
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from utils.dbt_functions import *
import datetime
import json
import requests

DBT_ACCOUNT_ID = Variable.get('dbt_account_id')
DBT_JOB_ID = 42906
default_args = {
	'start_date': datetime.datetime(2021,1,1)
}

job_run_date = datetime.date.today()
begin_run_date = job_run_date - datetime.timedelta(days=7)
date_json = {'run_date': str(job_run_date), 'begin_run_date': str(begin_run_date)}
dbt_override_statement = 'dbt run --select reading.reading_user --vars "{}"'.format(date_json)


with DAG(
    'call_reading_district_by_user_wk',
    schedule_interval='@daily',
    default_args=default_args,
    catchup=False,
    tags=["reading", "axiom", "reports", "reading-weekly-rollups", "rollups"]
) as dag:
    load_reading_district_by_user_wk = trigger_dbt_job_operator_with_override(
        task_id='reading_district_by_user_wk',
        job_id=DBT_JOB_ID,
        account_id=DBT_ACCOUNT_ID,
        override_statement=dbt_override_statement
    )
    check_dbt_job_status = dbt_wait_job_run_operator(
        task_id='check_dbt_job_status',
        job_id=DBT_JOB_ID,
        interval=60,
        retries=60,
        account_id=DBT_ACCOUNT_ID
    )
    load_reading_district_by_user_wk >> check_dbt_job_status

