from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from airflow.exceptions import AirflowFailException
from airflow.hooks.base import BaseHook
from airflow.models import Variable
import datetime
import json
import requests

DBT_API = BaseHook.get_connection('dbt_api').host
DBT_API_KEY = Variable.get('dbt_cloud_api_key_colin')
DBT_ACCOUNT_ID = Variable.get('dbt_account_id')
default_args = {
	'start_date': datetime.datetime(2021,1,1)
}

dbt_header = {
  'Content-Type': 'application/json',
  'Authorization': 'Token {}'.format(DBT_API_KEY)
}

def get_dbt_message_with_override(message, override_statement):
    return {
        'cause': message,
        'steps_override': [override_statement]
    }


def trigger_dbt_job_with_override(job_id, account_id=DBT_ACCOUNT_ID, override_statement='',  message='Triggered by Airflow.', **context):
    response = requests.post(
        f'{DBT_API}/accounts/{account_id}/jobs/{job_id}/run/',
        headers=dbt_header,
        data=json.dumps(get_dbt_message_with_override(message, override_statement))
    ).json()
    status_code = response['status']['code']
    job_run_id = response['data']['id']
    job_run_data = json.dumps({'status': status_code, 'job_run_id': job_run_id})
    context['ti'].xcom_push(key=f'job-{job_id}', value=job_run_data)


def trigger_dbt_job_operator_with_override(task_id, job_id, override_statement, account_id=DBT_ACCOUNT_ID, message='Triggered by Airflow.'):
    return PythonOperator(
        task_id=task_id,
        python_callable=trigger_dbt_job_with_override,
        op_kwargs={
            'job_id': job_id,
            'account_id': account_id,
            'message': message,
            'override_statement': override_statement
        }
    )


def dbt_wait_job_run(job_id, account_id=DBT_ACCOUNT_ID, **context):
    job_run_data = json.loads(context['ti'].xcom_pull(key=f'job-{job_id}'))
    job_run_id = job_run_data['job_run_id']
    response = requests.get(
        f'{DBT_API}/accounts/{account_id}/runs/{job_run_id}/',
        headers=dbt_header
    ).json()
    status = response['data']['in_progress']
    is_cancelled = response['data']['is_cancelled']
    is_success = response['data']['is_success']
    is_error = response['data']['is_error']
    if status == False:
        if is_success == True:
            return True
        elif is_error == True:
            raise AirflowFailException('Error on DBT job run: {}'.format(job_run_id))
        elif is_cancelled == True:
            raise AirflowFailException('DBT job run cancelled for id: {}'.format(job_run_id))


def dbt_wait_job_run_operator(task_id, job_id, interval=30, retries=20, account_id=DBT_ACCOUNT_ID):
    return PythonSensor(
        task_id=task_id,
        poke_interval=interval,
        timeout=(interval * retries),
        python_callable=dbt_wait_job_run,
        op_kwargs={'job_id': job_id, 'account_id': account_id}
    )


