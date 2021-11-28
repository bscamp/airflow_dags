from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.models import Variable
import datetime, json
default_args = {
	'start_date': datetime.datetime(2021,1,1)
}

job_run_date = datetime.date.today()

dbt_header = {
  'Content-Type': 'application/json',
  'Authorization': 'Token {}'.format(Variable.get('dbt_cloud_api_key_colin'))
}

school_year = '2021-2022'
begin_year_date = '2021-07-20'
date_json = {'run_date': str(job_run_date), 'school_year': school_year, 'begin_year_date': begin_year_date}
def getDbtMessage(message):
    return {
        'cause': message,
        'steps_override': [
            'dbt run --select reading.daily_reading --vars "{}"'.format(date_json)
        ]
    }


def getDbtApiLink(jobId, accountId):
    return 'accounts/{0}/jobs/{1}/run/'.format(accountId, jobId)


def getDbtApiOperator(task_id, jobId, message='Triggered by Airflow', accountId=16804):
    return SimpleHttpOperator(
            task_id=task_id,
            method='POST',
            data=json.dumps(getDbtMessage(message)),
            http_conn_id='dbt_api',
            endpoint=getDbtApiLink(jobId, accountId),
            headers=dbt_header
        )

with DAG(
    'call_reading_es_district_by_user_dt',
    schedule_interval='@daily',
    default_args=default_args,
    catchup=False,
    tags=["reading", "axiom", "reports"]
) as dag:
    load_reading_district_by_user_wk = getDbtApiOperator('reading_es_district_by_user_dt', 43083)

