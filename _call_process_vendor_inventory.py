# more examples can be found at 
# https://github.com/apache/airflow/blob/main/airflow/providers/snowflake/example_dags/example_snowflake.py

from datetime import datetime

from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.transfers.s3_to_snowflake import S3ToSnowflakeOperator
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.operators.python_operator import BranchPythonOperator,PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from utils.publish_sns_message import *
import logging

# snowflake settings
SNOWFLAKE_CONN_ID = 'snowflake_conn'
SNOWFLAKE_WAREHOUSE = 'LOOKER_WH'
SNOWFLAKE_DATABASE = 'DEV'
file_name = '' #calling an empty string until we figure out value of string in dag.

def master_or_detail(**kwargs):
    file_type = kwargs['dag_run'].conf.get('file_type')
    logging.info("file_type: {}".format(file_type))
    if file_type.lower() == 'master':
        return "master_branch_start"
    else:
        return "detail_branch_start"


def set_vars(dag_run, **context):
    file_name = dag_run.conf['message']
    context['ti'].xcom_push(key='file-name', value=file_name)
    logging.info("file_name: {}".format(file_name))
    vendor_num=dag_run.conf['vendor_num']
    context['ti'].xcom_push(key='vendor-num', value=vendor_num)
    logging.info("vendor_num: {}".format(vendor_num))
    file_type = dag_run.conf['file_type']
    context['ti'].xcom_push(key='file-type', value=file_type)
    logging.info("file_type: {}".format(file_type))
    file_date=dag_run.conf['file_date']
    context['ti'].xcom_push(key='file-date', value=file_date)
    logging.info("file_date: {}".format(file_date))


def print_vars(**context):
    file_name = context['ti'].xcom_pull(key='file-name')
    logging.info('file name: {}'.format(file_name))


def snowflake_load_file(**context):
    file_name = context['ti'].xcom_pull(key='file-name')
    vendor_num = context['ti'].xcom_pull(key='vendor-num')
    file_type = context['ti'].xcom_pull(key='file-type')
    file_date = context['ti'].xcom_pull(key='file-date')
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute('BEGIN;')
    cursor.execute(
        "DELETE FROM AIRFLOW_POC.LAND_SALES_OPS_INVENTORY_"
        + file_type
        + " WHERE VENDOR_ID = '"
        + vendor_num
        + "';"
    )
    cursor.execute(
        "copy into AIRFLOW_POC.LAND_SALES_OPS_INVENTORY_"
        + file_type
        + " from s3://"
        + file_name
        + " storage_integration = vendor_inventory_s3_dev file_format = (format_name = my_csv_format, trim_space=true) FORCE=TRUE;"
    )
    cursor.execute(
        "update AIRFLOW_POC.LAND_SALES_OPS_INVENTORY_"
        + file_type
        + " set VENDOR_ID = '"
        + vendor_num
        + "' where VENDOR_ID is null;"
    )
    cursor.execute(
        "DELETE FROM AIRFLOW_POC.SALES_OPS_INVENTORY_"
        + file_type
        + " WHERE VENDOR_ID = '"
        + vendor_num
        + "' AND INVENTORY_UPLOAD_DATE = '"
        + file_date
        + "';"
    )
    cursor.execute(
        "INSERT INTO AIRFLOW_POC.SALES_OPS_INVENTORY_"
        + file_type
        + " SELECT *, '"
        + vendor_num
        + "' FROM AIRFLOW_POC.LAND_SALES_OPS_INVENTORY_"
        + file_type
        + " WHERE VENDOR_ID = '"
        + vendor_num
        + "';"
    )
    cursor.close()

dag = DAG(
    'call_process_vendor_inventory',
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    default_args={'message': 'a/b/c/d/e/f.csv', 'snowflake_conn_id': SNOWFLAKE_CONN_ID},
    tags=['axiom'],
    catchup=False,
)

branch_task = BranchPythonOperator(
        task_id="File_Type",
        python_callable=master_or_detail,
        provide_context=True,
        dag=dag)

master_branch_start=DummyOperator(task_id='master_branch_start')
detail_branch_start=DummyOperator(task_id='detail_branch_start')
end_task = DummyOperator(task_id='end_task', trigger_rule='one_success')
branch_task >> master_branch_start >> end_task
branch_task >> detail_branch_start >> end_task

set_vars_task = PythonOperator(task_id="Set_Variables", python_callable=set_vars, provide_context=True, dag=dag)

print_vars_task = PythonOperator(task_id="Print_Variables", python_callable=print_vars, provide_context=True, dag=dag)

snowflake_load_tables = PythonOperator(
        task_id='sample_file_test',
        python_callable=snowflake_load_file
)
file_loaded = PublishSnsMsg('sns_msg', "{{ dag_run.conf['message'] }}", 'File Loaded to Snowflake')

set_vars_task >> print_vars_task >> snowflake_load_tables >> file_loaded
