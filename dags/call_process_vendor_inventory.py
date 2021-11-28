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

# extract variables

file_name="{{ dag_run.conf['message'] }}"
vendor_num="{{ dag_run.conf['vendor_num'] }}"
file_type="{{ dag_run.conf['file_type'] }}"
file_date="{{ dag_run.conf['file_date'] }}"

def master_or_detail(**kwargs):
    logging.info('Type of dag run conf file type is: {}'.format(type(file_type)))
    logging.info('upper: {}'.format(file_type.upper()))
    logging.info('lower: {}'.format(file_type.lower()))
    logging.info('original: {}'.format(file_type))
    sample_file_type = kwargs['dag_run'].conf.get('file_type')
    #FILE_TYPE = {{ dag_run.conf['file_type'] }}
    #logging.info('new format: {}'.format(FILE_TYPE))
    if file_type.lower() == 'master':
        return "master_branch_start"
    elif sample_file_type == 'master':
        return "master_branch_start"
    else:
        return "detail_branch_start"

print("vendor_num: {}".format(vendor_num))
print("file_type: {}".format(file_type))

BEGIN_CMD = "BEGIN TRANSACTION;"
DELETE_PREV_LAND_RECS="DELETE FROM AIRFLOW_POC.LAND_SALES_OPS_INVENTORY_" + file_type + " WHERE VENDOR_ID = '" + vendor_num + "';"
#COPY_CMD = "copy into AIRFLOW_POC.LAND_SALES_OPS_INVENTORY_MASTER from s3://" + file_name + " storage_integration = vendor_inventory_s3_dev file_format = (format_name = my_csv_format, trim_space=true) FORCE=TRUE"
COPY_CMD = (
        "copy into AIRFLOW_POC.LAND_SALES_OPS_INVENTORY_"
        + file_type
        + " from s3://"
        + file_name
        + " storage_integration = vendor_inventory_s3_dev file_format = (format_name = my_csv_format, trim_space=true) FORCE=TRUE;"
)
UPD_VEND_CMD = "update AIRFLOW_POC.LAND_SALES_OPS_INVENTORY_" + file_type + " set VENDOR_ID = '" + vendor_num + "' where VENDOR_ID is null;"
DEL_FROM_SALES_OPS = "DELETE FROM AIRFLOW_POC.SALES_OPS_INVENTORY_" + file_type + " WHERE VENDOR_ID = '" + vendor_num + "' AND INVENTORY_UPLOAD_DATE = '" + file_date + "';"
INSERT_IN_SALES_OPS = "INSERT INTO AIRFLOW_POC.SALES_OPS_INVENTORY_" + file_type + " SELECT *, '" + vendor_num +"' FROM AIRFLOW_POC.LAND_SALES_OPS_INVENTORY_" + file_type + " WHERE VENDOR_ID = '"+ vendor_num + "';" 
COMMIT_CMD= "COMMIT;"

cmd_list = []
cmd_list.append(BEGIN_CMD)
cmd_list.append(DELETE_PREV_LAND_RECS)
cmd_list.append(COPY_CMD)
cmd_list.append(UPD_VEND_CMD)
cmd_list.append(DEL_FROM_SALES_OPS)
cmd_list.append(INSERT_IN_SALES_OPS)
cmd_list.append(COMMIT_CMD)

#ONE_SQL_CMD = BEGIN_CMD + DELETE_PREV_LAND_RECS + COPY_CMD + UPD_VEND_CMD + DEL_FROM_SALES_OPS + INSERT_IN_SALES_OPS + COMMIT_CMD
#print(COPY_CMD)
file_name1 = ""
def set_vars(dag_run, **context):
    global file_name1
    file_name1 = dag_run.conf['message']
    context['ti'].xcom_push(key='file-name-1', value=file_name1)
    logging.info(file_name1)
    print(file_name1)
#    vendor_num={{ dag_run.conf['vendor_num'] }}
#    print(vendor_num)
#    file_type={{ dag_run.conf['file_type'] }}.upper()
#    file_date={{ dag_run.conf['file_date'] }}

def print_vars(**context):
    global file_name1
    file_name2 = context['ti'].xcom_pull(key='file-name-1')
    logging.info('file name 1: {}'.format(file_name1))
    logging.info('ifle name 2: {}'.format(file_name2))
    print("file_name1: " + file_name1)
    print("file_name2: " + file_name2)

dag = DAG(
    'call_process_vendor_inventory_old',
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    default_args={'message': 'a/b/c/d/e/f.csv', 'snowflake_conn_id': SNOWFLAKE_CONN_ID},
    tags=['axiom'],
    catchup=False,
)

branch_task = BranchPythonOperator(
        task_id="File_Type",
        python_callable=master_or_detail,
#        python_callable=lambda f: "master_branch_start" if file_type == "master" else "detail_branch_start",
        provide_context=True,
        dag=dag)

master_branch_start=DummyOperator(task_id='master_branch_start')
detail_branch_start=DummyOperator(task_id='detail_branch_start')
end_task = DummyOperator(task_id='end_task', trigger_rule='one_success')
branch_task >> master_branch_start >> end_task
branch_task >> detail_branch_start >> end_task

set_vars_task = PythonOperator(task_id="Set_Variables", python_callable=set_vars, dag=dag)

print_vars_task = PythonOperator(task_id="Print_Variables", python_callable=print_vars, dag=dag)

snowflake_op_sql_str = SnowflakeOperator(
    task_id='snowflake_op_sql_str',
    dag=dag,
    sql=cmd_list,
#    sql="copy into AIRFLOW_POC.LAND_SALES_OPS_INVENTORY_{0} from s3://{1} storage_integration = vendor_inventory_s3_dev file_format = (format_name = my_csv_format, trim_space=true) FORCE=TRUE".format(file_type,file_name),
    warehouse=SNOWFLAKE_WAREHOUSE,
    database=SNOWFLAKE_DATABASE,
)

file_loaded = PublishSnsMsg('sns_msg', file_name,'File loaded to Snowflake')



set_vars_task >> print_vars_task >>  snowflake_op_sql_str >> file_loaded
