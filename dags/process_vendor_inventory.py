# more examples can be found at 
# https://github.com/apache/airflow/blob/main/airflow/providers/snowflake/example_dags/example_snowflake.py

from datetime import datetime

from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.transfers.s3_to_snowflake import S3ToSnowflakeOperator
#from airflow.providers.snowflake.transfers.snowflake_to_slack import SnowflakeToSlackOperator
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from utils.publish_sns_message import *

# snowflake settings
SNOWFLAKE_CONN_ID = 'snowflake_conn'
#SNOWFLAKE_SCHEMA = 'AMPLIFY'
SNOWFLAKE_WAREHOUSE = 'LOOKER_WH'
SNOWFLAKE_DATABASE = 'DEV'
#SNOWFLAKE_ROLE = 'AMP_DBA'
#SNOWFLAKE_SAMPLE_TABLE = 'DROP_SALES_OPS_INVENTORY_MASTER'


# extract variables

file_name="{{ dag_run.conf['message'] }}"

vendor_num="{{ dag_run.conf['vendor_num'] }}"
file_type="{{ dag_run.conf['file_type'] }}"
file_date="{{ dag_run.conf['file_date'] }}"

def master_or_detail():
    if file_type.lower() == 'master':
        return "master_branch_start"
    else:
        return "detail_branch_start"

print("vendor_num: {}".format(vendor_num))
print("file_type: {}".format(file_type))

COPY_CMD = "copy into amplify.drop_SALES_OPS_INVENTORY_{} from s3://" + file_name + " storage_integration = vendor_inventory_s3_dev file_format = (format_name = my_csv_format, trim_space=true) FORCE=TRUE;".format(file_type.upper())
DELETE_CMD = "DELETE FROM ZZ_LAND.SALES_OPS_INVENTORY_{} WHERE VENDOR_ID = '{}'".format(file_type.upper(),vendor_num)
UPD_VEND_CMD = "update ZZ_LAND.SALES_OPS_INVENTORY_{} set VENDOR_ID = '{}' where VENDOR_ID is null".format(file_type.upper(),vendor_num)
DEL_FROM_SALES_OPS = "DELETE FROM SALESOPS.SALES_OPS_INVENTORY_{} WHERE VENDOR_ID = '{}' AND INVENTORY_UPLOAD_DATE = '{}'".format(file_type.upper(),vendor_num,file_date)
print(COPY_CMD)

dag = DAG(
    'process_vendor_inventory',
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    default_args={'message': 'a/b/c/d/e/f.csv', 'snowflake_conn_id': SNOWFLAKE_CONN_ID},
    tags=['axiom'],
    catchup=False,
)

branch_task = BranchPythonOperator(
        task_id="File_Type",
        python_callable=master_or_detail,
        dag=dag)

master_branch_start=DummyOperator(task_id='master_branch_start')
detail_branch_start=DummyOperator(task_id='detail_branch_start')
end_task = DummyOperator(task_id='end_task', trigger_rule='one_success')
branch_task >> master_branch_start >> end_task
branch_task >> detail_branch_start >> end_task
#copy_into_table = S3ToSnowflakeOperator(
#    task_id='copy_into_table',
#    s3_keys="s3://"+"{{ dag_run.conf['message'] }}",
#    table=SNOWFLAKE_SAMPLE_TABLE,
#    schema=SNOWFLAKE_SCHEMA,
#    stage="vendor_inventory_s3_dev_stage",
#    file_format="(type = 'CSV',field_delimiter = ';')",
#    dag=dag,
#)
# [START howto_operator_snowflake]

snowflake_op_sql_str = SnowflakeOperator(
    task_id='snowflake_op_sql_str',
    dag=dag,
    sql=COPY_CMD,
    warehouse=SNOWFLAKE_WAREHOUSE,
    database=SNOWFLAKE_DATABASE,
#    schema=SNOWFLAKE_SCHEMA,
#    role=SNOWFLAKE_ROLE,
)

file_loaded = PublishSnsMsg('sns_msg', file_name,'File loaded to Snowflake')

snowflake_op_sql_str >> file_loaded
