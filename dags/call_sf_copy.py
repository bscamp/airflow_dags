# more examples can be found at 
# https://github.com/apache/airflow/blob/main/airflow/providers/snowflake/example_dags/example_snowflake.py

from datetime import datetime

from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.transfers.s3_to_snowflake import S3ToSnowflakeOperator
from airflow.providers.snowflake.transfers.snowflake_to_slack import SnowflakeToSlackOperator
from utils.publish_sns_message import *

SNOWFLAKE_CONN_ID = 'snowflake_conn'
SNOWFLAKE_SCHEMA = 'AMPLIFY'
SNOWFLAKE_WAREHOUSE = 'LOOKER_WH'
SNOWFLAKE_DATABASE = 'DEV'
SNOWFLAKE_ROLE = 'AMP_DBA'
SNOWFLAKE_SAMPLE_TABLE = 'DROP_SALES_OPS_INVENTORY_MASTER'

# SQL commands

file_name="{{ dag_run.conf['message'] }}"

vars=file_name.split('/')
#vendor_num=vars[len(vars)-2]
#file_type=vars[len(vars)-3]

#print("vendor_num: {}".format(vendor_num))
#print("file_type: {}".format(file_type))

COPY_CMD = "copy into amplify.drop_SALES_OPS_INVENTORY_MASTER from s3://" + file_name + " storage_integration = vendor_inventory_s3_dev file_format = (format_name = my_csv_format, trim_space=true) FORCE=TRUE;"
print(COPY_CMD)

dag = DAG(
    'call_snowflake_copy',
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    default_args={'message': 'a/b/c/d/e/f/xyz.csv'},
    tags=['example'],
    catchup=False,
)

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
