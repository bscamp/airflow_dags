"""
Example use of Snowflake related operators.
"""
from datetime import datetime

from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.transfers.s3_to_snowflake import S3ToSnowflakeOperator
from airflow.providers.snowflake.transfers.snowflake_to_slack import SnowflakeToSlackOperator

SNOWFLAKE_CONN_ID = 'snowflake_conn'
SNOWFLAKE_SCHEMA = 'AMPLIFY'
SNOWFLAKE_WAREHOUSE = 'LOOKER_WH'
SNOWFLAKE_DATABASE = 'DEV'
SNOWFLAKE_ROLE = 'AMP_DBA'
SNOWFLAKE_SAMPLE_TABLE = 'sample_airflow_table'

# SQL commands
CREATE_TABLE_SQL_STRING = (
    f"CREATE OR REPLACE TRANSIENT TABLE {SNOWFLAKE_SAMPLE_TABLE} (name VARCHAR(250), id INT);"
)

dag = DAG(
    'ronaks_example_snowflake',
    start_date=datetime(2021, 1, 1),
    default_args={'snowflake_conn_id': SNOWFLAKE_CONN_ID},
    tags=['example'],
    catchup=False,
)

# [START howto_operator_snowflake]

snowflake_op_sql_str = SnowflakeOperator(
    task_id='snowflake_op_sql_str',
    dag=dag,
    sql=CREATE_TABLE_SQL_STRING,
    warehouse=SNOWFLAKE_WAREHOUSE,
    database=SNOWFLAKE_DATABASE,
    schema=SNOWFLAKE_SCHEMA,
    role=SNOWFLAKE_ROLE,
)
