from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import datetime
import snowflake.connector
import requests

def return_snowflake_conn():

    # Initialize the SnowflakeHook
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    
    # Execute the query and fetch results
    return hook.get_conn()

@task
def set_stage():
    create_user_session_channel = """
        CREATE TABLE IF NOT EXISTS dev.raw_data.user_session_channel (
            userId int not NULL,
            sessionId varchar(32) primary key,
            channel varchar(32) default 'direct'  
    );
    """

    create_session_timestamp = """
        CREATE TABLE IF NOT EXISTS dev.raw_data.session_timestamp (
            sessionId varchar(32) primary key,
            ts timestamp  
    );
    """
    
    # for the following query to run, 
    # the S3 bucket should have LIST/READ privileges for everyone
    create_stage = """
        CREATE OR REPLACE STAGE dev.raw_data.blob_stage
        url = 's3://s3-geospatial/readonly/'
        file_format = (type = csv, skip_header = 1, field_optionally_enclosed_by = '"');
    """

    populate_user_session_channel = """
        COPY INTO dev.raw_data.user_session_channel
        FROM @dev.raw_data.blob_stage/user_session_channel.csv;
    """
    
    populate_session_timestamp = """
        COPY INTO dev.raw_data.session_timestamp
        FROM @dev.raw_data.blob_stage/session_timestamp.csv;
    """

    stage_results = create_user_session_channel, create_session_timestamp, create_stage, populate_user_session_channel, populate_session_timestamp
    return stage_results

@task
def load(results):
    conn = return_snowflake_conn()
    cur = conn.cursor()

    try:
        cur.execute("BEGIN;")
        cur.execute(results[0])
        cur.execute(results[1])
        cur.execute(results[2])
        cur.execute(results[3])
        cur.execute(results[4])
        cur.execute("COMMIT;")

    except Exception as e:
        cur.execute("ROLLBACK;")
        print(e)
        raise e
  
    finally:
        cur.close()
        conn.close()
  

with DAG(
    dag_id = 'SessionToSnowflake',
    start_date = datetime(2024,10,20),
    catchup=False,
    tags=['ETL'],
    schedule = '0 10 * * *'
) as dag:
  
    # Tasks
    stage_results = set_stage()
    load(stage_results)