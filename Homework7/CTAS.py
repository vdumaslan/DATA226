from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import datetime
import snowflake.connector
import requests
import logging

# Initialize the logger
logger = logging.getLogger(__name__)

def return_snowflake_conn():

    # Initialize the SnowflakeHook
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    
    # Execute the query and fetch results
    return hook.get_conn()

@task
def run_ctas():
    conn = return_snowflake_conn()
    cur = conn.cursor()

    try:
        cur.execute("BEGIN;")
        cur.execute("""
            CREATE OR REPLACE TABLE dev.analytics.session_summary AS
                SELECT u.*, s.ts
                FROM dev.raw_data.user_session_channel u
                JOIN dev.raw_data.session_timestamp s ON u.sessionId=s.sessionId;
            """)
        cur.execute("COMMIT;")

    except Exception as e:
        cur.execute("ROLLBACK;")
        print(e)
        raise e
  
    finally:
        cur.close()
        conn.close()

@task
def check_duplicates():
    conn = return_snowflake_conn()
    cur = conn.cursor()

    try:
        # Checking for duplicates
        cur.execute("""
            SELECT COUNT(*) AS cnt
            FROM dev.analytics.session_summary;
            """)
        total_count = cur.fetchone()[0]

        cur.execute("""
            SELECT COUNT(*)
            FROM (
                SELECT DISTINCT *
                FROM dev.analytics.session_summary
            );
            """)
        distinct_count = cur.fetchone()[0]

        # Compare counts to determine if duplicates exist
        if total_count == distinct_count:
            logger.info("No duplicates found.")

    except Exception as e:
        cur.execute("ROLLBACK;")
        print(e)
        raise e
  
    finally:
        cur.close()
        conn.close()
  

with DAG(
    dag_id = 'BuildSummary',
  start_date = datetime(2024,10,20),
    catchup=False,
    tags=['ELT'],
    schedule = '0 11 * * *'
) as dag:
  
    # Tasks
    run_ctas() >> check_duplicates()