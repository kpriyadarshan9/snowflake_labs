# The pipeline_execution function orchestrates the extraction, validation, and loading of data.
# Error Handling and Logging: If an error occurs during any step, the error message is logged
# in the error_log_table using the log_error_to_table function.
# The DAG is scheduled to run daily.
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from src.connection_util import get_snowflake_session, close_snowflake_session
from src.extract_stage_data import extract_data_from_stage
from src.validate_stage_data import validate_stage_data
from src.load_target_data import load_data_into_target

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1
}

dag = DAG(
    'snowflake_data_pipeline',
    default_args=default_args,
    description='ETL pipeline to extract, validate, and load data into Snowflake',
    schedule_interval='@daily'  # Runs daily
)


def pipeline_execution():
    session = get_snowflake_session()
    try:
        # Step 1: Extract
        extract_data_from_stage(session)

        # Step 2: Validate
        validate_stage_data(session)

        # Step 3: Load
        load_data_into_target(session)
    except Exception as e:
        log_error_to_table(session, str(e))
    finally:
        close_snowflake_session(session)


def log_error_to_table(session, error_message):
    """
    Logs error messages into a Snowflake log table.
    """
    query = f"""
    INSERT INTO error_log_table (error_message, error_time)
    VALUES ('{error_message}', CURRENT_TIMESTAMP());
    """
    session.sql(query).collect()


# Airflow task
run_pipeline = PythonOperator(
    task_id='run_snowflake_pipeline',
    python_callable=pipeline_execution,
    dag=dag
)

run_pipeline
