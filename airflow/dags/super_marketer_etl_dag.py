"""
Super Marketer ETL Pipeline DAG
This DAG orchestrates the extraction, transformation, and loading of customer data
for marketing analytics.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

# Import our ETL functions
from etl_functions import (
    extract_transactions_data,
    extract_users_data,
    perform_clustering,
    transform_and_load_age_count,
    transform_and_load_trans_by_day,
    transform_and_load_gender_count,
    transform_and_load_service_count,
    transform_and_load_user_geo,
    transform_and_load_stats
)

# Default arguments for all tasks
default_args = {
    'owner': 'data-engineering-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

# Define the DAG
dag = DAG(
    'super_marketer_etl_pipeline',
    default_args=default_args,
    description='Super Marketer Customer Analytics ETL Pipeline',
    schedule_interval='0 0 * * *',  # Daily at midnight
    max_active_runs=1,
    tags=['etl', 'marketing', 'customer-analytics', 'spark']
)

# Start task
start_pipeline = DummyOperator(
    task_id='start',
    dag=dag
)

# Extraction tasks (can run in parallel)
extract_transactions = PythonOperator(
    task_id='extract_transactions',
    python_callable=extract_transactions_data,
    dag=dag,
    pool='extraction_pool'
)

extract_users = PythonOperator(
    task_id='extract_users',
    python_callable=extract_users_data,
    dag=dag,
    pool='extraction_pool'
)

# Data quality checkpoint
data_extracted = DummyOperator(
    task_id='data_extracted',
    dag=dag
)

# Clustering task (depends on both extractions)
clustering = PythonOperator(
    task_id='perform_clustering',
    python_callable=perform_clustering,
    dag=dag,
    pool='transformation_pool'
)

# Transformation and loading tasks (can run in parallel after clustering)
transform_age_count = PythonOperator(
    task_id='transform_load_age_count',
    python_callable=transform_and_load_age_count,
    dag=dag,
    pool='loading_pool'
)

transform_trans_by_day = PythonOperator(
    task_id='transform_load_trans_by_day',
    python_callable=transform_and_load_trans_by_day,
    dag=dag,
    pool='loading_pool'
)

transform_gender_count = PythonOperator(
    task_id='transform_load_gender_count',
    python_callable=transform_and_load_gender_count,
    dag=dag,
    pool='loading_pool'
)

transform_service_count = PythonOperator(
    task_id='transform_load_service_count',
    python_callable=transform_and_load_service_count,
    dag=dag,
    pool='loading_pool'
)

transform_user_geo = PythonOperator(
    task_id='transform_load_user_geo',
    python_callable=transform_and_load_user_geo,
    dag=dag,
    pool='loading_pool'
)

transform_stats = PythonOperator(
    task_id='transform_load_stats',
    python_callable=transform_and_load_stats,
    dag=dag,
    pool='loading_pool'
)

# End task
end_pipeline = DummyOperator(
    task_id='end',
    dag=dag
)

# Define task dependencies
start_pipeline >> [extract_transactions, extract_users]
[extract_transactions, extract_users] >> data_extracted
data_extracted >> clustering

# All analytics transformations depend on clustering
clustering >> [
    transform_age_count,
    transform_gender_count,
    transform_user_geo,
    transform_stats
]

# Some transformations depend only on raw transaction data
clustering >> [
    transform_trans_by_day,
    transform_service_count
]

# All transformations must complete before ending
[
    transform_age_count,
    transform_trans_by_day,
    transform_gender_count,
    transform_service_count,
    transform_user_geo,
    transform_stats
] >> end_pipeline
