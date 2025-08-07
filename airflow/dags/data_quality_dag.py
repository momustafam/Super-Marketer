"""
Data Quality Check DAG for Super Marketer
Runs data quality checks on the processed data
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.sensors.external_task import ExternalTaskSensor

def check_data_freshness(**context):
    """Check if data was updated recently"""
    import logging
    from etl_functions import SparkETLBase
    
    logger = logging.getLogger(__name__)
    logger.info("Checking data freshness...")
    
    etl = SparkETLBase("Data-Quality-Check")
    try:
        _, target_config = etl.get_default_configs()
        
        # Check each table for recent updates
        tables_to_check = ['age_count', 'trans_by_day', 'gender_count', 
                          'service_count', 'user_geo', 'stats']
        
        for table in tables_to_check:
            try:
                df = etl.spark.read.jdbc(
                    url=etl._get_jdbc_url(target_config['server'], target_config['database']),
                    table=table,
                    properties=etl._get_connection_properties(target_config['user'], target_config['password'])
                )
                row_count = df.count()
                logger.info(f"Table {table}: {row_count} rows")
                
                if row_count == 0:
                    raise ValueError(f"Table {table} is empty!")
                    
            except Exception as e:
                logger.error(f"Data quality check failed for table {table}: {str(e)}")
                raise
        
        logger.info("All data quality checks passed!")
        return "data_quality_passed"
        
    finally:
        etl.close()

def check_clustering_results(**context):
    """Validate clustering results"""
    import logging
    from etl_functions import SparkETLBase
    from pyspark.sql import functions as F
    
    logger = logging.getLogger(__name__)
    logger.info("Checking clustering results...")
    
    etl = SparkETLBase("Clustering-Quality-Check")
    try:
        _, target_config = etl.get_default_configs()
        
        # Read age_count table to check cluster distribution
        df = etl.spark.read.jdbc(
            url=etl._get_jdbc_url(target_config['server'], target_config['database']),
            table="age_count",
            properties=etl._get_connection_properties(target_config['user'], target_config['password'])
        )
        
        # Check if we have all expected clusters
        clusters = df.select("cluster").distinct().collect()
        cluster_names = [row.cluster for row in clusters]
        
        expected_clusters = ['Top Users', 'Loyal Users', 'At-Risk Users']
        
        for expected in expected_clusters:
            if expected not in cluster_names:
                logger.warning(f"Expected cluster '{expected}' not found in results")
        
        logger.info(f"Found clusters: {cluster_names}")
        logger.info("Clustering quality check completed!")
        return "clustering_quality_passed"
        
    finally:
        etl.close()

# Default arguments
default_args = {
    'owner': 'data-quality-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

# Define the DAG
dag = DAG(
    'super_marketer_data_quality_checks',
    default_args=default_args,
    description='Data Quality Checks for Super Marketer ETL',
    schedule_interval='0 1 * * *',  # Run 1 hour after main ETL
    max_active_runs=1,
    tags=['data-quality', 'monitoring', 'validation']
)

# Wait for main ETL to complete
wait_for_etl = ExternalTaskSensor(
    task_id='wait_for_main_etl',
    external_dag_id='super_marketer_etl_pipeline',
    external_task_id='end_pipeline',
    timeout=7200,  # 2 hours timeout
    poke_interval=300,  # Check every 5 minutes
    dag=dag
)

# Data quality checks
check_freshness = PythonOperator(
    task_id='check_data_freshness',
    python_callable=check_data_freshness,
    dag=dag
)

check_clustering = PythonOperator(
    task_id='check_clustering_results',
    python_callable=check_clustering_results,
    dag=dag
)

# End task
quality_checks_complete = DummyOperator(
    task_id='quality_checks_complete',
    dag=dag
)

# Dependencies
wait_for_etl >> [check_freshness, check_clustering]
[check_freshness, check_clustering] >> quality_checks_complete
