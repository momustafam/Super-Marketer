# Airflow Configuration
import os
from airflow.configuration import conf

# Custom configurations for Super Marketer ETL
AIRFLOW_VAR_ETL_CONFIG = {
    'source_db': {
        'server': 'data_warehouse:1433',
        'user': 'SA',
        'password': 'YourStrong!Passw0rd',
        'database': 'CustomerWarehouse'
    },
    'target_db': {
        'server': 'marketing_data_mart:1433',
        'user': 'SA',
        'password': 'Password1234!',
        'database': 'MarketingDataMart'
    },
    'spark_config': {
        'driver_memory': '2g',
        'executor_memory': '2g',
        'max_result_size': '1g'
    }
}

# Pool configurations
POOLS = {
    'extraction_pool': {
        'slots': 2,
        'description': 'Pool for data extraction tasks'
    },
    'transformation_pool': {
        'slots': 1,
        'description': 'Pool for data transformation tasks'
    },
    'loading_pool': {
        'slots': 4,
        'description': 'Pool for loading tasks'
    }
}
