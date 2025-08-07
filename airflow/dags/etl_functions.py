"""
Modular ETL functions for Airflow DAG
"""
import os
import sys
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import AgglomerativeClustering
from typing import Dict, Any, Tuple
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SparkETLBase:
    """Base class for Spark ETL operations"""
    
    def __init__(self, spark_app_name: str = "Super-Marketer-ETL"):
        self.spark_app_name = spark_app_name
        self.spark = None
        self._initialize_spark()
    
    def _initialize_spark(self):
        """Initialize Spark session with Docker-optimized configuration."""
        # Docker environment configuration
        os.environ['JAVA_HOME'] = '/usr/lib/jvm/java-17-openjdk-amd64'
        os.environ['SPARK_HOME'] = '/opt/spark'
        os.environ['PYSPARK_PYTHON'] = 'python3'
        os.environ['PYSPARK_DRIVER_PYTHON'] = 'python3'
        
        # Configure Spark for Docker/Linux environment
        self.spark = SparkSession.builder \
            .appName(self.spark_app_name) \
            .config("spark.jars.packages", "com.microsoft.sqlserver:mssql-jdbc:12.10.1.jre11") \
            .config("spark.sql.warehouse.dir", "file:///tmp/spark-warehouse") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
            .config("spark.sql.execution.arrow.maxRecordsPerBatch", "10000") \
            .config("spark.task.maxDirectResultSize", "1g") \
            .config("spark.sql.shuffle.partitions", "4") \
            .config("spark.driver.memory", "2g") \
            .config("spark.executor.memory", "2g") \
            .config("spark.driver.maxResultSize", "1g") \
            .config("spark.network.timeout", "300s") \
            .config("spark.executor.heartbeatInterval", "60s") \
            .master("local[*]") \
            .getOrCreate()
        
        # Set log level to reduce noise
        self.spark.sparkContext.setLogLevel("WARN")
        logger.info("Spark session initialized successfully!")
    
    def _get_connection_properties(self, user: str, password: str) -> Dict[str, str]:
        """Get JDBC connection properties for SQL Server."""
        return {
            "user": user,
            "password": password,
            "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
            "encrypt": "false",
            "trustServerCertificate": "true",
            "loginTimeout": "30"
        }
    
    def _get_jdbc_url(self, server: str, database: str) -> str:
        """Construct JDBC URL for SQL Server."""
        return f"jdbc:sqlserver://{server};databaseName={database}"
    
    def get_default_configs(self) -> Tuple[Dict[str, str], Dict[str, str]]:
        """Get default database configurations for Docker environment."""
        # Use Docker container names for internal communication
        source_config = {
            'server': 'data_warehouse:1433',
            'user': 'SA',
            'password': 'YourStrong!Passw0rd',
            'database': 'CustomerWarehouse'
        }
        
        target_config = {
            'server': 'marketing_data_mart:1433',
            'user': 'SA',
            'password': 'Password1234!',
            'database': 'MarketingDataMart'
        }
        
        return source_config, target_config
    
    def close(self):
        """Close the Spark session."""
        if self.spark:
            self.spark.stop()


def extract_transactions_data(**context) -> str:
    """
    Extract transaction data from warehouse
    Returns: XCom key for the extracted data
    """
    logger.info("Starting transaction data extraction...")
    
    etl = SparkETLBase("Extract-Transactions")
    try:
        source_config, _ = etl.get_default_configs()
        
        query = "SELECT * FROM fact_trans"
        jdbc_url = etl._get_jdbc_url(source_config['server'], source_config['database'])
        connection_properties = etl._get_connection_properties(source_config['user'], source_config['password'])
        
        # Read data using PySpark's JDBC connector
        df = etl.spark.read.jdbc(
            url=jdbc_url,
            table=f"({query}) as subquery",
            properties=connection_properties
        )
        
        # Cache the DataFrame for reuse
        df.cache()
        row_count = df.count()
        logger.info(f"Extracted {row_count} transaction records")
        
        # Store as temporary view for other tasks to access
        df.createOrReplaceTempView("transactions_temp")
        
        return "transactions_extracted"
        
    except Exception as e:
        logger.error(f"Transaction extraction failed: {str(e)}")
        raise
    finally:
        etl.close()


def extract_users_data(**context) -> str:
    """
    Extract users data from warehouse
    Returns: XCom key for the extracted data
    """
    logger.info("Starting users data extraction...")
    
    etl = SparkETLBase("Extract-Users")
    try:
        source_config, _ = etl.get_default_configs()
        
        query = "SELECT * FROM dim_users"
        jdbc_url = etl._get_jdbc_url(source_config['server'], source_config['database'])
        connection_properties = etl._get_connection_properties(source_config['user'], source_config['password'])
        
        # Read data using PySpark's JDBC connector
        df = etl.spark.read.jdbc(
            url=jdbc_url,
            table=f"({query}) as subquery",
            properties=connection_properties
        )
        
        # Cache the DataFrame for reuse
        df.cache()
        row_count = df.count()
        logger.info(f"Extracted {row_count} user records")
        
        # Store as temporary view for other tasks to access
        df.createOrReplaceTempView("users_temp")
        
        return "users_extracted"
        
    except Exception as e:
        logger.error(f"Users extraction failed: {str(e)}")
        raise
    finally:
        etl.close()


def perform_clustering(**context) -> str:
    """
    Perform RFM clustering on transaction and user data
    """
    logger.info("Starting RFM clustering...")
    
    etl = SparkETLBase("Clustering")
    try:
        # Read from temporary views created by extraction tasks
        transactions_data = etl.spark.table("transactions_temp")
        users_data = etl.spark.table("users_temp")
        
        # Engineer RFM features from transactions
        max_date = transactions_data.agg(F.max("trans_timestamp")).first()[0]
        rfm = transactions_data.groupBy("user_id").agg(
            F.max("trans_timestamp").alias("last_trans_date"),
            F.count("trans_amount").alias("frequency"),
            F.sum("trans_amount").alias("monetary")
        )
        rfm = rfm.withColumn(
            "recency", F.datediff(F.lit(max_date), F.col("last_trans_date"))
        )
        
        # Convert to Pandas for ML operations
        rfm_df = rfm.toPandas()
        
        # Handle edge cases for empty data
        if rfm_df.empty:
            logger.warning("No transaction data found for clustering")
            users_with_clusters = users_data.withColumn("cluster", F.lit("Unknown"))
        else:
            # Create RFM scores
            rfm_df['frequency_score'] = pd.qcut(rfm_df['frequency'], 5, labels=[1, 2, 3, 4, 5], duplicates='drop')
            rfm_df['monetary_score'] = pd.qcut(rfm_df['monetary'], 5, labels=[1, 2, 3, 4, 5], duplicates='drop')
            rfm_df['recency_score'] = rfm_df['recency'].apply(lambda x: 1 if x > 30 else 5)
            
            # Perform clustering
            features = ['frequency', 'monetary', 'recency']
            scaler = StandardScaler()
            scaled_features = scaler.fit_transform(rfm_df[features])
            
            # Fit Agglomerative Clustering with 3 clusters
            agg = AgglomerativeClustering(n_clusters=3)
            rfm_df['cluster'] = agg.fit_predict(scaled_features)
            
            # Map cluster labels to descriptive names
            label_map = {
                0: 'Top Users',
                1: 'Loyal Users',
                2: 'At-Risk Users',
            }
            rfm_df['cluster'] = rfm_df['cluster'].map(label_map)
            
            # Convert back to Spark DataFrame
            cluster_df = etl.spark.createDataFrame(rfm_df[['user_id', 'cluster']])
            
            # Join cluster information with users data
            users_with_clusters = users_data.join(cluster_df, on="user_id", how="left")
        
        # Cache the result and create temporary view
        users_with_clusters.cache()
        users_with_clusters.createOrReplaceTempView("users_with_clusters_temp")
        
        cluster_count = users_with_clusters.filter(F.col("cluster").isNotNull()).count()
        logger.info(f"Clustering completed for {cluster_count} users")
        
        return "clustering_completed"
        
    except Exception as e:
        logger.error(f"Clustering failed: {str(e)}")
        raise
    finally:
        etl.close()


def load_data_to_target(df, table_name: str, mode: str = "overwrite"):
    """Helper function to load data to target database"""
    etl = SparkETLBase(f"Load-{table_name}")
    try:
        _, target_config = etl.get_default_configs()
        
        jdbc_url = etl._get_jdbc_url(target_config['server'], target_config['database'])
        connection_properties = etl._get_connection_properties(target_config['user'], target_config['password'])
        
        # Write data to SQL Server
        df.write.jdbc(
            url=jdbc_url,
            table=table_name,
            mode=mode,
            properties=connection_properties
        )
        
        row_count = df.count()
        logger.info(f"Loaded {row_count} records to {table_name}")
        
    finally:
        etl.close()


def transform_and_load_age_count(**context) -> str:
    """Transform and load age count analytics"""
    logger.info("Processing age count analytics...")
    
    etl = SparkETLBase("Age-Count-Analytics")
    try:
        users_with_clusters = etl.spark.table("users_with_clusters_temp")
        
        age_count_df = users_with_clusters.groupBy(["age", "cluster"]).agg(
            F.count("user_id").alias("count")
        )
        
        load_data_to_target(age_count_df, "age_count")
        return "age_count_completed"
        
    except Exception as e:
        logger.error(f"Age count analytics failed: {str(e)}")
        raise
    finally:
        etl.close()


def transform_and_load_trans_by_day(**context) -> str:
    """Transform and load hourly transaction analytics"""
    logger.info("Processing hourly transaction analytics...")
    
    etl = SparkETLBase("Trans-By-Day-Analytics")
    try:
        transactions_data = etl.spark.table("transactions_temp")
        
        hourly_trans_df = transactions_data.withColumn(
            "hour", F.hour("trans_timestamp")
        ).groupBy("hour").agg(
            F.count("trans_id").alias("count")
        ).orderBy("hour")
        
        load_data_to_target(hourly_trans_df, "trans_by_day")
        return "trans_by_day_completed"
        
    except Exception as e:
        logger.error(f"Hourly transaction analytics failed: {str(e)}")
        raise
    finally:
        etl.close()


def transform_and_load_gender_count(**context) -> str:
    """Transform and load gender count analytics"""
    logger.info("Processing gender count analytics...")
    
    etl = SparkETLBase("Gender-Count-Analytics")
    try:
        users_with_clusters = etl.spark.table("users_with_clusters_temp")
        
        gender_count_df = users_with_clusters.groupBy(["gender", "cluster"]).agg(
            F.count("user_id").alias("count")
        )
        
        load_data_to_target(gender_count_df, "gender_count")
        return "gender_count_completed"
        
    except Exception as e:
        logger.error(f"Gender count analytics failed: {str(e)}")
        raise
    finally:
        etl.close()


def transform_and_load_service_count(**context) -> str:
    """Transform and load service count analytics"""
    logger.info("Processing service count analytics...")
    
    etl = SparkETLBase("Service-Count-Analytics")
    try:
        transactions_data = etl.spark.table("transactions_temp")
        
        # Load MCC codes from CSV
        mcc_df = pd.read_csv("https://raw.githubusercontent.com/greggles/mcc-codes/main/mcc_codes.csv")
        mcc_df.drop(columns=["edited_description","combined_description","usda_description","irs_reportable"], inplace=True)
        mcc_df.rename(columns={"irs_description":"description"}, inplace=True)

        # Convert pandas DataFrame to Spark DataFrame
        mcc_spark_df = etl.spark.createDataFrame(mcc_df)

        # Group transactions by service and get counts
        service_count_df = transactions_data.groupBy(["trans_service"]).agg(
            F.count("trans_id").alias("count"),
        ).withColumnRenamed("trans_service", "service")

        # Join with MCC codes to get descriptions
        service_with_descriptions = service_count_df.join(
            mcc_spark_df,
            service_count_df.service == mcc_spark_df.mcc,
            "left"
        ).select(
            F.coalesce(F.col("description"), F.col("service")).alias("service"),
            F.col("count")
        ).groupBy("service").agg(
            F.sum("count").alias("count")
        ).orderBy(F.desc("count"))
        
        load_data_to_target(service_with_descriptions, "service_count")
        return "service_count_completed"
        
    except Exception as e:
        logger.error(f"Service count analytics failed: {str(e)}")
        raise
    finally:
        etl.close()


def transform_and_load_user_geo(**context) -> str:
    """Transform and load user geographical data"""
    logger.info("Processing user geographical data...")
    
    etl = SparkETLBase("User-Geo-Analytics")
    try:
        users_with_clusters = etl.spark.table("users_with_clusters_temp")
        
        user_geo = users_with_clusters.select(
            "longitude", 
            "latitude", 
            "city", 
            "state",
            "cluster"
        ).withColumnRenamed("state", "country")
        
        load_data_to_target(user_geo, "user_geo")
        return "user_geo_completed"
        
    except Exception as e:
        logger.error(f"User geographical analytics failed: {str(e)}")
        raise
    finally:
        etl.close()


def transform_and_load_stats(**context) -> str:
    """Transform and load dashboard statistics"""
    logger.info("Processing dashboard statistics...")
    
    etl = SparkETLBase("Stats-Analytics")
    try:
        users_with_clusters = etl.spark.table("users_with_clusters_temp")
        transactions_data = etl.spark.table("transactions_temp")
        
        # Calculate statistics
        new_users = users_with_clusters.count()
        returning_users = new_users // 4
        
        from datetime import date
        today = date.today()
        first_day_of_month = today.replace(day=1)
        
        current_month_transactions = transactions_data.filter(
            F.col("trans_timestamp") >= F.lit(first_day_of_month)
        )
        new_transactions = current_month_transactions.count()
        
        user_clv = transactions_data.groupBy("user_id").agg(
            F.sum("trans_amount").alias("total_spent")
        )
        avg_clv = user_clv.agg(F.avg("total_spent")).collect()[0][0]
        
        if avg_clv is None:
            avg_clv = 0.0
        
        # Create statistics DataFrame
        stats_data = [(new_users, returning_users, new_transactions, float(avg_clv))]
        stats_df = etl.spark.createDataFrame(
            stats_data, 
            ["new_users", "returning_users", "new_transactions", "avg_clv"]
        )
        
        load_data_to_target(stats_df, "stats")
        return "stats_completed"
        
    except Exception as e:
        logger.error(f"Dashboard statistics failed: {str(e)}")
        raise
    finally:
        etl.close()
