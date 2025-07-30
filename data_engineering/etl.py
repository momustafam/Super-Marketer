"""
ETL script to extract data from a SQL Server datawarehouse, transform it,
and load it into another data mart.
"""
import os
import pymssql
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import AgglomerativeClustering
import pandas as pd
from typing import Dict, Any
import schedule
import time
import logging

# Configure logging
logger = logging.getLogger(__name__)


class ETLPipeline:
    """
    ETL Pipeline class for extracting data from SQL Server datawarehouse,
    transforming it, and loading it into another data mart.
    """
    
    def __init__(self, spark_app_name: str = "Super-Marketer-ETL"):
        """
        Initialize the ETL Pipeline.
        
        :param spark_app_name: Name for the Spark application
        """
        self.spark_app_name = spark_app_name
        self.spark = None
        try:
            self._initialize_spark()
            print("Spark session initialized successfully!")
        except Exception as e:
            print(f"Failed to initialize Spark session: {str(e)}")
            print("Please ensure you have the required dependencies installed:")
            print("- Java 8 or 11 installed and JAVA_HOME set")
            print("- PySpark and required Python packages")
            raise
    
    def _initialize_spark(self):
        """Initialize Spark session with environment-aware configuration."""
        import os
        import sys
        
        # Detect if running in Docker
        is_docker = os.path.exists('/.dockerenv') or os.getenv('PYTHONPATH', '').startswith('/app')
        
        if is_docker:
            print("Detected Docker environment - using Docker-optimized Spark configuration")
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
        else:
            print("Detected Windows environment - using Windows-compatible Spark configuration")
            # Windows environment configuration (original code)
            script_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            
            # Set HADOOP_HOME to the project directory (where winutils.exe is located)
            os.environ['HADOOP_HOME'] = script_dir
            os.environ['HADOOP_CONF_DIR'] = script_dir
            
            # Fix Python version mismatch by setting both driver and worker to use the same Python
            python_exe = sys.executable
            os.environ['PYSPARK_PYTHON'] = python_exe
            os.environ['PYSPARK_DRIVER_PYTHON'] = python_exe
            
            # Configure Spark for Windows and local mode with minimal Hadoop dependency
            self.spark = SparkSession.builder \
                .appName(self.spark_app_name) \
                .config("spark.jars.packages", "com.microsoft.sqlserver:mssql-jdbc:12.10.1.jre11") \
                .config("spark.sql.warehouse.dir", "file:///tmp/spark-warehouse") \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
                .config("spark.hadoop.fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem") \
                .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
                .config("spark.sql.execution.arrow.maxRecordsPerBatch", "10000") \
                .config("spark.pyspark.python", python_exe) \
                .config("spark.pyspark.driver.python", python_exe) \
                .config("spark.task.maxDirectResultSize", "1g") \
                .config("spark.sql.shuffle.partitions", "4") \
                .master("local[*]") \
                .getOrCreate()
        
        # Common configuration for both environments
        print(f"Using Python executable: {os.environ.get('PYSPARK_PYTHON', sys.executable)}")
        print(f"Python version: {sys.version}")
        print(f"PYSPARK_PYTHON: {os.environ.get('PYSPARK_PYTHON', 'Not set')}")
        print(f"PYSPARK_DRIVER_PYTHON: {os.environ.get('PYSPARK_DRIVER_PYTHON', 'Not set')}")
        print(f"Environment: {'Docker' if is_docker else 'Windows'}")
        
        # Set log level to reduce noise
        self.spark.sparkContext.setLogLevel("WARN")
    
    def _get_connection_properties(self, user: str, password: str) -> Dict[str, str]:
        """
        Get JDBC connection properties for SQL Server.
        
        :param user: Username for SQL Server authentication
        :param password: Password for SQL Server authentication
        :return: Dictionary containing connection properties
        """
        return {
            "user": user,
            "password": password,
            "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
            "encrypt": "false",  # Disable encryption for local development
            "trustServerCertificate": "true",  # Trust server certificate
            "loginTimeout": "30"
        }
    
    def _get_jdbc_url(self, server: str, database: str) -> str:
        """
        Construct JDBC URL for SQL Server.
        
        :param server: SQL Server address
        :param database: Database name
        :return: JDBC URL string
        """
        return f"jdbc:sqlserver://{server};databaseName={database}"
    
    def extract_data(self, server: str, 
                     user: str, 
                     password: str, 
                     database: str, 
                     query: str) -> pyspark.sql.DataFrame:
        """
        Extracts data from a SQL Server database using the provided query.
        
        :param server: SQL Server address
        :param user: Username for SQL Server authentication
        :param password: Password for SQL Server authentication
        :param database: Database name to connect to
        :param query: SQL query to execute for data extraction
        :return: PySpark DataFrame containing the extracted data
        """
        jdbc_url = self._get_jdbc_url(server, database)
        connection_properties = self._get_connection_properties(user, password)
        
        # Read data using PySpark's JDBC connector
        df = self.spark.read.jdbc(
            url=jdbc_url,
            table=f"({query}) as subquery",
            properties=connection_properties
        )
        
        return df
    
    def clustering(self, transactions_data: pyspark.sql.DataFrame, 
                   users_data: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
        """
        Performs RFM (Recency, Frequency, Monetary) clustering on transaction data
        and joins the cluster information back to the users data.
        
        :param transactions_data: PySpark DataFrame containing transaction data
        :param users_data: PySpark DataFrame containing user data
        :return: PySpark DataFrame containing users with cluster information
        """
        try:
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
                print("Warning: No transaction data found for clustering")
                return users_data.withColumn("cluster", F.lit("Unknown"))
            
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
            cluster_df = self.spark.createDataFrame(rfm_df[['user_id', 'cluster']])
            
            # Join cluster information with users data
            users_with_clusters = users_data.join(cluster_df, on="user_id", how="left")
            
            return users_with_clusters
            
        except Exception as e:
            print(f"Error in clustering: {str(e)}")
            print("Returning users data without clustering information")
            return users_data.withColumn("cluster", F.lit("Unknown"))
    
    def transform_age_count(self, users_df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
        """
        Transforms the data to get age count by cluster.
        
        :param users_df: PySpark DataFrame containing the data to be transformed
        :return: PySpark DataFrame with the transformed data
        """
        age_count_df = users_df.groupBy(["age", "cluster"]).agg(
            F.count("user_id").alias("count"),
        )
        
        return age_count_df
    
    def transform_trans_by_day(self, transactions_df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
        """
        Transforms the transaction data to get hourly transaction counts.
        
        :param transactions_df: PySpark DataFrame containing the transaction data
        :return: PySpark DataFrame with hourly transaction counts (0-23 hours)
        """
        # Extract hour from timestamp and group by hour
        hourly_trans_df = transactions_df.withColumn(
            "hour", F.hour("trans_timestamp")
        ).groupBy("hour").agg(
            F.count("trans_id").alias("count")
        ).orderBy("hour")
        
        return hourly_trans_df
    
    def transform_gender_count(self, users_df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
        """
        Transforms the data to get gender count by cluster.
        
        :param users_df: PySpark DataFrame containing the data to be transformed
        :return: PySpark DataFrame with the transformed data
        """
        gender_count_df = users_df.groupBy(["gender", "cluster"]).agg(
            F.count("user_id").alias("count"),
        )
        
        return gender_count_df
    
    def transform_service_count(self, transactions_df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
        """
        Transforms the transaction data to get service count by cluster.
        
        :param transactions_df: PySpark DataFrame containing the transaction data
        :return: PySpark DataFrame with service counts by cluster
        """
        service_count_df = transactions_df.groupBy(["trans_service"]).agg(
            F.count("trans_id").alias("count"),
        ).withColumnRenamed("trans_service", "service")
        
        return service_count_df
    
    def transform_user_geo(self, users_df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
        """
        Transforms the user data to get geographical distribution.
        
        :param users_df: PySpark DataFrame containing the user data
        :return: PySpark DataFrame with geographical distribution
        """
        user_geo = users_df.select(
            "longitude", 
            "latitude", 
            "city", 
            "state",
            "cluster"
        ).withColumnRenamed("state", "country")

        return user_geo
    
    def transform_stats(self, users_df: pyspark.sql.DataFrame, transactions_df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
        """
        Transforms the data to get key statistics for the dashboard.
        
        :param users_df: PySpark DataFrame containing the user data
        :param transactions_df: PySpark DataFrame containing the transaction data
        :return: PySpark DataFrame with statistics (single row)
        """
        # Calculate new users (total number of users)
        new_users = users_df.count()
        
        # Calculate returning users (new_users // 4)
        returning_users = new_users // 4
        
        # Calculate new transactions from the beginning of current month
        from datetime import datetime, date
        import calendar
        
        # Get current date and first day of current month
        today = date.today()
        first_day_of_month = today.replace(day=1)
        
        # Filter transactions from beginning of current month
        current_month_transactions = transactions_df.filter(
            F.col("trans_timestamp") >= F.lit(first_day_of_month)
        )
        new_transactions = current_month_transactions.count()
        
        # Calculate average CLV (Customer Lifetime Value)
        # CLV = Total transaction amount per user
        user_clv = transactions_df.groupBy("user_id").agg(
            F.sum("trans_amount").alias("total_spent")
        )
        avg_clv = user_clv.agg(F.avg("total_spent")).collect()[0][0]
        
        # Handle None case for avg_clv
        if avg_clv is None:
            avg_clv = 0.0
        
        # Create a single row DataFrame with the statistics
        stats_data = [(new_users, returning_users, new_transactions, float(avg_clv))]
        stats_df = self.spark.createDataFrame(
            stats_data, 
            ["new_users", "returning_users", "new_transactions", "avg_clv"]
        )
        
        return stats_df

    def load_data(self, df: pyspark.sql.DataFrame, 
                  server: str,
                  user: str,
                  password: str,
                  database: str,
                  table: str,
                  mode: str = "append"):
        """
        Loads the data into a SQL Server database.
        
        :param df: PySpark DataFrame containing the data to be loaded
        :param server: SQL Server address
        :param user: Username for SQL Server authentication
        :param password: Password for SQL Server authentication
        :param database: Database name to connect to
        :param table: Table name to write data to
        :param mode: Write mode ('append', 'overwrite', 'ignore', 'error')
        """
        jdbc_url = self._get_jdbc_url(server, database)
        connection_properties = self._get_connection_properties(user, password)
        
        # Write data to SQL Server
        df.write.jdbc(
            url=jdbc_url,
            table=table,
            mode=mode,
            properties=connection_properties
        )
    
    def run_etl_pipeline(self, 
                        source_config: Dict[str, str],
                        target_config: Dict[str, str],
                        transactions_query: str,
                        users_query: str,
                        age_count_table: str = "age_count",
                        trans_by_day_table: str = "trans_by_day",
                        gender_count_table: str = "gender_count",
                        service_count_table: str = "service_count",
                        user_geo_table: str = "user_geo",
                        stats_table: str = "stats") -> None:
        """
        Run the complete ETL pipeline.
        
        :param source_config: Source database configuration
        :param target_config: Target database configuration
        :param transactions_query: SQL query for transaction data extraction
        :param users_query: SQL query for users data extraction
        :param age_count_table: Target table name for age count analytics
        :param trans_by_day_table: Target table name for hourly transaction analytics
        :param gender_count_table: Target table name for gender count analytics
        :param service_count_table: Target table name for service count analytics
        :param user_geo_table: Target table name for user geographical data
        :param stats_table: Target table name for dashboard statistics
        """
        try:
            # Extract transaction data
            print("Starting transaction data extraction...")
            transactions_data = self.extract_data(
                server=source_config['server'],
                user=source_config['user'],
                password=source_config['password'],
                database=source_config['database'],
                query=transactions_query
            )
            
            # Extract users data
            print("Starting users data extraction...")
            users_data = self.extract_data(
                server=source_config['server'],
                user=source_config['user'],
                password=source_config['password'],
                database=source_config['database'],
                query=users_query
            )
            
            # Transform data - add clustering to users (keep in memory)
            print("Starting data transformation (clustering)...")
            users_with_clusters = self.clustering(transactions_data, users_data)
            
            # Transform for age count analytics using the DataFrame
            print("Starting age count transformation...")
            age_count_data = self.transform_age_count(users_with_clusters)
            
            # Transform for hourly transaction analytics
            print("Starting hourly transaction transformation...")
            hourly_trans_data = self.transform_trans_by_day(transactions_data)
            
            # Transform for gender count analytics
            print("Starting gender count transformation...")
            gender_count_data = self.transform_gender_count(users_with_clusters)
            
            # Transform for service count analytics
            print("Starting service count transformation...")
            service_count_data = self.transform_service_count(transactions_data)
            
            # Transform for user geographical data
            print("Starting user geographical transformation...")
            user_geo_data = self.transform_user_geo(users_with_clusters)
            
            # Transform for dashboard statistics
            print("Starting statistics transformation...")
            stats_data = self.transform_stats(users_with_clusters, transactions_data)
            
            # Load age count analytics
            print("Loading age count analytics...")
            self.load_data(
                df=age_count_data,
                server=target_config['server'],
                user=target_config['user'],
                password=target_config['password'],
                database=target_config['database'],
                table=age_count_table,
                mode="overwrite"  # Replace existing data
            )
            
            # Load hourly transaction analytics
            print("Loading hourly transaction analytics...")
            self.load_data(
                df=hourly_trans_data,
                server=target_config['server'],
                user=target_config['user'],
                password=target_config['password'],
                database=target_config['database'],
                table=trans_by_day_table,
                mode="overwrite"  # Replace existing data
            )
            
            # Load gender count analytics
            print("Loading gender count analytics...")
            self.load_data(
                df=gender_count_data,
                server=target_config['server'],
                user=target_config['user'],
                password=target_config['password'],
                database=target_config['database'],
                table=gender_count_table,
                mode="overwrite"  # Replace existing data
            )
            
            # Load service count analytics
            print("Loading service count analytics...")
            self.load_data(
                df=service_count_data,
                server=target_config['server'],
                user=target_config['user'],
                password=target_config['password'],
                database=target_config['database'],
                table=service_count_table,
                mode="overwrite"  # Replace existing data
            )
            
            # Load user geographical data
            print("Loading user geographical data...")
            self.load_data(
                df=user_geo_data,
                server=target_config['server'],
                user=target_config['user'],
                password=target_config['password'],
                database=target_config['database'],
                table=user_geo_table,
                mode="overwrite"  # Replace existing data
            )
            
            # Load dashboard statistics
            print("Loading dashboard statistics...")
            self.load_data(
                df=stats_data,
                server=target_config['server'],
                user=target_config['user'],
                password=target_config['password'],
                database=target_config['database'],
                table=stats_table,
                mode="overwrite"  # Replace existing data
            )
            
            print("ETL pipeline completed successfully!")
            
        except Exception as e:
            print(f"ETL pipeline failed: {str(e)}")
            raise
    
    def get_default_configs(self) -> tuple[Dict[str, str], Dict[str, str]]:
        """
        Get default database configurations based on environment.
        
        :return: Tuple of (source_config, target_config) dictionaries
        """
        # Check if running in Docker - use same detection as Spark initialization
        is_docker = os.path.exists('/.dockerenv') or os.getenv('PYTHONPATH', '').startswith('/app')
        
        if is_docker:
            print("Using Docker database configuration")
            # Use Docker container names for internal communication
            source_config = {
                'server': 'data_warehouse:1433',  # Internal Docker network
                'user': 'SA',
                'password': 'YourStrong!Passw0rd',
                'database': 'CustomerWarehouse'
            }
            
            target_config = {
                'server': 'marketing_data_mart:1433',  # Internal Docker network
                'user': 'SA',
                'password': 'Password1234!',
                'database': 'MarketingDataMart'
            }
        else:
            print("Using local development database configuration")
            # Use localhost for local development
            source_config = {
                'server': 'localhost:1434',  # External port mapping
                'user': 'SA',
                'password': 'YourStrong!Passw0rd',
                'database': 'CustomerWarehouse'
            }
            
            target_config = {
                'server': 'localhost:1435',  # External port mapping
                'user': 'SA',
                'password': 'Password1234!',
                'database': 'MarketingDataMart'
            }
        
        return source_config, target_config
    
    def close(self):
        """Close the Spark session."""
        if self.spark:
            self.spark.stop()


def run_etl_job(age_count_table="age_count", 
                trans_by_day_table="trans_by_day", 
                gender_count_table="gender_count",
                service_count_table="service_count",
                user_geo_table="user_geo",
                stats_table="stats"):
    """
    Convenience function to run the complete ETL pipeline
    """
    logger.info("Starting ETL job execution...")
    etl = ETLPipeline("Super-Marketer-ETL")
    
    try:
        # Get default configurations
        source_config, target_config = etl.get_default_configs()
        
        # SQL queries for data extraction
        transactions_query = """
        SELECT *
        FROM fact_trans
        """
        
        users_query = """
        SELECT *
        FROM dim_users
        """
        
        # Run the ETL pipeline
        etl.run_etl_pipeline(
            source_config=source_config,
            target_config=target_config,
            transactions_query=transactions_query,
            users_query=users_query,
            age_count_table=age_count_table,
            trans_by_day_table=trans_by_day_table,
            gender_count_table=gender_count_table,
            service_count_table=service_count_table,
            user_geo_table=user_geo_table,
            stats_table=stats_table
        )
        logger.info("ETL job completed successfully!")
    except Exception as e:
        logger.error(f"ETL job failed: {str(e)}")
        raise
    finally:
        etl.close()


def run_etl_scheduler():
    """
    Runs the ETL pipeline on a scheduled basis (daily at 00:00 AM)
    Includes immediate execution on startup and continuous monitoring
    """
    logger.info("ETL Pipeline running in scheduled mode (daily at 00:00 AM)")
    
    # Schedule the ETL to run daily at 00:00 AM
    schedule.every().day.at("00:00").do(run_etl_job)
    
    # Run once immediately on startup
    logger.info("Running initial ETL execution...")
    run_etl_job()
    
    # Keep the scheduler running
    while True:
        try:
            schedule.run_pending()
            time.sleep(60)  # Check every minute
        except KeyboardInterrupt:
            logger.info("ETL Scheduler stopped by user")
            break
        except Exception as e:
            logger.error(f"ETL Scheduler error: {str(e)}")
            time.sleep(300)  # Wait 5 minutes before retrying


# Example usage
if __name__ == "__main__":
    run_etl_scheduler()