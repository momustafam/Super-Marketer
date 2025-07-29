import os
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from sklearn.cluster import AgglomerativeClustering
from sklearn.preprocessing import StandardScaler

# Start Spark session
spark = SparkSession.builder \
    .appName("Scheduled ETL Job") \
    .config("spark.driver.extraClassPath", "/opt/spark-drivers/sqljdbc42.jar")
    .getOrCreate()

# JDBC connection parameters from environment
jdbc_wh_url = f"jdbc:sqlserver://{os.environ['SQL_SERVER']}:1433;databaseName={os.environ['SQL_DATABASE']}"
jdbc_dm_url = f"jdbc:sqlserver://{os.environ['SQL_MART_SERVER']}:1433;databaseName={os.environ['SQL_MART_DATABASE']}"

jdbc_properties_wh = {
    "user": os.environ["SQL_USER"],
    "password": os.environ["SQL_PASSWORD"],
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

jdbc_properties_dm = {
    "user": os.environ["SQL_MART_USER"],
    "password": os.environ["SQL_MART_PASSWORD"],
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# Extract from warehouse
fact_trans = spark.read.jdbc(
    url=jdbc_wh_url,
    table="fact_trans",
    properties=jdbc_properties_wh
)

dim_users = spark.read.jdbc(
    url=jdbc_wh_url,
    table="dim_users",
    properties=jdbc_properties_wh
)

# Join fact_trans with dim_users on user_id
data = fact_trans.join(dim_users, on="user_id", how="inner")

# Find max date
max_date = data.agg(F.max("trans_timestamp")).first()[0]

# RFM calculation
users = data.groupBy("user_id").agg(
    F.max("trans_timestamp").alias("last_trans_date"),
    F.count("trans_amount").alias("frequency"),
    F.sum("trans_amount").alias("monetary")
)

users = users.withColumn("recency", F.datediff(F.lit(max_date), F.col("last_trans_date")))

# OPTIONAL: Convert to Pandas for ML
users_df = users.toPandas()

# Add RFM scores (same as before)
users_df['frequency_score'] = pd.qcut(users_df['frequency'], 5, labels=[1, 2, 3, 4, 5])
users_df['monetary_score'] = pd.qcut(users_df['monetary'], 5, labels=[1, 2, 3, 4, 5])
users_df['recency_score'] = users_df['recency'].apply(lambda x: 1 if x > 30 else 5)

# Perform clustering
features = ['frequency', 'monetary', 'recency']
scaler = StandardScaler()
scaled_features = scaler.fit_transform(users_df[features])
model = AgglomerativeClustering(n_clusters=3)
users_df['cluster'] = model.fit_predict(scaled_features)


# Map cluster labels to descriptive names
label_map = {
    0: 'Best Users',
    1: 'Loyal Users',
    2: 'At-Risk Users',
}

# Apply mapping to create a new column
users_df['cluster'] = users_df['cluster'].map(label_map)

# Get age information from dim_users table
# Convert dim_users to pandas to get age information
dim_users_df = dim_users.toPandas()

# Merge users_df with dim_users_df to get age information
users_with_age = users_df.merge(
    dim_users_df[['user_id', 'age']], 
    on='user_id', 
    how='left'
)

# Calculate age count by cluster
age_count_data = users_with_age.groupby(['age', 'cluster']).size().reset_index(name='count')

# Convert back to Spark DataFrame for writing to data mart
age_count_spark = spark.createDataFrame(age_count_data)

# Write to age_count table in data mart
age_count_spark.write.jdbc(
    url=jdbc_dm_url,
    table="age_count",
    mode="overwrite",  # or "append" depending on your needs
    properties=jdbc_properties_dm
)