#!/bin/bash

# Airflow Initialization Script
# This script initializes the Airflow database and creates default admin user

echo "Initializing Airflow..."

# Initialize the database
airflow db init

# Create admin user
airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@supermarketer.com

# Create pools for task management
airflow pools set extraction_pool 2 "Pool for data extraction tasks"
airflow pools set transformation_pool 1 "Pool for data transformation tasks"  
airflow pools set analytics_pool 4 "Pool for analytics transformation tasks"

echo "Airflow initialization completed!"
echo "Access Airflow Web UI at: http://localhost:8080"
echo "Username: admin"
echo "Password: admin"
