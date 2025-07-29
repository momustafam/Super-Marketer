import pyodbc
import pandas as pd

# Database connection parameters
server = 'sqlserver_warehouse,1433'  # Adjust if using different port
database = 'CustomerWarehouse'
username = 'SA'
password = 'YourStrong!Passw0rd'

# Establish connection
conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'
conn = pyodbc.connect(conn_str)

print('--- Starting Data Validation Tests for fact_trans ---')

# 1. Total row count
print('1. ✅ Total row count in fact_trans table')
df = pd.read_sql("SELECT COUNT(*) AS total_rows FROM fact_trans", conn)
print(df, '\n')

# 2. NULLs in critical columns
print('2. ✅ Check for NULLs in critical columns')
df = pd.read_sql("""
    SELECT *
    FROM fact_trans
    WHERE id IS NULL OR user_id IS NULL OR card_id IS NULL OR trans_amount IS NULL
""", conn)
print(df if not df.empty else "✅ No NULLs found.\n")

# 3. Duplicate transaction_id
print('3. ✅ Check for duplicate transaction IDs (if should be unique)')
df = pd.read_sql("""
    SELECT id, COUNT(*) AS occurrences
    FROM fact_trans
    GROUP BY id
    HAVING COUNT(*) > 1
""", conn)
print(df if not df.empty else "✅ No duplicate transaction_ids found.\n")

# 4. Preview first 10 rows
print('4. ✅ Preview first 10 rows of data')
df = pd.read_sql("SELECT TOP 10 * FROM fact_trans", conn)
print(df, '\n')

# 5. Min and Max transaction amount
print('5. ✅ Check min and max transaction amount')
df = pd.read_sql("""
    SELECT 
        MIN(trans_amount) AS min_amount, 
        MAX(trans_amount) AS max_amount
    FROM fact_trans
""", conn)
print(df, '\n')

# 6. Aggregated transactions per user (Top 10)
print('6. ✅ Aggregated transactions per user (Top 10)')
df = pd.read_sql("""
    SELECT TOP 10 user_id, COUNT(*) AS num_trans, SUM(trans_amount) AS total_spent
    FROM fact_trans
    GROUP BY user_id
    ORDER BY total_spent DESC
""", conn)
print(df, '\n')

print('--- ✅ Data Validation Tests Completed ---')

conn.close()