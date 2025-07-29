import os 
import pyodbc

query = ""
sql_path = os.path.join("data-engineering", "init_warehouse.sql")
with open(sql_path, "r") as f:
    query= f.read() 
    statements = [s.strip() for s in query.split(';') if s.strip()]
    
# Change these based on your setup
SERVER = 'sqlserver_warehouse,1433'
DATABASE = 'CustomerWarehouse'
USERNAME = 'SA'
PASSWORD = 'YourStrong!Passw0rd'

# Mounted Docker path
base_path = '/data/fact_trans'

# Connect to SQL Server
conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={SERVER};DATABASE={DATABASE};UID={USERNAME};PWD={PASSWORD}'
conn = pyodbc.connect(conn_str, autocommit=True)
cursor = conn.cursor()

for s in statements:
    cursor.execute(s)
    conn.commit()
# Loop through all files (00000 to 00017)
for i in range(18):
    filename = f'/fact_trans_part_{i:05d}.csv'
    full_path = base_path + filename
    print(f'📥 Loading: {full_path}')

    try:
        bulk_sql = f"""
        BULK INSERT dbo.fact_trans
        FROM '{full_path}'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\\n',
            TABLOCK
        )
        """
        cursor.execute(bulk_sql)
        conn.commit()
        print(f'✅ Loaded: {filename}')
    except Exception as e:
        print(f'❌ Failed to load {filename}:\n{e}')

# Optionally check row count
cursor.execute("SELECT COUNT(*) FROM dbo.fact_trans")
count = cursor.fetchone()[0]
print(f'Total rows in fact_trans: {count}')

cursor.close()
conn.close()