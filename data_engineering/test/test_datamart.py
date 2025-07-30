import pymssql
import pytest

# Configuration
# Configuration
DB_CONFIG = {
    "server": "localhost:1435",  
    "user": "SA",
    "password": "Password1234!",
    "database": "MarketingDataMart"
}

# List of expected tables
EXPECTED_TABLES = [
    "age_count",
    "gender_count",
    "service_count",
    "user_geo",
    "trans_by_day",
    "stats"
]

@pytest.fixture(scope="module")
def connection():
    try:
        conn = pymssql.connect(**DB_CONFIG)
        yield conn
        conn.close()
    except pymssql.Error as e:
        pytest.fail(f"❌ Failed to connect to MarketingDataMart: {e}")

def test_datamart_tables_exist(connection):
    cursor = connection.cursor()
    for table in EXPECTED_TABLES:
        cursor.execute(f"""
            SELECT COUNT(*) 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_NAME = '{table}';
        """)
        result = cursor.fetchone()
        assert result[0] == 1, f"❌ Table '{table}' does not exist in MarketingDataMart."
    cursor.close()

def test_datamart_tables_have_columns(connection):
    cursor = connection.cursor()
    # Each table and at least one column check
    for table in EXPECTED_TABLES:
        cursor.execute(f"""
            SELECT COUNT(*) 
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_NAME = '{table}';
        """)
        result = cursor.fetchone()
        assert result[0] > 0, f"❌ Table '{table}' has no columns defined."
    cursor.close()

@pytest.mark.skip(reason="Enable this if data is expected to be loaded before testing.")
def test_datamart_tables_not_empty(connection):
    cursor = connection.cursor()
    for table in EXPECTED_TABLES:
        cursor.execute(f"SELECT COUNT(*) FROM {table};")
        result = cursor.fetchone()
        assert result[0] > 0, f"⚠️ Table '{table}' is empty."
    cursor.close()
