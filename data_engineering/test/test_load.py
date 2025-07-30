import pymssql
import pytest

# Configuration
DB_CONFIG = {
    "server": "localhost:1434",  # Make sure port matches docker-compose
    "user": "SA",
    "password": "YourStrong!Passw0rd",
    "database": "CustomerWarehouse"
}

@pytest.fixture(scope="module")
def connection():
    try:
        conn = pymssql.connect(**DB_CONFIG)
        yield conn
        conn.close()
    except pymssql.Error as e:
        pytest.fail(f"Database connection failed: {e}")

def test_tables_exist(connection):
    """
    Check that the expected tables exist in the database.

    This test will fail if any of the tables do not exist in the database.
    """

    cursor = connection.cursor()
    tables = ['dim_users', 'dim_card', 'fact_trans']
    for table in tables:
        cursor.execute(f"""
            SELECT COUNT(*) 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_NAME = '{table}';
        """)
        result = cursor.fetchone()
        assert result[0] == 1, f"Table '{table}' does not exist."
    cursor.close()

def test_table_data_not_empty(connection):
    cursor = connection.cursor()
    tables = ['dim_users', 'dim_card', 'fact_trans']
    for table in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table};")
        result = cursor.fetchone()
        assert result[0] > 0, f"Table '{table}' is empty."
    cursor.close()

def test_fact_trans_user_id_fk(connection):
    cursor = connection.cursor()
    # Check if there are any user_ids in fact_trans not in dim_users
    cursor.execute("""
        SELECT COUNT(*) FROM fact_trans ft
        LEFT JOIN dim_users du ON ft.user_id = du.user_id
        WHERE du.user_id IS NULL;
    """)
    result = cursor.fetchone()
    assert result[0] == 0, "There are invalid user_id foreign keys in fact_trans."
    cursor.close()

def test_fact_trans_card_id_fk(connection):
    cursor = connection.cursor()
    # Check if there are any card_ids in fact_trans not in dim_card
    cursor.execute("""
        SELECT COUNT(*) FROM fact_trans ft
        LEFT JOIN dim_card dc ON ft.card_id = dc.card_id
        WHERE dc.card_id IS NULL;
    """)
    result = cursor.fetchone()
    assert result[0] == 0, "There are invalid card_id foreign keys in fact_trans."
    cursor.close()