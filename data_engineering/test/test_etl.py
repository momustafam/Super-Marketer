import pymssql
import pytest
import pandas as pd

# Configuration for MarketingDataMart
DB_CONFIG = {
    "server": "localhost:1435",  
    "user": "SA",
    "password": "Password1234!",
    "database": "MarketingDataMart"
}

@pytest.fixture(scope="module")
def connection():
    """Create database connection for testing"""
    try:
        conn = pymssql.connect(**DB_CONFIG)
        yield conn
        conn.close()
    except pymssql.Error as e:
        pytest.fail(f"❌ Failed to connect to MarketingDataMart: {e}")

def test_age_count_top_10_rows(connection):
    """Test to access and print the top 10 rows of the age_count table"""
    cursor = connection.cursor()
    
    try:
        # Query to get the top 10 rows from age_count table
        query = """
        SELECT TOP 10 age, count, cluster 
        FROM age_count 
        ORDER BY count DESC;
        """
        
        cursor.execute(query)
        results = cursor.fetchall()
        
        # Print the results
        print("\n" + "="*50)
        print("TOP 10 ROWS FROM AGE_COUNT TABLE")
        print("="*50)
        
        if results:
            # Print header
            print(f"{'Age':<5} {'Count':<10} {'Cluster':<20}")
            print("-" * 40)
            
            # Print each row
            for row in results:
                age, count, cluster = row
                print(f"{age:<5} {count:<10} {cluster:<20}")
                
            print("="*50)
            print(f"Total rows retrieved: {len(results)}")
        else:
            print("No data found in age_count table")
            
        # Assert that we can connect and query the table
        assert True, "Successfully accessed age_count table"
        
    except pymssql.Error as e:
        pytest.fail(f"❌ Failed to query age_count table: {e}")
    finally:
        cursor.close()

def test_age_count_table_structure(connection):
    """Test to verify the structure of the age_count table"""
    cursor = connection.cursor()
    
    try:
        # Check if age_count table exists and has the expected columns
        cursor.execute("""
            SELECT COLUMN_NAME, DATA_TYPE 
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_NAME = 'age_count'
            ORDER BY ORDINAL_POSITION;
        """)
        
        columns = cursor.fetchall()
        
        print("\n" + "="*50)
        print("AGE_COUNT TABLE STRUCTURE")
        print("="*50)
        
        expected_columns = ['age', 'count', 'cluster']
        column_names = [col[0] for col in columns]
        
        print(f"{'Column Name':<15} {'Data Type':<15}")
        print("-" * 30)
        
        for col_name, data_type in columns:
            print(f"{col_name:<15} {data_type:<15}")
            
        print("="*50)
        
        # Verify expected columns exist
        for expected_col in expected_columns:
            assert expected_col in column_names, f"Column '{expected_col}' not found in age_count table"
            
        print("✅ All expected columns found in age_count table")
        
    except pymssql.Error as e:
        pytest.fail(f"❌ Failed to check age_count table structure: {e}")
    finally:
        cursor.close()

def test_age_count_data_summary(connection):
    """Test to get summary statistics of the age_count table"""
    cursor = connection.cursor()
    
    try:
        # Get summary statistics
        cursor.execute("""
            SELECT 
                COUNT(*) as total_rows,
                MIN(age) as min_age,
                MAX(age) as max_age,
                SUM(count) as total_count,
                COUNT(DISTINCT cluster) as unique_clusters
            FROM age_count;
        """)
        
        result = cursor.fetchone()
        
        if result:
            total_rows, min_age, max_age, total_count, unique_clusters = result
            
            print("\n" + "="*50)
            print("AGE_COUNT TABLE SUMMARY")
            print("="*50)
            print(f"Total rows: {total_rows}")
            print(f"Age range: {min_age} - {max_age}")
            print(f"Total count: {total_count}")
            print(f"Unique clusters: {unique_clusters}")
            print("="*50)
        
        assert True, "Successfully retrieved age_count summary statistics"
        
    except pymssql.Error as e:
        pytest.fail(f"❌ Failed to get age_count summary: {e}")
    finally:
        cursor.close()

def test_trans_by_day_top_10_rows(connection):
    """Test to access and print the top 10 rows of the trans_by_day table"""
    cursor = connection.cursor()
    
    try:
        # Query to get the top 10 rows from trans_by_day table
        query = """
        SELECT TOP 10 hour, count 
        FROM trans_by_day 
        ORDER BY count DESC;
        """
        
        cursor.execute(query)
        results = cursor.fetchall()
        
        # Print the results
        print("\n" + "="*50)
        print("TOP 10 ROWS FROM TRANS_BY_DAY TABLE")
        print("="*50)
        
        if results:
            # Print header
            print(f"{'Hour':<6} {'Count':<10}")
            print("-" * 20)
            
            # Print each row
            for row in results:
                hour, count = row
                print(f"{hour:<6} {count:<10}")
                
            print("="*50)
            print(f"Total rows retrieved: {len(results)}")
        else:
            print("No data found in trans_by_day table")
            
        # Assert that we can connect and query the table
        assert True, "Successfully accessed trans_by_day table"
        
    except pymssql.Error as e:
        pytest.fail(f"❌ Failed to query trans_by_day table: {e}")
    finally:
        cursor.close()

def test_trans_by_day_table_structure(connection):
    """Test to verify the structure of the trans_by_day table"""
    cursor = connection.cursor()
    
    try:
        # Check if trans_by_day table exists and has the expected columns
        cursor.execute("""
            SELECT COLUMN_NAME, DATA_TYPE 
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_NAME = 'trans_by_day'
            ORDER BY ORDINAL_POSITION;
        """)
        
        columns = cursor.fetchall()
        
        print("\n" + "="*50)
        print("TRANS_BY_DAY TABLE STRUCTURE")
        print("="*50)
        
        expected_columns = ['hour', 'count']
        column_names = [col[0] for col in columns]
        
        print(f"{'Column Name':<15} {'Data Type':<15}")
        print("-" * 30)
        
        for col_name, data_type in columns:
            print(f"{col_name:<15} {data_type:<15}")
            
        print("="*50)
        
        # Verify expected columns exist
        for expected_col in expected_columns:
            assert expected_col in column_names, f"Column '{expected_col}' not found in trans_by_day table"
            
        print("✅ All expected columns found in trans_by_day table")
        
    except pymssql.Error as e:
        pytest.fail(f"❌ Failed to check trans_by_day table structure: {e}")
    finally:
        cursor.close()

def test_trans_by_day_data_summary(connection):
    """Test to get summary statistics of the trans_by_day table"""
    cursor = connection.cursor()
    
    try:
        # Get summary statistics
        cursor.execute("""
            SELECT 
                COUNT(*) as total_hours,
                MIN(hour) as min_hour,
                MAX(hour) as max_hour,
                SUM(count) as total_transactions,
                AVG(CAST(count AS FLOAT)) as avg_transactions_per_hour
            FROM trans_by_day;
        """)
        
        result = cursor.fetchone()
        
        if result:
            total_hours, min_hour, max_hour, total_transactions, avg_transactions = result
            
            print("\n" + "="*50)
            print("TRANS_BY_DAY TABLE SUMMARY")
            print("="*50)
            print(f"Total hours with data: {total_hours}")
            print(f"Hour range: {min_hour} - {max_hour}")
            print(f"Total transactions: {total_transactions}")
            print(f"Average transactions per hour: {avg_transactions:.2f}")
            print("="*50)
        
        assert True, "Successfully retrieved trans_by_day summary statistics"
        
    except pymssql.Error as e:
        pytest.fail(f"❌ Failed to get trans_by_day summary: {e}")
    finally:
        cursor.close()

def test_trans_by_day_hourly_distribution(connection):
    """Test to show the complete hourly distribution of transactions"""
    cursor = connection.cursor()
    
    try:
        # Get all hours ordered by hour
        cursor.execute("""
            SELECT hour, count 
            FROM trans_by_day 
            ORDER BY hour;
        """)
        
        results = cursor.fetchall()
        
        print("\n" + "="*50)
        print("COMPLETE HOURLY TRANSACTION DISTRIBUTION")
        print("="*50)
        
        if results:
            print(f"{'Hour':<6} {'Count':<8} {'Bar Chart':<30}")
            print("-" * 50)
            
            # Find max count for scaling the bar chart
            max_count = max(row[1] for row in results) if results else 1
            
            for hour, count in results:
                # Create a simple bar chart using asterisks
                bar_length = int((count / max_count) * 20) if max_count > 0 else 0
                bar = "*" * bar_length
                print(f"{hour:<6} {count:<8} {bar:<30}")
                
            print("="*50)
            print(f"Total hours: {len(results)}")
            print("Note: Each '*' represents approximately {:.1f} transactions".format(max_count/20 if max_count > 0 else 0))
        else:
            print("No data found in trans_by_day table")
            
        # Verify we have data for all 24 hours (or at least some hours)
        assert len(results) > 0, "No hourly data found"
        assert all(0 <= row[0] <= 23 for row in results), "Invalid hour values found"
        
    except pymssql.Error as e:
        pytest.fail(f"❌ Failed to query hourly distribution: {e}")
    finally:
        cursor.close()

def test_gender_count_top_10_rows(connection):
    """Test to access and print the top 10 rows of the gender_count table"""
    cursor = connection.cursor()
    
    try:
        # Query to get the top 10 rows from gender_count table
        query = """
        SELECT TOP 10 gender, count, cluster 
        FROM gender_count 
        ORDER BY count DESC;
        """
        
        cursor.execute(query)
        results = cursor.fetchall()
        
        # Print the results
        print("\n" + "="*50)
        print("TOP 10 ROWS FROM GENDER_COUNT TABLE")
        print("="*50)
        
        if results:
            # Print header
            print(f"{'Gender':<8} {'Count':<10} {'Cluster':<20}")
            print("-" * 40)
            
            # Print each row
            for row in results:
                gender, count, cluster = row
                cluster_str = cluster if cluster is not None else "NULL"
                print(f"{gender:<8} {count:<10} {cluster_str:<20}")
                
            print("="*50)
            print(f"Total rows retrieved: {len(results)}")
        else:
            print("No data found in gender_count table")
            
        # Assert that we can connect and query the table
        assert True, "Successfully accessed gender_count table"
        
    except pymssql.Error as e:
        pytest.fail(f"❌ Failed to query gender_count table: {e}")
    finally:
        cursor.close()

def test_gender_count_table_structure(connection):
    """Test to verify the structure of the gender_count table"""
    cursor = connection.cursor()
    
    try:
        # Check if gender_count table exists and has the expected columns
        cursor.execute("""
            SELECT COLUMN_NAME, DATA_TYPE 
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_NAME = 'gender_count'
            ORDER BY ORDINAL_POSITION;
        """)
        
        columns = cursor.fetchall()
        
        print("\n" + "="*50)
        print("GENDER_COUNT TABLE STRUCTURE")
        print("="*50)
        
        expected_columns = ['gender', 'count', 'cluster']
        column_names = [col[0] for col in columns]
        
        print(f"{'Column Name':<15} {'Data Type':<15}")
        print("-" * 30)
        
        for col_name, data_type in columns:
            print(f"{col_name:<15} {data_type:<15}")
            
        print("="*50)
        
        # Verify expected columns exist
        for expected_col in expected_columns:
            assert expected_col in column_names, f"Column '{expected_col}' not found in gender_count table"
            
        print("✅ All expected columns found in gender_count table")
        
    except pymssql.Error as e:
        pytest.fail(f"❌ Failed to check gender_count table structure: {e}")
    finally:
        cursor.close()

def test_gender_count_data_summary(connection):
    """Test to get summary statistics of the gender_count table"""
    cursor = connection.cursor()
    
    try:
        # Get summary statistics
        cursor.execute("""
            SELECT 
                COUNT(*) as total_rows,
                SUM(count) as total_users,
                COUNT(DISTINCT gender) as unique_genders,
                COUNT(DISTINCT cluster) as unique_clusters
            FROM gender_count;
        """)
        
        result = cursor.fetchone()
        
        if result:
            total_rows, total_users, unique_genders, unique_clusters = result
            
            print("\n" + "="*50)
            print("GENDER_COUNT TABLE SUMMARY")
            print("="*50)
            print(f"Total rows: {total_rows}")
            print(f"Total users: {total_users}")
            print(f"Unique genders: {unique_genders}")
            print(f"Unique clusters: {unique_clusters}")
            
            # Get gender distribution
            cursor.execute("""
                SELECT gender, SUM(count) as total_count
                FROM gender_count 
                GROUP BY gender
                ORDER BY total_count DESC;
            """)
            
            gender_dist = cursor.fetchall()
            print("\nGender Distribution:")
            for gender, count in gender_dist:
                print(f"  {gender}: {count} users")
            
            print("="*50)
        
        assert True, "Successfully retrieved gender_count summary statistics"
        
    except pymssql.Error as e:
        pytest.fail(f"❌ Failed to get gender_count summary: {e}")
    finally:
        cursor.close()

def test_service_count_top_10_rows(connection):
    """Test to access and print the top 10 rows of the service_count table"""
    cursor = connection.cursor()
    
    try:
        # Query to get the top 10 rows from service_count table
        query = """
        SELECT TOP 10 [service], count 
        FROM service_count 
        ORDER BY count DESC;
        """
        
        cursor.execute(query)
        results = cursor.fetchall()
        
        # Print the results
        print("\n" + "="*50)
        print("TOP 10 ROWS FROM SERVICE_COUNT TABLE")
        print("="*50)
        
        if results:
            # Print header
            print(f"{'Service':<20} {'Count':<10}")
            print("-" * 32)
            
            # Print each row
            for row in results:
                service, count = row
                print(f"{service:<20} {count:<10}")
                
            print("="*50)
            print(f"Total rows retrieved: {len(results)}")
        else:
            print("No data found in service_count table")
            
        # Assert that we can connect and query the table
        assert True, "Successfully accessed service_count table"
        
    except pymssql.Error as e:
        pytest.fail(f"❌ Failed to query service_count table: {e}")
    finally:
        cursor.close()

def test_service_count_table_structure(connection):
    """Test to verify the structure of the service_count table"""
    cursor = connection.cursor()
    
    try:
        # Check if service_count table exists and has the expected columns
        cursor.execute("""
            SELECT COLUMN_NAME, DATA_TYPE 
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_NAME = 'service_count'
            ORDER BY ORDINAL_POSITION;
        """)
        
        columns = cursor.fetchall()
        
        print("\n" + "="*50)
        print("SERVICE_COUNT TABLE STRUCTURE")
        print("="*50)
        
        expected_columns = ['service', 'count']
        column_names = [col[0] for col in columns]
        
        print(f"{'Column Name':<15} {'Data Type':<15}")
        print("-" * 30)
        
        for col_name, data_type in columns:
            print(f"{col_name:<15} {data_type:<15}")
            
        print("="*50)
        
        # Verify expected columns exist
        for expected_col in expected_columns:
            assert expected_col in column_names, f"Column '{expected_col}' not found in service_count table"
            
        print("✅ All expected columns found in service_count table")
        
    except pymssql.Error as e:
        pytest.fail(f"❌ Failed to check service_count table structure: {e}")
    finally:
        cursor.close()

def test_service_count_data_summary(connection):
    """Test to get summary statistics of the service_count table"""
    cursor = connection.cursor()
    
    try:
        # Get summary statistics
        cursor.execute("""
            SELECT 
                COUNT(*) as total_services,
                SUM(count) as total_transactions,
                AVG(CAST(count AS FLOAT)) as avg_transactions_per_service,
                MAX(count) as max_transactions,
                MIN(count) as min_transactions
            FROM service_count;
        """)
        
        result = cursor.fetchone()
        
        if result:
            total_services, total_transactions, avg_transactions, max_transactions, min_transactions = result
            
            print("\n" + "="*50)
            print("SERVICE_COUNT TABLE SUMMARY")
            print("="*50)
            print(f"Total services: {total_services}")
            print(f"Total transactions: {total_transactions}")
            print(f"Average transactions per service: {avg_transactions:.2f}")
            print(f"Most popular service transactions: {max_transactions}")
            print(f"Least popular service transactions: {min_transactions}")
            print("="*50)
        
        assert True, "Successfully retrieved service_count summary statistics"
        
    except pymssql.Error as e:
        pytest.fail(f"❌ Failed to get service_count summary: {e}")
    finally:
        cursor.close()

def test_service_count_distribution(connection):
    """Test to show the complete service distribution with visual representation"""
    cursor = connection.cursor()
    
    try:
        # Get all services ordered by count
        cursor.execute("""
            SELECT [service], count 
            FROM service_count 
            ORDER BY count DESC;
        """)
        
        results = cursor.fetchall()
        
        print("\n" + "="*60)
        print("COMPLETE SERVICE USAGE DISTRIBUTION")
        print("="*60)
        
        if results:
            print(f"{'Service':<25} {'Count':<8} {'Bar Chart':<25}")
            print("-" * 60)
            
            # Find max count for scaling the bar chart
            max_count = max(row[1] for row in results) if results else 1
            
            for service, count in results:
                # Create a simple bar chart using asterisks
                bar_length = int((count / max_count) * 20) if max_count > 0 else 0
                bar = "*" * bar_length
                # Truncate service name if too long
                service_name = service[:24] if service else "Unknown"
                print(f"{service_name:<25} {count:<8} {bar:<25}")
                
            print("="*60)
            print(f"Total services: {len(results)}")
            print("Note: Each '*' represents approximately {:.1f} transactions".format(max_count/20 if max_count > 0 else 0))
        else:
            print("No data found in service_count table")
            
        # Verify we have service data
        assert len(results) > 0, "No service data found"
        
    except pymssql.Error as e:
        pytest.fail(f"❌ Failed to query service distribution: {e}")
    finally:
        cursor.close()

def test_user_geo_top_10_rows(connection):
    """Test to access and print the top 10 rows of the user_geo table"""
    cursor = connection.cursor()
    
    try:
        # Query to get the top 10 rows from user_geo table
        query = """
        SELECT TOP 10 longitude, latitude, city, country, cluster 
        FROM user_geo 
        ORDER BY city, country;
        """
        
        cursor.execute(query)
        results = cursor.fetchall()
        
        # Print the results
        print("\n" + "="*70)
        print("TOP 10 ROWS FROM USER_GEO TABLE")
        print("="*70)
        
        if results:
            # Print header
            print(f"{'Longitude':<12} {'Latitude':<12} {'City':<15} {'Country':<15} {'Cluster':<15}")
            print("-" * 70)
            
            # Print each row
            for row in results:
                longitude, latitude, city, country, cluster = row
                longitude_str = f"{longitude:.4f}" if longitude else "N/A"
                latitude_str = f"{latitude:.4f}" if latitude else "N/A"
                city_str = city[:14] if city else "N/A"
                country_str = country[:14] if country else "N/A"
                cluster_str = cluster[:14] if cluster else "N/A"
                print(f"{longitude_str:<12} {latitude_str:<12} {city_str:<15} {country_str:<15} {cluster_str:<15}")
                
            print("="*70)
            print(f"Total rows retrieved: {len(results)}")
        else:
            print("No data found in user_geo table")
            
        # Assert that we can connect and query the table
        assert True, "Successfully accessed user_geo table"
        
    except pymssql.Error as e:
        pytest.fail(f"❌ Failed to query user_geo table: {e}")
    finally:
        cursor.close()

def test_user_geo_table_structure(connection):
    """Test to verify the structure of the user_geo table"""
    cursor = connection.cursor()
    
    try:
        # Check if user_geo table exists and has the expected columns
        cursor.execute("""
            SELECT COLUMN_NAME, DATA_TYPE 
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_NAME = 'user_geo'
            ORDER BY ORDINAL_POSITION;
        """)
        
        columns = cursor.fetchall()
        
        print("\n" + "="*50)
        print("USER_GEO TABLE STRUCTURE")
        print("="*50)
        
        expected_columns = ['longitude', 'latitude', 'city', 'country', 'cluster']
        column_names = [col[0] for col in columns]
        
        print(f"{'Column Name':<15} {'Data Type':<15}")
        print("-" * 30)
        
        for col_name, data_type in columns:
            print(f"{col_name:<15} {data_type:<15}")
            
        print("="*50)
        
        # Verify expected columns exist
        for expected_col in expected_columns:
            assert expected_col in column_names, f"Column '{expected_col}' not found in user_geo table"
            
        print("✅ All expected columns found in user_geo table")
        
    except pymssql.Error as e:
        pytest.fail(f"❌ Failed to check user_geo table structure: {e}")
    finally:
        cursor.close()

def test_user_geo_data_summary(connection):
    """Test to get summary statistics of the user_geo table"""
    cursor = connection.cursor()
    
    try:
        # Get summary statistics
        cursor.execute("""
            SELECT 
                COUNT(*) as total_users,
                COUNT(DISTINCT city) as unique_cities,
                COUNT(DISTINCT country) as unique_countries,
                COUNT(DISTINCT cluster) as unique_clusters,
                MIN(longitude) as min_longitude,
                MAX(longitude) as max_longitude,
                MIN(latitude) as min_latitude,
                MAX(latitude) as max_latitude
            FROM user_geo;
        """)
        
        result = cursor.fetchone()
        
        if result:
            total_users, unique_cities, unique_countries, unique_clusters, min_lon, max_lon, min_lat, max_lat = result
            
            print("\n" + "="*50)
            print("USER_GEO TABLE SUMMARY")
            print("="*50)
            print(f"Total users: {total_users}")
            print(f"Unique cities: {unique_cities}")
            print(f"Unique countries: {unique_countries}")
            print(f"Unique clusters: {unique_clusters}")
            if min_lon is not None and max_lon is not None:
                print(f"Longitude range: {min_lon:.4f} to {max_lon:.4f}")
            if min_lat is not None and max_lat is not None:
                print(f"Latitude range: {min_lat:.4f} to {max_lat:.4f}")
            
            # Get top countries by user count
            cursor.execute("""
                SELECT TOP 5 country, COUNT(*) as user_count
                FROM user_geo 
                WHERE country IS NOT NULL
                GROUP BY country
                ORDER BY user_count DESC;
            """)
            
            country_dist = cursor.fetchall()
            if country_dist:
                print("\nTop 5 Countries by User Count:")
                for country, count in country_dist:
                    print(f"  {country}: {count} users")
            
            print("="*50)
        
        assert True, "Successfully retrieved user_geo summary statistics"
        
    except pymssql.Error as e:
        pytest.fail(f"❌ Failed to get user_geo summary: {e}")
    finally:
        cursor.close()

def test_user_geo_cluster_distribution(connection):
    """Test to show geographical distribution by cluster"""
    cursor = connection.cursor()
    
    try:
        # Get cluster distribution by country
        cursor.execute("""
            SELECT cluster, country, COUNT(*) as user_count
            FROM user_geo 
            WHERE country IS NOT NULL AND cluster IS NOT NULL
            GROUP BY cluster, country
            ORDER BY cluster, user_count DESC;
        """)
        
        results = cursor.fetchall()
        
        print("\n" + "="*60)
        print("GEOGRAPHICAL DISTRIBUTION BY CLUSTER")
        print("="*60)
        
        if results:
            current_cluster = None
            for cluster, country, count in results:
                if cluster != current_cluster:
                    if current_cluster is not None:
                        print("-" * 40)
                    print(f"\n{cluster}:")
                    current_cluster = cluster
                print(f"  {country}: {count} users")
                
            print("="*60)
            print(f"Total cluster-country combinations: {len(results)}")
        else:
            print("No geographical cluster data found")
            
        # Verify we have geographical data
        assert len(results) > 0, "No geographical cluster data found"
        
    except pymssql.Error as e:
        pytest.fail(f"❌ Failed to query geographical cluster distribution: {e}")
    finally:
        cursor.close()

def test_stats_data(connection):
    """Test to access and display the stats table data"""
    cursor = connection.cursor()
    
    try:
        # Query the stats table
        query = """
        SELECT new_users, returning_users, new_transactions, avg_clv 
        FROM stats;
        """
        
        cursor.execute(query)
        results = cursor.fetchall()
        
        # Print the results
        print("\n" + "="*70)
        print("DASHBOARD STATISTICS")
        print("="*70)
        
        if results:
            # Should only be one row
            for row in results:
                new_users, returning_users, new_transactions, avg_clv = row
                
                print(f"📊 KEY PERFORMANCE INDICATORS")
                print("-" * 40)
                print(f"New Users:           {new_users:,}")
                print(f"Returning Users:     {returning_users:,}")
                print(f"New Transactions:    {new_transactions:,}")
                print(f"Average CLV:         ${avg_clv:,.2f}")
                print("-" * 40)
                
                # Calculate some additional metrics
                total_users = new_users + returning_users
                retention_rate = (returning_users / new_users * 100) if new_users > 0 else 0
                transactions_per_user = (new_transactions / total_users) if total_users > 0 else 0
                
                print(f"📈 CALCULATED METRICS")
                print("-" * 40)
                print(f"Total Users:         {total_users:,}")
                print(f"User Retention Rate: {retention_rate:.1f}%")
                print(f"Transactions/User:   {transactions_per_user:.2f}")
                
            print("="*70)
            print(f"Total rows in stats table: {len(results)}")
            
            # Verify we have exactly one row
            assert len(results) == 1, f"Expected exactly 1 row in stats table, got {len(results)}"
            
        else:
            print("No data found in stats table")
            pytest.fail("Stats table should contain exactly one row")
            
        # Assert that we can connect and query the table
        assert True, "Successfully accessed stats table"
        
    except pymssql.Error as e:
        pytest.fail(f"❌ Failed to query stats table: {e}")
    finally:
        cursor.close()

def test_stats_table_structure(connection):
    """Test to verify the structure of the stats table"""
    cursor = connection.cursor()
    
    try:
        # Check if stats table exists and has the expected columns
        cursor.execute("""
            SELECT COLUMN_NAME, DATA_TYPE 
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_NAME = 'stats'
            ORDER BY ORDINAL_POSITION;
        """)
        
        columns = cursor.fetchall()
        
        print("\n" + "="*50)
        print("STATS TABLE STRUCTURE")
        print("="*50)
        
        expected_columns = ['new_users', 'returning_users', 'new_transactions', 'avg_clv']
        column_names = [col[0] for col in columns]
        
        print(f"{'Column Name':<20} {'Data Type':<15}")
        print("-" * 35)
        
        for col_name, data_type in columns:
            print(f"{col_name:<20} {data_type:<15}")
            
        print("="*50)
        
        # Verify expected columns exist
        for expected_col in expected_columns:
            assert expected_col in column_names, f"Column '{expected_col}' not found in stats table"
            
        print("✅ All expected columns found in stats table")
        
    except pymssql.Error as e:
        pytest.fail(f"❌ Failed to check stats table structure: {e}")
    finally:
        cursor.close()

def test_stats_data_validation(connection):
    """Test to validate the business logic of stats data"""
    cursor = connection.cursor()
    
    try:
        # Get stats data for validation
        cursor.execute("""
            SELECT new_users, returning_users, new_transactions, avg_clv 
            FROM stats;
        """)
        
        result = cursor.fetchone()
        
        if result:
            new_users, returning_users, new_transactions, avg_clv = result
            
            print("\n" + "="*50)
            print("STATS DATA VALIDATION")
            print("="*50)
            
            # Validate business rules
            validation_results = []
            
            # Check if returning_users is approximately new_users // 4
            expected_returning = new_users // 4
            validation_results.append(("Returning users calculation", returning_users == expected_returning))
            
            # Check if all values are non-negative
            validation_results.append(("New users >= 0", new_users >= 0))
            validation_results.append(("Returning users >= 0", returning_users >= 0))
            validation_results.append(("New transactions >= 0", new_transactions >= 0))
            validation_results.append(("Average CLV >= 0", avg_clv >= 0))
            
            # Check if values are reasonable
            validation_results.append(("New users > 0", new_users > 0))
            validation_results.append(("Average CLV > 0", avg_clv > 0))
            
            # Print validation results
            print("Business Logic Validation:")
            print("-" * 30)
            for test_name, passed in validation_results:
                status = "✅ PASS" if passed else "❌ FAIL"
                print(f"{test_name:<25} {status}")
            
            print("\nDetailed Values:")
            print("-" * 30)
            print(f"New Users:        {new_users:,}")
            print(f"Expected Returning: {expected_returning:,}")
            print(f"Actual Returning: {returning_users:,}")
            print(f"New Transactions: {new_transactions:,}")
            print(f"Average CLV:      ${avg_clv:,.2f}")
            
            print("="*50)
            
            # Assert critical validations
            assert new_users > 0, "New users should be greater than 0"
            assert returning_users >= 0, "Returning users should be non-negative"
            assert new_transactions >= 0, "New transactions should be non-negative"
            assert avg_clv >= 0, "Average CLV should be non-negative"
            assert returning_users == expected_returning, f"Returning users should be {expected_returning}, got {returning_users}"
            
        else:
            pytest.fail("No data found in stats table for validation")
        
        assert True, "Successfully validated stats data"
        
    except pymssql.Error as e:
        pytest.fail(f"❌ Failed to validate stats data: {e}")
    finally:
        cursor.close()
        
if __name__ == "__main__":
    print("Running all ETL tests...")
    # Run all tests in this file with verbose output
    exit_code = pytest.main([__file__, "--verbose"])