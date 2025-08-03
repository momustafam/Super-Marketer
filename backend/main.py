from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Dict, Any, Optional, Union
import os
import logging
from contextlib import contextmanager
from datetime import datetime
import hashlib
import pymssql
import re
from functools import wraps

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)
# App Initialization with Metadata
app = FastAPI(
    title="Super Marketer API",
    version="1.0.0",
    description="Real-time marketing analytics API powered by MarketingDataMart"
)
# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",  # React dev server
        "http://localhost:3001",  # Alternative port
        "http://127.0.0.1:3000",
        os.getenv("FRONTEND_URL", "")  # Production frontend URL
    ],
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],  # Restrict to needed methods
    allow_headers=["*"],
)

# Database configuration -> pymssql
DB_CONFIG = {
    "server": os.getenv("DB_SERVER", "localhost"),
    "port": int(os.getenv("DB_PORT", "1435")),
    "database": os.getenv("DB_NAME", "MarketingDataMart"),
    "user": os.getenv("DB_USER", "SA"),
    "password": os.getenv("DB_PASSWORD", "Password1234!")
}
# generate a unique, consistent color
def generate_consistent_color(text: str) -> str:
    """Generate consistent HSL color from text using MD5 hash"""
    if not text:
        return "hsl(0, 70%, 50%)"

    hash_object = hashlib.md5(str(text).encode())
    hash_hex = hash_object.hexdigest()
    hue = int(hash_hex[:3], 16) % 360  # Use more bits for better distribution
    saturation = 60 + (int(hash_hex[3:4], 16) % 20)  # 60-80% saturation
    lightness = 45 + (int(hash_hex[4:5], 16) % 20)   # 45-65% lightness
    return f"hsl({hue}, {saturation}%, {lightness}%)"
# sanitize and validate any user input
def validate_cluster_input(cluster: str) -> str:
    """Validate cluster parameter to prevent SQL injection"""
    if not cluster:
        return None

    # Allow alphanumeric, underscore, hyphen, and space
    if not re.match(r'^[a-zA-Z0-9_\s-]+$', cluster.strip()):
        # Protects your database from malicious queries
        logger.warning(f"Invalid cluster parameter attempted: {cluster}")
        raise HTTPException(status_code=400, detail="Invalid cluster parameter format")

    return cluster.strip()
# decorator to automatically log
def log_performance(func):
    """Decorator to log function performance"""
    @wraps(func)
    async def wrapper(*args, **kwargs):
        start_time = datetime.now()
        function_name = func.__name__
        logger.info(f"Starting {function_name}")
        # know if it
        try:
            result = await func(*args, **kwargs)
            # how long it took
            duration = (datetime.now() - start_time).total_seconds()
            logger.info(f"✅ {function_name} completed in {duration:.2f}s")
            return result
        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            logger.error(f"❌ {function_name} failed after {duration:.2f}s: {str(e)}")
            raise
    return wrapper

@contextmanager
def get_db_connection():
    """Database connection manager - pymssql exclusively"""
    conn = None
    try:
        logger.info(f"Connecting to {DB_CONFIG['database']} on {DB_CONFIG['server']}:{DB_CONFIG['port']}")

        conn = pymssql.connect(
            server=DB_CONFIG["server"],
            port=DB_CONFIG["port"],
            user=DB_CONFIG["user"],
            password=DB_CONFIG["password"],
            database=DB_CONFIG["database"],
            timeout=30,
            login_timeout=30,
            charset='UTF-8',
            autocommit=True  # Better for read operations
        )

        logger.info("✅ Database connection established")
        yield conn

    except pymssql.Error as e:
        logger.error(f"🔴 Database connection failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Database connection failed: {str(e)}")
    except Exception as e:
        logger.error(f"🔴 Unexpected connection error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Server error: {str(e)}")
    finally:
        if conn:
            try:
                conn.close()
                logger.info("Database connection closed")
            except Exception as e:
                logger.warning(f"Error closing connection: {str(e)}")

def execute_query(query: str, params: tuple = ()) -> List[Dict[str, Any]]:
    """Execute parameterized query - streamlined for pre-cleaned data"""
    query_start_time = datetime.now()

    try:
        with get_db_connection() as conn:
            cursor = conn.cursor(as_dict=True)

            # Log query details (truncate for security)
            query_preview = query.replace('\n', ' ').strip()[:100]
            logger.info(f"Executing query: {query_preview}{'...' if len(query) > 100 else ''}")

            if params:
                logger.info(f"🔧 Parameters: {len(params)} values")
                cursor.execute(query, params)
            else:
                cursor.execute(query)

            # Fetch results (data already clean from ETL)
            rows = cursor.fetchall()

            duration = (datetime.now() - query_start_time).total_seconds()
            logger.info(f"Query returned {len(rows)} rows in {duration:.3f}s")
            return rows

    except pymssql.Error as e:
        duration = (datetime.now() - query_start_time).total_seconds()
        logger.error(f"🔴 Database query failed after {duration:.3f}s: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Database query failed: {str(e)}")
    except Exception as e:
        duration = (datetime.now() - query_start_time).total_seconds()
        logger.error(f"🔴 Query execution failed after {duration:.3f}s: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Query execution failed: {str(e)}")

def build_cluster_filter(cluster: Optional[str], apply_filter: bool = True) -> tuple:
    """Reusable cluster filter builder with optional application"""
    if apply_filter and cluster:  # Only filter if explicitly requested AND cluster provided
        validated_cluster = validate_cluster_input(cluster)
        if validated_cluster:
            return " WHERE cluster = %s", (validated_cluster,)
    return "", ()

def build_advanced_filters(
        cluster: Optional[str] = None,
        enable_cluster_filter: bool = False,
        additional_conditions: Dict[str, Any] = None
) -> tuple:
    """Advanced filter builder with multiple conditions"""
    conditions = []
    params = []

    # Cluster filter (only if enabled)
    if enable_cluster_filter and cluster:
        validated_cluster = validate_cluster_input(cluster)
        if validated_cluster:
            conditions.append("cluster = %s")
            params.append(validated_cluster)

    # Additional filters (future extensibility)
    if additional_conditions:
        for field, value in additional_conditions.items():
            if value is not None:
                conditions.append(f"{field} = %s")
                params.append(value)

    # Build WHERE clause
    if conditions:
        where_clause = " WHERE " + " AND ".join(conditions)
        return where_clause, tuple(params)

    return "", ()

# Chart Data Formatters - Nivo-ready outputs

def format_bar_chart_data(raw_data: List[Dict]) -> List[Dict[str, Any]]:
    """Format data for Nivo ResponsiveBar component"""
    if not raw_data:
        return []

    # Group by index (service) and create cluster columns
    grouped = {}
    clusters = set()

    for row in raw_data:
        index_key = row.get('service', 'Unknown')
        cluster = row.get('cluster', 'default')
        count = row.get('count', 0)

        if index_key not in grouped:
            grouped[index_key] = {'service': index_key}

        grouped[index_key][cluster] = count
        grouped[index_key][f"{cluster}Color"] = generate_consistent_color(cluster)
        clusters.add(cluster)

    logger.info(f"Formatted bar chart: {len(grouped)} services, {len(clusters)} clusters")
    return list(grouped.values())

def format_pie_chart_data(raw_data: List[Dict]) -> List[Dict[str, Any]]:
    """Format data for Nivo ResponsivePie component"""
    if not raw_data:
        return []

    result = []
    for row in raw_data:
        item_id = row.get('gender', 'Unknown')
        result.append({
            "id": item_id,
            "label": item_id,
            "value": row.get('count', 0),
            "color": generate_consistent_color(item_id)
        })

    logger.info(f"Formatted pie chart: {len(result)} segments")
    return result

def format_line_chart_data(raw_data: List[Dict]) -> List[Dict[str, Any]]:
    """Format data for Nivo ResponsiveLine component"""
    if not raw_data:
        return []

    # Group by cluster (series)
    series = {}

    for row in raw_data:
        cluster = row.get('cluster', 'default')
        hour = row.get('hour', 0)
        count = row.get('count', 0)

        if cluster not in series:
            series[cluster] = {
                "id": cluster,
                "color": generate_consistent_color(cluster),
                "data": []
            }

        series[cluster]["data"].append({
            "x": hour,
            "y": count
        })

    # Sort data points by x value
    for cluster_data in series.values():
        cluster_data["data"].sort(key=lambda point: point["x"])

    result = list(series.values())
    logger.info(f"Formatted line chart: {len(result)} series")
    return result

def format_geography_data(raw_data: List[Dict]) -> List[Dict[str, Any]]:
    """Format data for Nivo ResponsiveChoropleth component"""
    if not raw_data:
        return []

    # Simple country name to ISO code mapping -> preparing data for the Nivo ResponsiveChoropleth
    country_codes = {
        "United States": "USA", "Canada": "CAN", "Mexico": "MEX",
        "United Kingdom": "GBR", "France": "FRA", "Germany": "DEU",
        "Spain": "ESP", "Italy": "ITA", "Netherlands": "NLD",
        "Australia": "AUS", "Japan": "JPN", "China": "CHN",
        "Brazil": "BRA", "Argentina": "ARG", "India": "IND",
        "Russia": "RUS", "South Korea": "KOR", "Thailand": "THA"
    }

    result = []
    for row in raw_data:
        country = row.get('country', 'Unknown')
        country_code = country_codes.get(country, country[:3].upper())

        result.append({
            "id": country_code,
            "value": row.get('user_count', 0)
        })

    logger.info(f"Formatted geography chart: {len(result)} countries")
    return result

# Advanced Filter System for Interactive Dashboard

def build_dashboard_filters(
        cluster: Optional[str] = None,
        date_range: Optional[str] = None,
        age_range: Optional[str] = None,
        gender: Optional[str] = None,
        service_type: Optional[str] = None,
        region: Optional[str] = None,
        enable_filters: bool = False
) -> tuple:
    """Comprehensive filter system for interactive dashboard"""
    conditions = []
    params = []

    if not enable_filters:
        return "", ()

    # Cluster filter
    if cluster:
        validated_cluster = validate_cluster_input(cluster)
        if validated_cluster:
            conditions.append("cluster = %s")
            params.append(validated_cluster)

    # Date range filter (last 7d, 30d, 90d, 1y)
    if date_range:
        date_conditions = {
            "7d": "created_date >= DATEADD(day, -7, GETDATE())",
            "30d": "created_date >= DATEADD(day, -30, GETDATE())",
            "90d": "created_date >= DATEADD(day, -90, GETDATE())",
            "1y": "created_date >= DATEADD(year, -1, GETDATE())"
        }
        if date_range in date_conditions:
            conditions.append(date_conditions[date_range])

    # Age range filter
    if age_range:
        age_ranges = {
            "18-25": "age BETWEEN 18 AND 25",
            "26-35": "age BETWEEN 26 AND 35",
            "36-45": "age BETWEEN 36 AND 45",
            "46-55": "age BETWEEN 46 AND 55",
            "55+": "age > 55"
        }
        if age_range in age_ranges:
            conditions.append(age_ranges[age_range])

    # Gender filter
    if gender and gender.lower() in ['male', 'female', 'other']:
        conditions.append("gender = %s")
        params.append(gender.lower())

    # Service type filter
    if service_type:
        conditions.append("service = %s")
        params.append(service_type)

    # Region filter
    if region:
        conditions.append("country = %s")
        params.append(region)

    # Build WHERE clause
    if conditions:
        where_clause = " WHERE " + " AND ".join(conditions)
        return where_clause, tuple(params)

    return "", ()

# Dashboard Data Endpoints

@app.get("/api/charts/service-usage")
@log_performance
async def get_service_usage_data(
        cluster: Optional[str] = Query(None, description="Filter by customer cluster"),
        enable_filter: bool = Query(False, description="Enable cluster filtering")
) -> List[Dict[str, Any]]:
    """Service usage by cluster - Bar chart data with optional filtering"""
    base_query = "SELECT [service], count, cluster FROM service_count"

    # Option 1: Simple filter with enable/disable
    where_clause, params = build_cluster_filter(cluster, apply_filter=enable_filter)

    # Option 2: Advanced filter (for future use)
    # where_clause, params = build_advanced_filters(
    #     cluster=cluster,
    #     enable_cluster_filter=enable_filter,
    #     additional_conditions={"status": "active"}  # Example additional filter
    # )

    query = base_query + where_clause + " ORDER BY [service], cluster"

    raw_data = execute_query(query, params)
    return format_bar_chart_data(raw_data)

@app.get("/api/charts/gender-distribution")
@log_performance
async def get_gender_distribution_data(
        cluster: Optional[str] = Query(None, description="Filter by customer cluster")
) -> List[Dict[str, Any]]:
    """Gender distribution - Pie chart data"""
    base_query = "SELECT gender, count, cluster FROM gender_count"
    where_clause, params = build_cluster_filter(cluster)
    query = base_query + where_clause + " ORDER BY count DESC"

    raw_data = execute_query(query, params)
    return format_pie_chart_data(raw_data)

@app.get("/api/charts/transactions-by-hour")
@log_performance
async def get_transactions_by_hour_data(
        cluster: Optional[str] = Query(None, description="Filter by customer cluster")
) -> List[Dict[str, Any]]:
    """Transaction patterns by hour - Line chart data"""
    base_query = "SELECT hour, count, cluster FROM trans_by_day"
    where_clause, params = build_cluster_filter(cluster)
    query = base_query + where_clause + " ORDER BY cluster, hour"

    raw_data = execute_query(query, params)
    return format_line_chart_data(raw_data)

@app.get("/api/charts/user-geography")
@log_performance
async def get_user_geography_data(
        cluster: Optional[str] = Query(None, description="Filter by customer cluster")
) -> List[Dict[str, Any]]:
    """User geographic distribution - Choropleth chart data"""
    base_query = """
    SELECT 
        country,
        COUNT(*) as user_count,
        cluster
    FROM user_geo
    """
    where_clause, params = build_cluster_filter(cluster)
    query = base_query + where_clause + " GROUP BY country, cluster ORDER BY user_count DESC"

    raw_data = execute_query(query, params)
    return format_geography_data(raw_data)

@app.get("/api/charts/age-distribution")
@log_performance
async def get_age_distribution_data(
        cluster: Optional[str] = Query(None, description="Filter by customer cluster")
) -> List[Dict[str, Any]]:
    """Age distribution by cluster - Bar chart data"""
    base_query = "SELECT age, count, cluster FROM age_count"
    where_clause, params = build_cluster_filter(cluster)
    query = base_query + where_clause + " ORDER BY age"

    raw_data = execute_query(query, params)
    return format_bar_chart_data(raw_data)

# COMPREHENSIVE DASHBOARD API

@app.get("/api/dashboard/overview")
@log_performance
async def get_dashboard_overview(
        cluster: Optional[str] = Query(None, description="Filter by customer cluster"),
        date_range: Optional[str] = Query(None, description="Date range: 7d, 30d, 90d, 1y"),
        age_range: Optional[str] = Query(None, description="Age range: 18-25, 26-35, 36-45, 46-55, 55+"),
        gender: Optional[str] = Query(None, description="Gender: male, female, other"),
        service_type: Optional[str] = Query(None, description="Service type filter"),
        region: Optional[str] = Query(None, description="Geographic region filter"),
        enable_filters: bool = Query(False, description="Enable all filtering")
) -> Dict[str, Any]:
    """Complete dashboard overview with KPIs and trends"""

    # Get KPI metrics
    stats_query = """
    SELECT 
        new_users,
        returning_users, 
        new_transactions,
        avg_clv,
        cluster
    FROM stats
    """
    where_clause, params = build_dashboard_filters(
        cluster, date_range, age_range, gender, service_type, region, enable_filters
    )

    stats_data = execute_query(stats_query + where_clause, params)

    # Calculate KPIs
    if stats_data:
        total_new = sum(row.get('new_users', 0) or 0 for row in stats_data)
        total_returning = sum(row.get('returning_users', 0) or 0 for row in stats_data)
        total_transactions = sum(row.get('new_transactions', 0) or 0 for row in stats_data)
        clv_values = [row.get('avg_clv', 0) or 0 for row in stats_data if row.get('avg_clv')]
        avg_clv = sum(clv_values) / len(clv_values) if clv_values else 0
    else:
        total_new = total_returning = total_transactions = avg_clv = 0

    # Calculate trend percentages (mock for now - you can implement actual trend calculation)
    trends = {
        "new_users_trend": "+14%",
        "returning_users_trend": "+21%",
        "transactions_trend": "+5%",
        "clv_trend": "-5%"
    }
    # don't make it mannual
    # last_period = execute_query("SELECT SUM(new_users) FROM stats WHERE created_date BETWEEN ? AND ?", (start_prev, end_prev))
    # current_period = execute_query("SELECT SUM(new_users) FROM stats WHERE created_date BETWEEN ? AND ?", (start_now, end_now))
    # trend = calculate_percentage_change(last_period, current_period)

    return {
        "kpis": {
            "new_users": total_new,
            "returning_users": total_returning,
            "new_transactions": total_transactions,
            "avg_clv": round(avg_clv, 2),
            "total_users": total_new + total_returning
        },
        "trends": trends,
        "applied_filters": {
            "cluster": cluster,
            "date_range": date_range,
            "age_range": age_range,
            "gender": gender,
            "service_type": service_type,
            "region": region,
            "filters_enabled": enable_filters
        },
        "timestamp": datetime.now().isoformat()
    }

@app.get("/api/dashboard/analytics")
@log_performance
async def get_dashboard_analytics(
        cluster: Optional[str] = Query(None, description="Filter by customer cluster"),
        date_range: Optional[str] = Query(None, description="Date range filter"),
        enable_filters: bool = Query(False, description="Enable filtering")
) -> Dict[str, Any]:
    """Comprehensive analytics data for all charts"""

    where_clause, params = build_dashboard_filters(
        cluster=cluster, date_range=date_range, enable_filters=enable_filters
    )

    # Get all chart data in parallel queries
    analytics_data = {}

    # Service usage data
    service_query = "SELECT [service], count, cluster FROM service_count" + where_clause + " ORDER BY [service], cluster"
    service_data = execute_query(service_query, params)
    analytics_data["service_usage"] = format_bar_chart_data(service_data)

    # Gender distribution
    gender_query = "SELECT gender, count, cluster FROM gender_count" + where_clause + " ORDER BY count DESC"
    gender_data = execute_query(gender_query, params)
    analytics_data["gender_distribution"] = format_pie_chart_data(gender_data)

    # Transaction patterns
    trans_query = "SELECT hour, count, cluster FROM trans_by_day" + where_clause + " ORDER BY cluster, hour"
    trans_data = execute_query(trans_query, params)
    analytics_data["transactions_by_hour"] = format_line_chart_data(trans_data)

    # Age distribution
    age_query = "SELECT age, count, cluster FROM age_count" + where_clause + " ORDER BY age"
    age_data = execute_query(age_query, params)
    analytics_data["age_distribution"] = format_bar_chart_data(age_data)

    # Geographic data
    geo_query = """
    SELECT country, COUNT(*) as user_count, cluster 
    FROM user_geo
    """ + where_clause + " GROUP BY country, cluster ORDER BY user_count DESC"
    geo_data = execute_query(geo_query, params)
    analytics_data["geography"] = format_geography_data(geo_data)

    return {
        "charts": analytics_data,
        "metadata": {
            "total_charts": len(analytics_data),
            "filters_applied": enable_filters,
            "cluster": cluster,
            "date_range": date_range
        },
        "timestamp": datetime.now().isoformat()
    }

@app.get("/api/dashboard/users")
@log_performance
async def get_user_analytics(
        cluster: Optional[str] = Query(None, description="Filter by customer cluster"),
        age_range: Optional[str] = Query(None, description="Age range filter"),
        gender: Optional[str] = Query(None, description="Gender filter"),
        region: Optional[str] = Query(None, description="Region filter"),
        enable_filters: bool = Query(False, description="Enable filtering")
) -> Dict[str, Any]:
    """👥 User demographics and behavior analytics"""

    where_clause, params = build_dashboard_filters(
        cluster=cluster, age_range=age_range, gender=gender, region=region, enable_filters=enable_filters
    )

    # User demographics summary
    demo_query = """
    SELECT 
        COUNT(*) as total_users,
        AVG(age) as avg_age,
        COUNT(CASE WHEN gender = 'male' THEN 1 END) as male_count,
        COUNT(CASE WHEN gender = 'female' THEN 1 END) as female_count,
        COUNT(DISTINCT country) as countries_count,
        cluster
    FROM user_geo ug
    """ + where_clause + " GROUP BY cluster"

    demo_data = execute_query(demo_query, params)

    return {
        "user_demographics": demo_data,
        "summary": {
            "total_users": sum(row.get('total_users', 0) for row in demo_data),
            "avg_age": round(sum(row.get('avg_age', 0) for row in demo_data) / len(demo_data), 1) if demo_data else 0,
            "gender_distribution": {
                "male": sum(row.get('male_count', 0) for row in demo_data),
                "female": sum(row.get('female_count', 0) for row in demo_data)
            },
            "geographic_reach": sum(row.get('countries_count', 0) for row in demo_data)
        },
        "applied_filters": {
            "cluster": cluster,
            "age_range": age_range,
            "gender": gender,
            "region": region,
            "filters_enabled": enable_filters
        },
        "timestamp": datetime.now().isoformat()
    }

@app.get("/api/dashboard/transactions")
@log_performance
async def get_transaction_analytics(
        cluster: Optional[str] = Query(None, description="Filter by customer cluster"),
        date_range: Optional[str] = Query(None, description="Date range filter"),
        service_type: Optional[str] = Query(None, description="Service type filter"),
        enable_filters: bool = Query(False, description="Enable filtering")
) -> Dict[str, Any]:
    """Transaction analytics and patterns"""

    where_clause, params = build_dashboard_filters(
        cluster=cluster, date_range=date_range, service_type=service_type, enable_filters=enable_filters
    )

    # Transaction patterns by hour
    hourly_query = "SELECT hour, count, cluster FROM trans_by_day" + where_clause + " ORDER BY hour"
    hourly_data = execute_query(hourly_query, params)

    # Service transaction distribution
    service_query = "SELECT [service], count, cluster FROM service_count" + where_clause + " ORDER BY count DESC"
    service_data = execute_query(service_query, params)

    return {
        "transaction_patterns": {
            "hourly": format_line_chart_data(hourly_data),
            "by_service": format_bar_chart_data(service_data)
        },
        "insights": {
            "peak_hours": [hour for hour in range(9, 18)],  # Business hours
            "top_services": [row.get('service') for row in service_data[:5]],
            "total_transactions": sum(row.get('count', 0) for row in hourly_data)
        },
        "applied_filters": {
            "cluster": cluster,
            "date_range": date_range,
            "service_type": service_type,
            "filters_enabled": enable_filters
        },
        "timestamp": datetime.now().isoformat()
    }

@app.get("/api/charts/clusters")
@log_performance
async def get_available_clusters() -> List[str]:
    """Get available customer clusters for filtering"""
    query = "SELECT DISTINCT cluster FROM service_count WHERE cluster IS NOT NULL ORDER BY cluster"

    raw_data = execute_query(query)
    clusters = [row['cluster'] for row in raw_data if row.get('cluster')]

    logger.info(f"Available clusters: {clusters}")
    return clusters

# Health and Debug Endpoints

@app.get("/api/health")
async def health_check():
    """Comprehensive health check"""
    try:
        start_time = datetime.now()
        test_data = execute_query("SELECT 1 as test, @@VERSION as sql_version")
        duration = (datetime.now() - start_time).total_seconds()

        return {
            "status": "healthy",
            "database": "connected",
            "response_time_ms": round(duration * 1000, 2),
            "sql_server": test_data[0].get('sql_version', 'Unknown') if test_data else 'Unknown',
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Health check failed: {str(e)}")
        return {
            "status": "unhealthy",
            "database": "disconnected",
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }

@app.get("/api/debug/tables")
async def debug_tables():
    """Debug: Check table status"""
    try:
        # Get all tables
        tables_query = """
        SELECT TABLE_NAME, TABLE_SCHEMA
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_TYPE = 'BASE TABLE'
        ORDER BY TABLE_NAME
        """
        tables = execute_query(tables_query)

        # Check row counts
        table_info = {}
        required_tables = ['service_count', 'gender_count', 'trans_by_day', 'user_geo', 'age_count', 'stats']

        for table in tables:
            table_name = table['TABLE_NAME']
            try:
                count_query = f"SELECT COUNT(*) as row_count FROM [{table_name}]"
                count_result = execute_query(count_query)
                row_count = count_result[0]['row_count'] if count_result else 0

                table_info[table_name] = {
                    "row_count": row_count,
                    "required": table_name in required_tables,
                    "status": "✅" if row_count > 0 else "Empty"
                }
            except Exception as e:
                table_info[table_name] = {
                    "row_count": 0,
                    "required": table_name in required_tables,
                    "status": f"❌ Error: {str(e)}"
                }

        return {
            "tables_found": len(tables),
            "required_tables_status": table_info,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Debug tables failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Debug failed: {str(e)}")

@app.get("/")
async def root():
    """API information and status"""
    return {
        "message": "Super Marketer API",
        "version": "1.0.0",
        "features": {
            "🔄": "pymssql exclusively",
            "🔐": "SQL injection protection",
            "🧹": "Clean modular structure",
            "📊": "Nivo-ready chart formats",
            "🧾": "Comprehensive logging",
            "♻️": "DRY reusable components"
        },
        "endpoints": {
            "charts": "/api/charts/",
            "health": "/api/health",
            "debug": "/api/debug/",
            "docs": "/docs"
        },
        "timestamp": datetime.now().isoformat()
    }
