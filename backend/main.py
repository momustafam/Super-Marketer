from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Dict, Any
import os
import logging
from contextlib import contextmanager
from datetime import datetime
import pymssql
from functools import wraps

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Super Marketer API",
    version="1.0.0",
    description="Real-time marketing analytics API powered by MarketingDataMart"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "http://localhost:3001",
        "http://127.0.0.1:3000",
        os.getenv("FRONTEND_URL", "")
    ],
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

DB_CONFIG = {
    "server": os.getenv("DB_SERVER", "localhost"),
    "port": int(os.getenv("DB_PORT", "1435")),
    "database": os.getenv("DB_NAME", "MarketingDataMart"),
    "user": os.getenv("DB_USER", "SA"),
    "password": os.getenv("DB_PASSWORD", "Password1234!")
}


def log_performance(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        start_time = datetime.now()
        function_name = func.__name__
        logger.info(f"Starting {function_name}")
        try:
            result = await func(*args, **kwargs)
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
            autocommit=True
        )
        logger.info("✅ Database connection established")
        yield conn
    except pymssql.Error as e:
        logger.error(f"🔴 Database connection failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Database connection failed: {str(e)}")
    finally:
        if conn:
            try:
                conn.close()
                logger.info("Database connection closed")
            except Exception as e:
                logger.warning(f"Error closing connection: {str(e)}")

def execute_query(query: str, params: tuple = ()) -> List[Dict[str, Any]]:
    query_start_time = datetime.now()
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor(as_dict=True)
            query_preview = query.replace('\n', ' ').strip()[:100]
            logger.info(f"Executing query: {query_preview}{'...' if len(query) > 100 else ''}")
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            rows = cursor.fetchall()
            duration = (datetime.now() - query_start_time).total_seconds()
            logger.info(f"Query returned {len(rows)} rows in {duration:.3f}s")
            return rows
    except Exception as e:
        duration = (datetime.now() - query_start_time).total_seconds()
        logger.error(f"🔴 Query execution failed after {duration:.3f}s: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Query execution failed: {str(e)}")



@app.get("/api/charts/service-usage")
@log_performance
async def get_service_usage_data() -> List[Dict[str, Any]]:
    query = "SELECT TOP 7 [service], count FROM service_count ORDER BY count DESC"
    raw_data = execute_query(query)
    return raw_data

@app.get("/api/charts/gender-distribution")
@log_performance
async def get_gender_distribution_data() -> List[Dict[str, Any]]:
    query = "SELECT gender, SUM(count) as count FROM gender_count GROUP BY gender ORDER BY SUM(count) DESC"
    raw_data = execute_query(query)
    return raw_data

@app.get("/api/charts/transactions-by-hour")
@log_performance
async def get_transactions_by_hour_data() -> List[Dict[str, Any]]:
    query = "SELECT hour, count FROM trans_by_day ORDER BY hour"
    raw_data = execute_query(query)
    return raw_data

@app.get("/api/charts/user-geography")
@log_performance
async def get_user_geography_data() -> List[Dict[str, Any]]:
    query = """
        SELECT 
            longitude, 
            latitude, 
            city, 
            country, 
            cluster 
        FROM user_geo
        WHERE longitude IS NOT NULL AND latitude IS NOT NULL
    """
    raw_data = execute_query(query)
    return [
        {
            "longitude": row["longitude"],
            "latitude": row["latitude"],
            "city": row["city"],
            "country": row["country"],
            "cluster": row["cluster"],
        }
        for row in raw_data
    ]


@app.get("/api/charts/age-distribution")
@log_performance
async def get_age_distribution_data() -> List[Dict[str, Any]]:
    query = "SELECT age, count, cluster FROM age_count ORDER BY age"
    raw_data = execute_query(query)
    return [
        {
            "age": row["age"],
            "count": row["count"],
            "cluster": row["cluster"]
        }
        for row in raw_data
    ]

@app.get("/api/info/new-users")
@log_performance
async def get_new_users_data() -> Dict[str, Any]:
    query = "SELECT TOP 1 new_users FROM stats"
    raw_data = execute_query(query)
    if raw_data:
        return {"new_users": raw_data[0]["new_users"]}
    else:
        raise HTTPException(status_code=404, detail="No data found in stats table.")


@app.get("/api/info/returning-users")
@log_performance
async def get_returning_users() -> Dict[str, Any]:
    query = "SELECT returning_users FROM stats;"
    raw_data = execute_query(query)
    if raw_data and raw_data[0]["returning_users"] is not None:
        return {"returning_users": raw_data[0]["returning_users"]}
    raise HTTPException(status_code=404, detail="No returning user data found.")

@app.get("/api/info/top-transaction-service")
@log_performance
async def get_top_transaction_service() -> Dict[str, Any]:
    query = """
    SELECT TOP 1 [service], [count]
    FROM service_count
    ORDER BY [count] DESC;
    """
    raw_data = execute_query(query)
    if raw_data:
        return {
            "most_used_service": raw_data[0]["service"],
            "usage_count": raw_data[0]["count"]
        }
    raise HTTPException(status_code=404, detail="No transaction service data found.")


@app.get("/api/info/average-clv")
@log_performance
async def get_average_clv() -> Dict[str, Any]:
    query = "SELECT avg_clv FROM stats;"
    raw_data = execute_query(query)
    if raw_data and raw_data[0]["avg_clv"] is not None:
        return {"average_clv": round(raw_data[0]["avg_clv"], 2)}
    raise HTTPException(status_code=404, detail="No CLV data found.")


@app.get("/api/check-db")
async def check_db_connection():
    """Simple DB connectivity check"""
    try:
        execute_query("SELECT 1 AS test")
        return {"status": "connected"}
    except Exception as e:
        logger.error(f"🔴 DB health check failed: {e}")
        return {"status": "disconnected", "error": str(e)}

@app.get("/")
async def root():
    return {
        "message": "Super Marketer API",
        "version": "1.0.0",
        "features": {
            "🔄": "pymssql exclusively",
            "🔐": "SQL injection protection",
            "📊": "Nivo-ready chart formats",
            "🧾": "Comprehensive logging"
        },
        "endpoints": {
            "charts": "/api/charts/",
            "health": "/api/check-db",
            "docs": "/docs"
        },
        "timestamp": datetime.now().isoformat()
    }