"""
RAG (Retrieval-Augmented Generation) System for Super Marketer Chatbot

This module implements:
1. LLM Routing - determines if user query needs private data or not
2. Database metadata extraction
3. SQL query generation and execution
4. Augmented prompt generation
5. Final LLM response generation

Supports multiple databases:
- Data Warehouse (CustomerWarehouse) - SQL Server
- Marketing Data Mart (MarketingDataMart) - SQL Server
- Chatbot Database - SQLite
"""

import os
import json
import re
import pyodbc
import sqlite3
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime
import httpx
from tenacity import retry, wait_exponential, stop_after_attempt
import asyncio
from contextlib import contextmanager
from dataclasses import dataclass
from rag_config import RAGConfig


@dataclass
class DatabaseConfig:
    """Database configuration class"""
    name: str
    type: str  # 'sqlserver' or 'sqlite'
    connection_string: str
    description: str


class DatabaseMetadataExtractor:
    """Extracts and manages database metadata for SQL generation"""
    
    def __init__(self):
        self.metadata_cache = {}
        self.last_cache_update = {}
    
    def get_sqlserver_connection_string(self, server: str, database: str, username: str, password: str, port: int = 1433) -> str:
        """Generate SQL Server connection string with fallback drivers"""
        # Try different ODBC drivers in order of preference
        drivers = [
            "ODBC Driver 17 for SQL Server",
            "ODBC Driver 18 for SQL Server", 
            "ODBC Driver 13 for SQL Server",
            "SQL Server Native Client 11.0",
            "SQL Server"
        ]
        
        for driver in drivers:
            try:
                conn_str = f"DRIVER={{{driver}}};SERVER={server},{port};DATABASE={database};UID={username};PWD={password};TrustServerCertificate=yes;"
                # Test the connection string
                test_conn = pyodbc.connect(conn_str, timeout=5)
                test_conn.close()
                print(f"[SUCCESS] Using ODBC driver: {driver}")
                return conn_str
            except Exception as e:
                print(f"[WARN] Driver '{driver}' failed: {str(e)[:100]}...")
                continue
        
        # If all drivers fail, return the default one and let it fail with proper error
        return f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server},{port};DATABASE={database};UID={username};PWD={password};TrustServerCertificate=yes;"
    
    @contextmanager
    def get_sqlserver_connection(self, connection_string: str):
        """Get SQL Server connection with proper error handling"""
        conn = None
        try:
            conn = pyodbc.connect(connection_string, timeout=30)
            yield conn
        except Exception as e:
            print(f"[ERROR] SQL Server connection failed: {e}")
            raise
        finally:
            if conn:
                conn.close()
    
    @contextmanager
    def get_sqlite_connection(self, db_path: str):
        """Get SQLite connection with proper error handling"""
        conn = None
        try:
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            yield conn
        except Exception as e:
            print(f"[ERROR] SQLite connection failed: {e}")
            raise
        finally:
            if conn:
                conn.close()
    
    def extract_sqlserver_metadata(self, connection_string: str, database_name: str) -> Dict[str, Any]:
        """Extract metadata from SQL Server database"""
        try:
            with self.get_sqlserver_connection(connection_string) as conn:
                cursor = conn.cursor()
                
                # Get table information
                cursor.execute("""
                    SELECT 
                        t.TABLE_NAME,
                        t.TABLE_TYPE,
                        c.COLUMN_NAME,
                        c.DATA_TYPE,
                        c.IS_NULLABLE,
                        c.COLUMN_DEFAULT,
                        c.ORDINAL_POSITION
                    FROM INFORMATION_SCHEMA.TABLES t
                    JOIN INFORMATION_SCHEMA.COLUMNS c ON t.TABLE_NAME = c.TABLE_NAME
                    WHERE t.TABLE_SCHEMA = 'dbo'
                    ORDER BY t.TABLE_NAME, c.ORDINAL_POSITION
                """)
                
                tables = {}
                for row in cursor.fetchall():
                    table_name = row[0]
                    if table_name not in tables:
                        tables[table_name] = {
                            'type': row[1],
                            'columns': []
                        }
                    
                    tables[table_name]['columns'].append({
                        'name': row[2],
                        'type': row[3],
                        'nullable': row[4] == 'YES',
                        'default': row[5],
                        'position': row[6]
                    })
                
                # Get foreign key relationships
                cursor.execute("""
                    SELECT 
                        fk.TABLE_NAME AS foreign_table,
                        fk.COLUMN_NAME AS foreign_column,
                        pk.TABLE_NAME AS primary_table,
                        pk.COLUMN_NAME AS primary_column,
                        fk.CONSTRAINT_NAME
                    FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc
                    JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE fk 
                        ON fk.CONSTRAINT_NAME = rc.CONSTRAINT_NAME
                    JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE pk 
                        ON pk.CONSTRAINT_NAME = rc.UNIQUE_CONSTRAINT_NAME
                """)
                
                relationships = []
                for row in cursor.fetchall():
                    relationships.append({
                        'foreign_table': row[0],
                        'foreign_column': row[1],
                        'primary_table': row[2],
                        'primary_column': row[3],
                        'constraint_name': row[4]
                    })
                
                return {
                    'database_name': database_name,
                    'type': 'sqlserver',
                    'tables': tables,
                    'relationships': relationships,
                    'last_updated': datetime.now().isoformat()
                }
                
        except Exception as e:
            print(f"[ERROR] Failed to extract SQL Server metadata: {e}")
            return {}
    
    def extract_sqlite_metadata(self, db_path: str, database_name: str) -> Dict[str, Any]:
        """Extract metadata from SQLite database"""
        try:
            with self.get_sqlite_connection(db_path) as conn:
                cursor = conn.cursor()
                
                # Get table information
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
                table_names = [row[0] for row in cursor.fetchall()]
                
                tables = {}
                for table_name in table_names:
                    # Get column information
                    cursor.execute(f"PRAGMA table_info({table_name})")
                    columns = []
                    for row in cursor.fetchall():
                        columns.append({
                            'name': row[1],
                            'type': row[2],
                            'nullable': not row[3],
                            'default': row[4],
                            'position': row[0]
                        })
                    
                    tables[table_name] = {
                        'type': 'table',
                        'columns': columns
                    }
                
                # Get foreign key relationships
                relationships = []
                for table_name in table_names:
                    cursor.execute(f"PRAGMA foreign_key_list({table_name})")
                    for row in cursor.fetchall():
                        relationships.append({
                            'foreign_table': table_name,
                            'foreign_column': row[3],
                            'primary_table': row[2],
                            'primary_column': row[4],
                            'constraint_name': f"fk_{table_name}_{row[3]}"
                        })
                
                return {
                    'database_name': database_name,
                    'type': 'sqlite',
                    'tables': tables,
                    'relationships': relationships,
                    'last_updated': datetime.now().isoformat()
                }
                
        except Exception as e:
            print(f"[ERROR] Failed to extract SQLite metadata: {e}")
            return {}
    
    def get_database_metadata(self, db_config: DatabaseConfig, force_refresh: bool = False) -> Dict[str, Any]:
        """Get database metadata with caching"""
        cache_key = f"{db_config.name}_{db_config.type}"
        
        # Check cache
        if not force_refresh and cache_key in self.metadata_cache:
            last_update = self.last_cache_update.get(cache_key, datetime.min)
            if (datetime.now() - last_update).seconds < 3600:  # Cache for 1 hour
                return self.metadata_cache[cache_key]
        
        # Extract fresh metadata
        if db_config.type == 'sqlserver':
            metadata = self.extract_sqlserver_metadata(db_config.connection_string, db_config.name)
        elif db_config.type == 'sqlite':
            # Extract db path from connection string
            db_path = db_config.connection_string.replace('sqlite:///', '')
            metadata = self.extract_sqlite_metadata(db_path, db_config.name)
        else:
            print(f"[ERROR] Unsupported database type: {db_config.type}")
            return {}
        
        # Update cache
        if metadata:
            self.metadata_cache[cache_key] = metadata
            self.last_cache_update[cache_key] = datetime.now()
        
        return metadata


class LLMRouter:
    """Routes user queries to determine if private data access is needed"""
    
    def __init__(self, groq_api_key: str, groq_model: str = "llama3-70b-8192"):
        self.groq_api_key = groq_api_key
        self.groq_model = groq_model
        self.groq_api_base = "https://api.groq.com/openai/v1"
    
    @retry(wait=wait_exponential(multiplier=1, min=1, max=8), stop=stop_after_attempt(3))
    async def call_groq_api(self, messages: List[Dict[str, str]]) -> str:
        """Call Groq API with retry logic"""
        url = f"{self.groq_api_base}/chat/completions"
        headers = {
            "Authorization": f"Bearer {self.groq_api_key}",
            "Content-Type": "application/json"
        }
        payload = {
            "model": self.groq_model,
            "messages": messages,
            "temperature": RAGConfig.ROUTING_TEMPERATURE,
            "max_tokens": 500
        }
        
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(url, headers=headers, json=payload)
            if resp.status_code >= 400:
                raise Exception(f"Groq API error {resp.status_code}: {resp.text[:200]}")
            
            data = resp.json()
            content = data.get("choices", [{}])[0].get("message", {}).get("content", "")
            return content.strip()
    
    async def route_query(self, user_query: str, conversation_history: List[Dict[str, str]] = None) -> Dict[str, Any]:
        """
        Determine if user query needs private data access and which databases
        
        Args:
            user_query: The current user query
            conversation_history: Previous conversation messages for context
        
        Returns:
        {
            'needs_private_data': bool,
            'databases_needed': List[str],
            'query_type': str,
            'reasoning': str
        }
        """
        
        # Quick keyword-based pre-screening for obvious conversation queries
        memory_keywords = ['remember', 'who am i', 'my position', 'my role', 'my name', 'you mentioned', 'earlier', 'before', 'said i was', 'told you', 'what did i say', 'what did i tell', 'do you know who i am']
        user_query_lower = user_query.lower()
        
        # Check for memory keywords with debugging
        memory_match = any(keyword in user_query_lower for keyword in memory_keywords)
        if memory_match:
            matched_keywords = [keyword for keyword in memory_keywords if keyword in user_query_lower]
            print(f"[INFO] Memory query detected. Query: '{user_query}', Matched keywords: {matched_keywords}")
            return {
                "needs_private_data": False,
                "databases_needed": [],
                "query_type": "memory_query",
                "reasoning": f"Query contains memory keywords: {matched_keywords}"
            }
        
        print(f"[INFO] No memory keywords found in query: '{user_query}'")
        print(f"[INFO] Checking for keywords: {memory_keywords}")
        print(f"[INFO] Query lowercase: '{user_query_lower}'")
        
        # Build conversation context if available
        conversation_context = ""
        if conversation_history:
            recent_messages = conversation_history[-10:]  # Last 10 messages for context
            conversation_context = "\n\nRecent conversation history:\n"
            for msg in recent_messages:
                role = msg.get('role', 'unknown')
                content = msg.get('content', '')[:200]  # Limit content length
                conversation_context += f"{role}: {content}\n"
        
        routing_prompt = f"""
You are a query router for a marketing analytics system. Analyze the user query and determine:

1. Does this query need access to private customer/transaction/marketing data?
2. Which databases might be needed (if any)?
3. What type of query is this?

IMPORTANT: Check the conversation history first. If the user is asking about something they mentioned earlier in the conversation (like their name, position, role, etc.), this should be answered from conversation history, NOT database queries.

Available databases:
- CustomerWarehouse: Contains customer demographics (dim_users), card info (dim_card), and transactions (fact_trans)
- MarketingDataMart: Contains aggregated marketing analytics (age_count, gender_count, service_count)
- ChatbotDatabase: Contains chat history and user sessions

Query types:
- data_query: Needs database access for specific data
- general_marketing: General marketing advice, no specific data needed
- analytics_request: Requests for charts, reports, or data analysis
- conversational: General conversation, references to chat history, personal info mentioned before
- memory_query: User asking to remember something from earlier conversation

User Query: "{user_query}"{conversation_context}

Analysis Guidelines:
- If user asks "who am I", "do you remember", "my position", "my role" -> Check conversation history first
- If user asks for specific numbers, analytics, customer data -> Use databases
- If user asks general marketing questions -> No database needed
- If user mentions names, companies, or personal details from chat history -> Conversational response

Respond in this exact JSON format:
{{
    "needs_private_data": true/false,
    "databases_needed": ["database_name1", "database_name2"],
    "query_type": "data_query|general_marketing|analytics_request|conversational|memory_query",
    "reasoning": "Brief explanation of your decision"
}}"""
        
        messages = [
            {"role": "system", "content": "You are a precise query router. Always respond with valid JSON only."},
            {"role": "user", "content": routing_prompt}
        ]
        
        try:
            response = await self.call_groq_api(messages)
            
            # Extract JSON from response
            json_match = re.search(r'\{.*\}', response, re.DOTALL)
            if json_match:
                routing_result = json.loads(json_match.group())
                return routing_result
            else:
                # Fallback parsing based on keywords
                user_query_lower = user_query.lower()
                
                # Check for memory queries
                memory_keywords = ['remember', 'who am i', 'my position', 'my role', 'my name']
                if any(keyword in user_query_lower for keyword in memory_keywords):
                    return {
                        "needs_private_data": False,
                        "databases_needed": [],
                        "query_type": "memory_query",
                        "reasoning": "Fallback: Detected memory query keywords"
                    }
                
                # Check for data queries
                data_keywords = ['count', 'number of', 'how many', 'total', 'analytics', 'transactions']
                if any(keyword in user_query_lower for keyword in data_keywords):
                    return {
                        "needs_private_data": True,
                        "databases_needed": ["CustomerWarehouse", "MarketingDataMart"],
                        "query_type": "data_query",
                        "reasoning": "Fallback: Detected data query keywords"
                    }
                
                # Default to conversational
                return {
                    "needs_private_data": False,
                    "databases_needed": [],
                    "query_type": "conversational",
                    "reasoning": "Fallback: Default to conversational response"
                }
                
        except Exception as e:
            print(f"[ERROR] Query routing failed: {e}")
            # Safe fallback based on query content
            user_query_lower = user_query.lower()
            
            # Memory queries should never need database access
            memory_keywords = ['remember', 'who am i', 'my position', 'my role', 'my name']
            if any(keyword in user_query_lower for keyword in memory_keywords):
                return {
                    "needs_private_data": False,
                    "databases_needed": [],
                    "query_type": "memory_query",
                    "reasoning": "Error fallback: Memory query detected"
                }
            
            # Conservative fallback - treat as conversational to avoid unnecessary DB queries
            return {
                "needs_private_data": False,
                "databases_needed": [],
                "query_type": "conversational",
                "reasoning": f"Router error fallback: {str(e)}"
            }


class SQLQueryGenerator:
    """Generates SQL queries based on user intent and database metadata"""
    
    def __init__(self, groq_api_key: str, groq_model: str = "llama3-70b-8192"):
        self.groq_api_key = groq_api_key
        self.groq_model = groq_model
        self.groq_api_base = "https://api.groq.com/openai/v1"
    
    @retry(wait=wait_exponential(multiplier=1, min=1, max=8), stop=stop_after_attempt(3))
    async def call_groq_api(self, messages: List[Dict[str, str]]) -> str:
        """Call Groq API with retry logic"""
        url = f"{self.groq_api_base}/chat/completions"
        headers = {
            "Authorization": f"Bearer {self.groq_api_key}",
            "Content-Type": "application/json"
        }
        payload = {
            "model": self.groq_model,
            "messages": messages,
            "temperature": RAGConfig.SQL_GENERATION_TEMPERATURE,
            "max_tokens": 1000
        }
        
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(url, headers=headers, json=payload)
            if resp.status_code >= 400:
                raise Exception(f"Groq API error {resp.status_code}: {resp.text[:200]}")
            
            data = resp.json()
            content = data.get("choices", [{}])[0].get("message", {}).get("content", "")
            return content.strip()
    
    async def generate_sql_query(self, user_query: str, database_metadata: Dict[str, Any]) -> str:
        """
        Generate SQL query based on user intent and database schema
        
        Args:
            user_query: The user's natural language query
            database_metadata: Database schema information
            
        Returns:
            Generated SQL query string
        """
        
        # Build schema description
        schema_description = self._build_schema_description(database_metadata)
        
        sql_generation_prompt = f"""
You are an expert SQL query generator for a marketing analytics database. Generate a SQL query based on the user's request.

Database Schema:
{schema_description}

User Query: "{user_query}"

Guidelines:
1. Generate valid SQL for {database_metadata.get('type', 'unknown')} database
2. Use proper JOIN clauses when joining tables
3. Include appropriate WHERE clauses for filtering
4. Use aggregate functions (COUNT, SUM, AVG) when needed
5. Limit results to reasonable numbers (use TOP 100 for SQL Server, LIMIT 100 for SQLite)
6. Handle dates properly if timestamp fields are involved
7. Use meaningful column aliases

Respond with only the SQL query, no explanation needed.
"""
        
        messages = [
            {"role": "system", "content": "You are a precise SQL generator. Return only valid SQL queries."},
            {"role": "user", "content": sql_generation_prompt}
        ]
        
        try:
            response = await self.call_groq_api(messages)
            
            # Clean up the response to extract SQL
            sql_query = self._clean_sql_response(response)
            return sql_query
            
        except Exception as e:
            print(f"[ERROR] SQL generation failed: {e}")
            # Return a safe fallback query
            return self._get_fallback_query(database_metadata)
    
    def _build_schema_description(self, metadata: Dict[str, Any]) -> str:
        """Build a human-readable schema description"""
        if not metadata or 'tables' not in metadata:
            return "No schema information available"
        
        description = f"Database: {metadata.get('database_name', 'Unknown')}\n\n"
        
        for table_name, table_info in metadata['tables'].items():
            description += f"Table: {table_name}\n"
            description += f"Type: {table_info.get('type', 'table')}\n"
            description += "Columns:\n"
            
            for column in table_info.get('columns', []):
                nullable = " (NULL)" if column.get('nullable') else " (NOT NULL)"
                description += f"  - {column['name']}: {column['type']}{nullable}\n"
            
            description += "\n"
        
        # Add relationships
        relationships = metadata.get('relationships', [])
        if relationships:
            description += "Relationships:\n"
            for rel in relationships:
                description += f"  - {rel['foreign_table']}.{rel['foreign_column']} -> {rel['primary_table']}.{rel['primary_column']}\n"
            description += "\n"
        
        return description
    
    def _clean_sql_response(self, response: str) -> str:
        """Clean and extract SQL from LLM response"""
        # Remove markdown code blocks
        response = re.sub(r'```sql\n?', '', response)
        response = re.sub(r'```\n?', '', response)
        
        # Remove leading/trailing whitespace
        response = response.strip()
        
        # Extract the first SQL statement (basic heuristic)
        lines = response.split('\n')
        sql_lines = []
        in_sql = False
        
        for line in lines:
            line = line.strip()
            if line.upper().startswith(('SELECT', 'WITH', 'INSERT', 'UPDATE', 'DELETE')):
                in_sql = True
            
            if in_sql:
                sql_lines.append(line)
                
            # Stop at semicolon or empty line after SQL started
            if in_sql and (line.endswith(';') or (not line and sql_lines)):
                break
        
        sql_query = '\n'.join(sql_lines).strip()
        
        # Remove trailing semicolon for consistency
        if sql_query.endswith(';'):
            sql_query = sql_query[:-1]
        
        return sql_query
    
    def _get_fallback_query(self, metadata: Dict[str, Any]) -> str:
        """Generate a safe fallback query when SQL generation fails"""
        tables = metadata.get('tables', {})
        if not tables:
            return "SELECT 1 as status"
        
        # Get the first table
        first_table = list(tables.keys())[0]
        db_type = metadata.get('type', 'sqlite')
        
        if db_type == 'sqlserver':
            return f"SELECT TOP 10 * FROM {first_table}"
        else:
            return f"SELECT * FROM {first_table} LIMIT 10"


class SQLQueryExecutor:
    """Executes SQL queries against different database types"""
    
    def __init__(self, metadata_extractor: DatabaseMetadataExtractor):
        self.metadata_extractor = metadata_extractor
    
    async def execute_query(self, sql_query: str, db_config: DatabaseConfig) -> Dict[str, Any]:
        """
        Execute SQL query against specified database
        
        Args:
            sql_query: The SQL query to execute
            db_config: Database configuration
            
        Returns:
            Dictionary containing query results and metadata
        """
        try:
            if db_config.type == 'sqlserver':
                return await self._execute_sqlserver_query(sql_query, db_config)
            elif db_config.type == 'sqlite':
                return await self._execute_sqlite_query(sql_query, db_config)
            else:
                return {
                    'success': False,
                    'error': f"Unsupported database type: {db_config.type}",
                    'data': [],
                    'columns': []
                }
                
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'data': [],
                'columns': []
            }
    
    async def _execute_sqlserver_query(self, sql_query: str, db_config: DatabaseConfig) -> Dict[str, Any]:
        """Execute query against SQL Server"""
        try:
            with self.metadata_extractor.get_sqlserver_connection(db_config.connection_string) as conn:
                cursor = conn.cursor()
                cursor.execute(sql_query)
                
                # Get column names
                columns = [column[0] for column in cursor.description] if cursor.description else []
                
                # Fetch data
                rows = cursor.fetchall()
                data = []
                for row in rows:
                    data.append(dict(zip(columns, row)))
                
                return {
                    'success': True,
                    'data': data,
                    'columns': columns,
                    'row_count': len(data),
                    'query': sql_query
                }
                
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'data': [],
                'columns': [],
                'query': sql_query
            }
    
    async def _execute_sqlite_query(self, sql_query: str, db_config: DatabaseConfig) -> Dict[str, Any]:
        """Execute query against SQLite"""
        try:
            db_path = db_config.connection_string.replace('sqlite:///', '')
            
            with self.metadata_extractor.get_sqlite_connection(db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(sql_query)
                
                # Get column names
                columns = [description[0] for description in cursor.description] if cursor.description else []
                
                # Fetch data
                rows = cursor.fetchall()
                data = []
                for row in rows:
                    data.append(dict(row))
                
                return {
                    'success': True,
                    'data': data,
                    'columns': columns,
                    'row_count': len(data),
                    'query': sql_query
                }
                
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'data': [],
                'columns': [],
                'query': sql_query
            }


class RAGSystem:
    """
    Main RAG System that orchestrates the entire process:
    1. Route queries (LLM Router)
    2. Extract metadata if needed
    3. Generate SQL if needed
    4. Execute SQL if needed
    5. Generate augmented prompts
    6. Get final LLM response
    """
    
    def __init__(self, groq_api_key: str = None, groq_model: str = None):
        self.groq_api_key = groq_api_key or RAGConfig.GROQ_API_KEY
        self.groq_model = groq_model or RAGConfig.GROQ_MODEL
        self.groq_api_base = RAGConfig.GROQ_API_BASE
        
        # Initialize components
        self.metadata_extractor = DatabaseMetadataExtractor()
        self.router = LLMRouter(self.groq_api_key, self.groq_model)
        self.sql_generator = SQLQueryGenerator(self.groq_api_key, self.groq_model)
        self.sql_executor = SQLQueryExecutor(self.metadata_extractor)
        
        # Database configurations
        self.database_configs = self._setup_database_configs()
        
        # Validate configuration
        if not self.groq_api_key:
            print("[WARN] GROQ_API_KEY not found. RAG system will have limited functionality.")
        
        # Print config status for debugging
        RAGConfig.print_config_status()
        
        # Test database connections
        print("[INFO] Testing database connections...")
        connection_status = self.test_database_connections()
        
        # Report connection summary
        successful_connections = sum(connection_status.values())
        total_connections = len(connection_status)
        print(f"[INFO] Database connections: {successful_connections}/{total_connections} successful")
        
        if successful_connections == 0:
            print("[WARN] No database connections available. RAG system will have limited functionality.")
        elif successful_connections < total_connections:
            print(f"[WARN] Some database connections failed. Check the logs above.")
            print(f"[INFO] Available databases: {[db for db, status in connection_status.items() if status]}")
            print(f"[INFO] Failed databases: {[db for db, status in connection_status.items() if not status]}")
    
    def _setup_database_configs(self) -> Dict[str, DatabaseConfig]:
        """Setup database configurations from RAGConfig"""
        configs = {}
        db_configs = RAGConfig.get_database_configs()
        
        for db_name, config in db_configs.items():
            if config['type'] == 'sqlserver':
                connection_string = self.metadata_extractor.get_sqlserver_connection_string(
                    server=config['server'],
                    database=config['database'],
                    username=config['username'],
                    password=config['password'],
                    port=config['port']
                )
                configs[db_name] = DatabaseConfig(
                    name=config['name'],
                    type=config['type'],
                    connection_string=connection_string,
                    description=config['description']
                )
            elif config['type'] == 'sqlite':
                connection_string = f"sqlite:///{config['path']}"
                configs[db_name] = DatabaseConfig(
                    name=config['name'],
                    type=config['type'],
                    connection_string=connection_string,
                    description=config['description']
                )
        
        return configs
    
    def test_database_connections(self) -> Dict[str, bool]:
        """Test all database connections and return status"""
        connection_status = {}
        
        for db_name, config in self.database_configs.items():
            try:
                print(f"[INFO] Testing connection to {db_name}...")
                
                if config.type == 'sqlserver':
                    with self.metadata_extractor.get_sqlserver_connection(config.connection_string) as conn:
                        # Test with a simple query
                        cursor = conn.cursor()
                        cursor.execute("SELECT 1")
                        cursor.fetchone()
                        cursor.close()
                        connection_status[db_name] = True
                        print(f"[SUCCESS] {db_name} connection successful")
                        
                elif config.type == 'sqlite':
                    # Extract path from connection string
                    db_path = config.connection_string.replace('sqlite:///', '')
                    with self.metadata_extractor.get_sqlite_connection(db_path) as conn:
                        # Test with a simple query
                        cursor = conn.cursor()
                        cursor.execute("SELECT 1")
                        cursor.fetchone()
                        cursor.close()
                        connection_status[db_name] = True
                        print(f"[SUCCESS] {db_name} connection successful")
                        
            except Exception as e:
                connection_status[db_name] = False
                print(f"[ERROR] {db_name} connection failed: {str(e)}")
        
        return connection_status
    
    @retry(wait=wait_exponential(multiplier=1, min=1, max=8), stop=stop_after_attempt(3))
    async def call_groq_api(self, messages: List[Dict[str, str]], temperature: float = 0.7) -> str:
        """Call Groq API with retry logic"""
        url = f"{self.groq_api_base}/chat/completions"
        headers = {
            "Authorization": f"Bearer {self.groq_api_key}",
            "Content-Type": "application/json"
        }
        payload = {
            "model": self.groq_model,
            "messages": messages,
            "temperature": temperature,
            "max_tokens": 2000
        }
        
        async with httpx.AsyncClient(timeout=60) as client:
            resp = await client.post(url, headers=headers, json=payload)
            if resp.status_code >= 400:
                raise Exception(f"Groq API error {resp.status_code}: {resp.text[:200]}")
            
            data = resp.json()
            content = data.get("choices", [{}])[0].get("message", {}).get("content", "")
            return content.strip()
    
    async def process_query(self, user_query: str, conversation_history: List[Dict[str, str]] = None) -> str:
        """
        Main method to process user query through the RAG pipeline
        
        Args:
            user_query: User's natural language query
            conversation_history: Previous conversation messages
            
        Returns:
            Final AI response
        """
        try:
            # Step 1: Route the query with conversation history
            print(f"[INFO] Routing query: {user_query[:100]}...")
            routing_result = await self.router.route_query(user_query, conversation_history)
            print(f"[INFO] Routing result: {routing_result}")
            
            # Step 2: If no private data needed, go directly to LLM
            if not routing_result.get('needs_private_data', False):
                print("[INFO] No private data needed, using direct LLM response")
                return await self._generate_direct_llm_response(user_query, conversation_history)
            
            # Step 3: Process databases that are needed
            databases_needed = routing_result.get('databases_needed', [])
            all_data_results = []
            
            for db_name in databases_needed:
                if db_name not in self.database_configs:
                    print(f"[WARN] Database {db_name} not configured, skipping")
                    continue
                
                print(f"[INFO] Processing database: {db_name}")
                db_config = self.database_configs[db_name]
                
                # Step 4: Extract database metadata
                metadata = self.metadata_extractor.get_database_metadata(db_config)
                if not metadata:
                    print(f"[WARN] Could not extract metadata for {db_name}")
                    continue
                
                # Step 5: Generate SQL query
                sql_query = await self.sql_generator.generate_sql_query(user_query, metadata)
                print(f"[INFO] Generated SQL for {db_name}: {sql_query[:100]}...")
                
                # Step 6: Execute SQL query
                query_result = await self.sql_executor.execute_query(sql_query, db_config)
                
                if query_result['success']:
                    print(f"[INFO] Query executed successfully, got {query_result['row_count']} rows")
                    all_data_results.append({
                        'database': db_name,
                        'query': sql_query,
                        'result': query_result
                    })
                else:
                    print(f"[ERROR] Query execution failed: {query_result['error']}")
            
            # Step 7: Generate augmented response with retrieved data
            return await self._generate_augmented_llm_response(
                user_query, 
                all_data_results, 
                conversation_history
            )
            
        except Exception as e:
            print(f"[ERROR] RAG processing failed: {e}")
            # Fallback to direct LLM response
            return await self._generate_direct_llm_response(
                user_query, 
                conversation_history, 
                error_context=f"Data retrieval failed: {str(e)}"
            )
    
    async def _generate_direct_llm_response(
        self, 
        user_query: str, 
        conversation_history: List[Dict[str, str]] = None,
        error_context: str = None
    ) -> str:
        """Generate direct LLM response without private data"""
        
        system_prompt = """You are Super Marketer AI, a senior marketing & growth strategist. 
Provide clear, actionable, data-driven guidance. Be concise and professional.
Focus on marketing strategy, campaign optimization, customer segmentation, and growth tactics.

IMPORTANT: You have access to the conversation history. If the user asks about something they mentioned earlier (like their name, position, company, role, etc.), refer back to the conversation history to provide accurate answers. Don't say you need to query databases for information that was already shared in the conversation."""
        
        if error_context:
            system_prompt += f"\n\nNote: {error_context}. Provide general marketing advice based on your knowledge."
        
        messages = [{"role": "system", "content": system_prompt}]
        
        # Add conversation history
        if conversation_history:
            messages.extend(conversation_history[-15:])  # Include more history for better context
        
        # Add current query
        messages.append({"role": "user", "content": user_query})
        
        try:
            response = await self.call_groq_api(messages)
            return response
        except Exception as e:
            print(f"[ERROR] Direct LLM response failed: {e}")
            return "I'm having trouble generating a response right now. Could you please rephrase your question or try again?"
    
    async def _generate_augmented_llm_response(
        self, 
        user_query: str, 
        data_results: List[Dict[str, Any]], 
        conversation_history: List[Dict[str, str]] = None
    ) -> str:
        """Generate LLM response augmented with retrieved data"""
        
        # Build data context
        data_context = self._build_data_context(data_results)
        
        augmented_prompt = f"""You are Super Marketer AI, a senior marketing & growth strategist.
The user has asked a question that requires analysis of private company data.

User Query: "{user_query}"

Retrieved Data:
{data_context}

Instructions:
1. Analyze the retrieved data to answer the user's question
2. Provide actionable marketing insights based on the data
3. Include specific numbers and findings from the data
4. Suggest concrete next steps or recommendations
5. Be professional and data-driven in your response
6. If the data doesn't fully answer the question, acknowledge this and provide what insights you can

Format your response in a clear, professional manner with key insights highlighted."""
        
        messages = [{"role": "system", "content": "You are a senior marketing strategist analyzing company data."}]
        
        # Add conversation history (limited)
        if conversation_history:
            messages.extend(conversation_history[-5:])  # Last 5 messages for context
        
        # Add augmented prompt
        messages.append({"role": "user", "content": augmented_prompt})
        
        try:
            response = await self.call_groq_api(messages, temperature=RAGConfig.RESPONSE_TEMPERATURE)
            return response
        except Exception as e:
            print(f"[ERROR] Augmented LLM response failed: {e}")
            return f"I retrieved the following data but had trouble analyzing it:\n\n{data_context}\n\nCould you help me understand what specific insights you're looking for?"
    
    def _build_data_context(self, data_results: List[Dict[str, Any]]) -> str:
        """Build formatted data context for LLM"""
        if not data_results:
            return "No data was retrieved."
        
        context_parts = []
        
        for result in data_results:
            db_name = result['database']
            query = result['query']
            query_result = result['result']
            
            context_parts.append(f"Database: {db_name}")
            context_parts.append(f"Query: {query}")
            
            if query_result['success']:
                data = query_result['data']
                row_count = query_result['row_count']
                
                context_parts.append(f"Results: {row_count} rows returned")
                
                if data:
                    # Show first few rows as examples
                    context_parts.append("Sample data:")
                    for i, row in enumerate(data[:5]):  # First 5 rows
                        context_parts.append(f"  Row {i+1}: {row}")
                    
                    if len(data) > 5:
                        context_parts.append(f"  ... and {len(data) - 5} more rows")
                    
                    # Add summary statistics if numeric data
                    self._add_data_summary(context_parts, data)
                else:
                    context_parts.append("No data returned")
            else:
                context_parts.append(f"Query failed: {query_result['error']}")
            
            context_parts.append("")  # Empty line between databases
        
        return "\n".join(context_parts)
    
    def _add_data_summary(self, context_parts: List[str], data: List[Dict[str, Any]]):
        """Add summary statistics for numeric columns"""
        if not data:
            return
        
        # Find numeric columns
        numeric_columns = []
        sample_row = data[0]
        
        for column, value in sample_row.items():
            if isinstance(value, (int, float)) and not isinstance(value, bool):
                numeric_columns.append(column)
        
        if numeric_columns:
            context_parts.append("Summary statistics:")
            for col in numeric_columns[:3]:  # Limit to first 3 numeric columns
                values = [row[col] for row in data if row[col] is not None]
                if values:
                    total = sum(values)
                    avg = total / len(values)
                    min_val = min(values)
                    max_val = max(values)
                    context_parts.append(f"  {col}: Total={total}, Average={avg:.2f}, Min={min_val}, Max={max_val}")
