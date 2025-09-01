"""
RAG System Configuration

This module contains configuration settings for the RAG system
including database connections, model settings, and other parameters.
"""

import os
from typing import Dict, Any

class RAGConfig:
    """Configuration class for RAG system"""
    
    # LLM Configuration
    GROQ_API_KEY = os.getenv("GROQ_API_KEY")
    GROQ_MODEL = os.getenv("GROQ_MODEL", "llama3-70b-8192")
    GROQ_API_BASE = os.getenv("GROQ_API_BASE", "https://api.groq.com/openai/v1")
    
    # Database Configuration
    # Data Warehouse (running in Docker, accessible via localhost:1434)
    DW_SERVER = os.getenv("DW_SERVER", "localhost")
    DW_PORT = int(os.getenv("DW_PORT", "1434"))
    DW_DATABASE = os.getenv("DW_DATABASE", "CustomerWarehouse")
    DW_USERNAME = os.getenv("DW_USERNAME", "sa")
    DW_PASSWORD = os.getenv("DW_PASSWORD", "YourStrong!Passw0rd")
    
    # Marketing Data Mart (running in Docker, accessible via localhost:1435)
    MART_SERVER = os.getenv("MART_SERVER", "localhost")
    MART_PORT = int(os.getenv("MART_PORT", "1435"))
    MART_DATABASE = os.getenv("MART_DATABASE", "MarketingDataMart")
    MART_USERNAME = os.getenv("MART_USERNAME", "sa")
    MART_PASSWORD = os.getenv("MART_PASSWORD", "Password1234!")
    
    # Chatbot Database (local SQLite file)
    CHATBOT_DB_PATH = os.getenv("DATABASE_PATH", "./chatbot.db")
    
    # RAG System Settings
    METADATA_CACHE_TTL = int(os.getenv("METADATA_CACHE_TTL", "3600"))  # 1 hour
    QUERY_TIMEOUT = int(os.getenv("QUERY_TIMEOUT", "30"))  # 30 seconds
    MAX_QUERY_RESULTS = int(os.getenv("MAX_QUERY_RESULTS", "100"))
    
    # LLM Temperature Settings
    ROUTING_TEMPERATURE = float(os.getenv("ROUTING_TEMPERATURE", "0.1"))
    SQL_GENERATION_TEMPERATURE = float(os.getenv("SQL_GENERATION_TEMPERATURE", "0.1"))
    RESPONSE_TEMPERATURE = float(os.getenv("RESPONSE_TEMPERATURE", "0.3"))
    
    # System Prompts
    ROUTING_SYSTEM_PROMPT = os.getenv(
        "ROUTING_SYSTEM_PROMPT",
        "You are a precise query router. Always respond with valid JSON only."
    )
    
    SQL_GENERATION_SYSTEM_PROMPT = os.getenv(
        "SQL_GENERATION_SYSTEM_PROMPT",
        "You are a precise SQL generator. Return only valid SQL queries."
    )
    
    RESPONSE_SYSTEM_PROMPT = os.getenv(
        "RESPONSE_SYSTEM_PROMPT",
        "You are a senior marketing strategist analyzing company data."
    )
    
    @classmethod
    def get_database_configs(cls) -> Dict[str, Dict[str, Any]]:
        """Get all database configurations"""
        return {
            "CustomerWarehouse": {
                "name": "CustomerWarehouse",
                "type": "sqlserver",
                "server": cls.DW_SERVER,
                "port": cls.DW_PORT,
                "database": cls.DW_DATABASE,
                "username": cls.DW_USERNAME,
                "password": cls.DW_PASSWORD,
                "description": "Customer data warehouse with demographics and transactions"
            },
            "MarketingDataMart": {
                "name": "MarketingDataMart",
                "type": "sqlserver",
                "server": cls.MART_SERVER,
                "port": cls.MART_PORT,
                "database": cls.MART_DATABASE,
                "username": cls.MART_USERNAME,
                "password": cls.MART_PASSWORD,
                "description": "Marketing analytics and aggregated data"
            },
            "ChatbotDatabase": {
                "name": "ChatbotDatabase",
                "type": "sqlite",
                "path": cls.CHATBOT_DB_PATH,
                "description": "Chatbot conversation history and user sessions"
            }
        }
    
    @classmethod
    def validate_config(cls) -> Dict[str, bool]:
        """Validate configuration settings"""
        validation = {
            "groq_api_key": bool(cls.GROQ_API_KEY),
            "groq_model": bool(cls.GROQ_MODEL),
            "chatbot_db_exists": os.path.exists(cls.CHATBOT_DB_PATH) if os.path.isabs(cls.CHATBOT_DB_PATH) else True,
            "database_configs": bool(cls.get_database_configs()),
        }
        return validation
    
    @classmethod
    def print_config_status(cls):
        """Print configuration status for debugging"""
        print("RAG System Configuration Status:")
        print("=" * 40)
        
        validation = cls.validate_config()
        for key, status in validation.items():
            status_text = "✅ OK" if status else "❌ MISSING"
            print(f"{key.replace('_', ' ').title()}: {status_text}")
        
        print("\nDatabase Configurations:")
        for db_name, config in cls.get_database_configs().items():
            print(f"  {db_name}: {config['type']} - {config['description']}")
        
        print(f"\nModel Settings:")
        print(f"  GROQ Model: {cls.GROQ_MODEL}")
        print(f"  Cache TTL: {cls.METADATA_CACHE_TTL}s")
        print(f"  Query Timeout: {cls.QUERY_TIMEOUT}s")
