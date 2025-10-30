"""Configuration management utilities."""
import os
from dotenv import load_dotenv
from typing import Optional
import logging

logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()


class Config:
    """Application configuration."""
    
    # Azure OpenAI
    AZURE_OPENAI_ENDPOINT: str = os.getenv("AZURE_OPENAI_ENDPOINT", "")
    AZURE_OPENAI_API_KEY: str = os.getenv("AZURE_OPENAI_API_KEY", "")
    AZURE_OPENAI_DEPLOYMENT_NAME: str = os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME", "gpt-4")
    AZURE_OPENAI_EMBEDDING_DEPLOYMENT: str = os.getenv("AZURE_OPENAI_EMBEDDING_DEPLOYMENT", "text-embedding-ada-002")
    AZURE_OPENAI_API_VERSION: str = os.getenv("AZURE_OPENAI_API_VERSION", "2024-02-15-preview")
    
    # Microsoft Fabric SQL Database (uses Entra ID authentication only)
    FABRIC_SQL_ENDPOINT: str = os.getenv("FABRIC_SQL_ENDPOINT", "")
    FABRIC_SQL_DATABASE: str = os.getenv("FABRIC_SQL_DATABASE", "CustomerDB")
    FABRIC_SQL_DRIVER: str = os.getenv("FABRIC_SQL_DRIVER", "ODBC Driver 18 for SQL Server")
    
    # Microsoft Fabric CosmosDB (unified endpoint, uses Entra ID authentication only)
    FABRIC_COSMOSDB_ENDPOINT: str = os.getenv("FABRIC_COSMOSDB_ENDPOINT", "")
    
    # CosmosDB for PostgreSQL API settings
    FABRIC_COSMOSDB_PG_DATABASE: str = os.getenv("FABRIC_COSMOSDB_PG_DATABASE", "ProductDB")
    FABRIC_COSMOSDB_PG_PORT: int = int(os.getenv("FABRIC_COSMOSDB_PG_PORT", "5432"))
    
    # CosmosDB NoSQL API settings
    FABRIC_COSMOSDB_NOSQL_DATABASE: str = os.getenv("FABRIC_COSMOSDB_NOSQL_DATABASE", "AnalyticsDB")
    FABRIC_COSMOSDB_NOSQL_CONTAINER: str = os.getenv("FABRIC_COSMOSDB_NOSQL_CONTAINER", "Sessions")
    
    # Azure Entra ID Configuration (optional - for service principal auth)
    AZURE_TENANT_ID: str = os.getenv("AZURE_TENANT_ID", "")
    AZURE_CLIENT_ID: str = os.getenv("AZURE_CLIENT_ID", "")
    AZURE_CLIENT_SECRET: str = os.getenv("AZURE_CLIENT_SECRET", "")
    
    # Application settings
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
    DEBUG_MODE: bool = os.getenv("DEBUG_MODE", "false").lower() == "true"
    
    @classmethod
    def validate(cls) -> bool:
        """Validate required configuration."""
        required_fields = [
            "AZURE_OPENAI_ENDPOINT",
            "AZURE_OPENAI_API_KEY",
            "FABRIC_SQL_ENDPOINT",
            "FABRIC_COSMOSDB_ENDPOINT"
        ]
        
        missing = []
        for field in required_fields:
            if not getattr(cls, field):
                missing.append(field)
        
        if missing:
            logger.error(f"Missing required configuration: {', '.join(missing)}")
            return False
        
        logger.info("Configuration validated successfully")
        return True


def get_config() -> Config:
    """Get configuration instance."""
    return Config()
