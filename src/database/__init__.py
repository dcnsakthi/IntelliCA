"""Database package initialization."""
from .fabric_sql import FabricSQLConnector
from .fabric_cosmos import FabricCosmosDBConnector

# Backward compatibility aliases
AzureSQLConnector = FabricSQLConnector
CosmosDBConnector = FabricCosmosDBConnector
# PostgreSQL has been migrated to CosmosDB NoSQL
PostgreSQLConnector = FabricCosmosDBConnector

__all__ = [
    'FabricSQLConnector',
    'FabricCosmosDBConnector',
    'AzureSQLConnector',
    'PostgreSQLConnector',  # Legacy alias
    'CosmosDBConnector'
]
