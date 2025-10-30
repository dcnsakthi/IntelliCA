from src.database.fabric_cosmos import FabricCosmosDBConnector
import os
from dotenv import load_dotenv

load_dotenv()

conn = FabricCosmosDBConnector(
    os.getenv('FABRIC_COSMOSDB_ENDPOINT'),
    os.getenv('FABRIC_COSMOSDB_DATABASE')
)
conn.initialize()

# Test query without VectorDistance
query1 = "SELECT TOP 1 c.productName, c.brand FROM c WHERE c.type = 'product'"
items = conn.query_items(query1, container=conn.products_container)
print("Query 1 result (no VectorDistance):")
print(f"  Columns: {list(items[0].keys()) if items else 'No results'}")
print(f"  productName: {items[0].get('productName', 'NOT FOUND') if items else 'No results'}")
