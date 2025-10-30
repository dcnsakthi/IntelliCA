from src.database.fabric_cosmos import FabricCosmosDBConnector
from src.agent_integration import generate_embeddings
import os
from dotenv import load_dotenv

load_dotenv()

conn = FabricCosmosDBConnector(
    os.getenv('FABRIC_COSMOSDB_ENDPOINT'),
    os.getenv('FABRIC_COSMOSDB_DATABASE')
)
conn.initialize()

# Generate embedding for "laptop"
embedding = generate_embeddings(['laptop'], None, use_azure=True)
print(f"Generated embedding with {len(embedding[0])} dimensions")

# Test with adjusted threshold (0.15 = 85% similarity)
results = conn.search_products_by_embedding(
    embedding[0], 
    limit=5, 
    similarity_threshold=0.15
)

print(f"\nFound {len(results)} products with threshold=0.15 (85% similarity)")

if not results.empty:
    print("\nColumns:", results.columns.tolist())
    print("\nTop Products:")
    print("-" * 80)
    for idx, row in results.iterrows():
        print(f"\n📦 {row.get('productName', 'Unknown')}")
        print(f"   Brand: {row.get('brand', 'N/A')}")
        print(f"   Category: {row.get('category', 'N/A')}")
        print(f"   Price: ${row.get('price', 0):.2f}")
        print(f"   Stock: {row.get('stockQuantity', 0)} units")
        print(f"   Match Score: {row.get('similarity', 0):.2%}")
        print(f"   Description: {row.get('description', 'N/A')[:100]}...")
else:
    print("\nNo products found!")
    print("Trying with threshold=0.0 to see all results...")
    results = conn.search_products_by_embedding(embedding[0], limit=5, similarity_threshold=0.0)
    if not results.empty:
        print(f"Found {len(results)} products with threshold=0.0")
        print("Similarity scores:", results['similarity'].tolist())
