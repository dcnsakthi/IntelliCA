"""
Quick script to test semantic search functionality
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from dotenv import load_dotenv
load_dotenv()

from src.database.fabric_cosmos import FabricCosmosDBConnector
from src.agent_integration import get_embedding_service
import asyncio

async def main():
    print("=" * 60)
    print("Testing Semantic Search")
    print("=" * 60)
    
    # Initialize CosmosDB connector
    try:
        cosmos_conn = FabricCosmosDBConnector(
            endpoint=os.getenv("FABRIC_COSMOSDB_ENDPOINT"),
            database_name=os.getenv("FABRIC_COSMOSDB_DATABASE")
        )
        cosmos_conn.initialize()
        print("✓ CosmosDB connected\n")
    except Exception as e:
        print(f"❌ Error connecting to CosmosDB: {e}")
        return
    
    # Initialize embedding service
    try:
        embedding_service = get_embedding_service(use_azure=True)
        print("✓ Embedding service initialized\n")
    except Exception as e:
        print(f"❌ Error initializing embedding service: {e}")
        return
    
    # Check how many products exist in CosmosDB
    try:
        products = cosmos_conn.get_all_products(limit=100)
        print(f"Found {len(products)} products in CosmosDB")
        
        # Check if products have embeddings
        products_with_embeddings = [p for p in products if 'descriptionEmbedding' in p]
        print(f"Products with embeddings: {len(products_with_embeddings)}\n")
        
        if len(products_with_embeddings) == 0:
            print("⚠️  No products with embeddings found!")
            print("   Run 'python scripts/generate_sample_data.py' to create sample products with embeddings.")
            return
    except Exception as e:
        print(f"❌ Error fetching products: {e}")
        return
    
    # Test semantic search
    test_queries = [
        "comfortable laptop for work",
        "wireless audio devices",
        "office furniture"
    ]
    
    for query in test_queries:
        print(f"\n{'='*60}")
        print(f"Query: '{query}'")
        print(f"{'='*60}")
        
        try:
            # Generate embedding for query
            query_embeddings = await embedding_service.generate_embeddings([query])
            
            if query_embeddings and len(query_embeddings) > 0:
                # Search products
                results_df = cosmos_conn.search_products_by_embedding(
                    query_embedding=query_embeddings[0],
                    limit=3,
                    similarity_threshold=0.5
                )
                
                if not results_df.empty:
                    print(f"\nFound {len(results_df)} results:")
                    for idx, product in results_df.iterrows():
                        similarity = 1 - product.get('similarity', 1)
                        print(f"\n  {idx+1}. {product.get('productName', 'Unknown')}")
                        print(f"     Category: {product.get('category', 'N/A')}")
                        print(f"     Price: ${product.get('price', 0):.2f}")
                        print(f"     Match: {similarity:.2%}")
                else:
                    print("  No results found.")
            else:
                print("  Failed to generate embedding.")
                
        except Exception as e:
            print(f"  ❌ Error: {e}")
    
    print(f"\n{'='*60}")
    print("✅ Semantic search test completed!")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
