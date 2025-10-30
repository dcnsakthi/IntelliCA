from src.database.fabric_cosmos import FabricCosmosDBConnector
from src.agent_integration import generate_embeddings
import os
from dotenv import load_dotenv
import pandas as pd

load_dotenv()

conn = FabricCosmosDBConnector(
    os.getenv('FABRIC_COSMOSDB_ENDPOINT'),
    os.getenv('FABRIC_COSMOSDB_DATABASE')
)
conn.initialize()

# Test search for "monitor"
embedding = generate_embeddings(['monitor'], None, use_azure=True)
results_df = conn.search_products_by_embedding(embedding[0], limit=2, similarity_threshold=0.15)

print(f'Found: {len(results_df)} products')
print(f'Columns: {results_df.columns.tolist()}')

print('\nTesting iterrows() like Streamlit does:')
for idx, (_, product) in enumerate(results_df.iterrows(), 1):
    print(f'\n{"="*80}')
    print(f'Product {idx}:')
    print(f'Type of product: {type(product)}')
    
    # Test different access methods
    print(f'\nMethod 1 - product.get():')
    product_name = product.get('productName', 'Unknown Product')
    print(f'  product_name = "{product_name}"')
    
    print(f'\nMethod 2 - product["productName"]:')
    try:
        product_name2 = product['productName']
        print(f'  product_name = "{product_name2}"')
    except Exception as e:
        print(f'  Error: {e}')
    
    print(f'\nMethod 3 - Check if key exists:')
    print(f'  "productName" in product: {"productName" in product}')
    print(f'  Keys: {list(product.keys())[:10]}...')
    
    print(f'\nActual field values:')
    print(f'  productName: {product.get("productName", "MISSING")}')
    print(f'  brand: {product.get("brand", "MISSING")}')
    print(f'  category: {product.get("category", "MISSING")}')
    print(f'  price: {product.get("price", "MISSING")}')
    print(f'  similarity: {product.get("similarity", "MISSING")}')
