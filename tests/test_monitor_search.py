from src.database.fabric_cosmos import FabricCosmosDBConnector
from src.agent_integration import generate_embeddings
import os
from dotenv import load_dotenv
import json

load_dotenv()

conn = FabricCosmosDBConnector(
    os.getenv('FABRIC_COSMOSDB_ENDPOINT'),
    os.getenv('FABRIC_COSMOSDB_DATABASE')
)
conn.initialize()

# Test search for "monitor"
embedding = generate_embeddings(['monitor'], None, use_azure=True)
results = conn.search_products_by_embedding(embedding[0], limit=2, similarity_threshold=0.15)

print(f'Found: {len(results)} products')

for idx, row in results.iterrows():
    print(f'\n{"="*80}')
    print(f'Product {idx+1}:')
    product_dict = {k: v for k, v in dict(row).items() if k != 'descriptionEmbedding' and not k.startswith('_')}
    print(json.dumps(product_dict, indent=2, default=str))
    
    # Test the same logic as in the UI
    product_name = (row.get('productName') or 
                   row.get('name') or 
                   row.get('productname') or 
                   'Unknown Product')
    print(f'\nExtracted product name: "{product_name}"')
    print(f'Has productName key: {"productName" in row}')
    if 'productName' in row:
        print(f'productName value: "{row["productName"]}"')
        print(f'productName type: {type(row["productName"])}')
