"""
Generate products with embeddings in CosmosDB
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from dotenv import load_dotenv
load_dotenv()

from src.database.fabric_cosmos import FabricCosmosDBConnector
from src.agent_integration import get_embedding_service, generate_embeddings
import asyncio
from datetime import datetime
import random

# Sample product data
SAMPLE_PRODUCTS = [
    {
        "productId": "PROD-001",
        "sku": "SKU-001",
        "productName": "Laptop Pro 15",
        "brand": "TechBrand",
        "category": "Electronics",
        "subcategory": "Computers",
        "description": "High-performance 15-inch laptop with Intel i7 processor, 16GB RAM, and 512GB SSD. Perfect for professionals and content creators.",
        "price": 1299.99,
        "cost": 899.99,
        "stockQuantity": 50
    },
    {
        "productId": "PROD-002",
        "sku": "SKU-002",
        "productName": "Wireless Mouse",
        "brand": "TechBrand",
        "category": "Electronics",
        "subcategory": "Accessories",
        "description": "Ergonomic wireless mouse with precision tracking and long battery life. Comfortable for all-day use.",
        "price": 29.99,
        "cost": 12.99,
        "stockQuantity": 150
    },
    {
        "productId": "PROD-003",
        "sku": "SKU-003",
        "productName": "USB-C Hub",
        "brand": "TechBrand",
        "category": "Electronics",
        "subcategory": "Accessories",
        "description": "Multi-port USB-C hub with HDMI, USB 3.0, and SD card reader. Essential for modern laptops.",
        "price": 49.99,
        "cost": 22.99,
        "stockQuantity": 200
    },
    {
        "productId": "PROD-004",
        "sku": "SKU-004",
        "productName": "4K Monitor 27\"",
        "brand": "ViewTech",
        "category": "Electronics",
        "subcategory": "Displays",
        "description": "Stunning 27-inch 4K monitor with HDR support and ultra-thin bezels. Ideal for gaming and professional work.",
        "price": 399.99,
        "cost": 249.99,
        "stockQuantity": 75
    },
    {
        "productId": "PROD-005",
        "sku": "SKU-005",
        "productName": "Mechanical Keyboard RGB",
        "brand": "KeyMaster",
        "category": "Electronics",
        "subcategory": "Accessories",
        "description": "Premium mechanical keyboard with customizable RGB lighting and tactile switches. Perfect for typing and gaming.",
        "price": 129.99,
        "cost": 69.99,
        "stockQuantity": 100
    },
    {
        "productId": "PROD-006",
        "sku": "SKU-006",
        "productName": "Noise-Canceling Headphones",
        "brand": "SoundWave",
        "category": "Electronics",
        "subcategory": "Audio",
        "description": "Premium over-ear headphones with active noise cancellation and 30-hour battery life. Exceptional sound quality.",
        "price": 249.99,
        "cost": 149.99,
        "stockQuantity": 60
    },
    {
        "productId": "PROD-007",
        "sku": "SKU-007",
        "productName": "Portable SSD 1TB",
        "brand": "DataStore",
        "category": "Electronics",
        "subcategory": "Storage",
        "description": "Ultra-fast portable SSD with 1TB capacity and USB 3.2 Gen 2 support. Compact and durable design.",
        "price": 119.99,
        "cost": 69.99,
        "stockQuantity": 120
    },
    {
        "productId": "PROD-008",
        "sku": "SKU-008",
        "productName": "Webcam HD 1080p",
        "brand": "ViewTech",
        "category": "Electronics",
        "subcategory": "Accessories",
        "description": "Crystal-clear 1080p webcam with auto-focus and built-in microphone. Perfect for video calls and streaming.",
        "price": 79.99,
        "cost": 39.99,
        "stockQuantity": 90
    },
    {
        "productId": "PROD-009",
        "sku": "SKU-009",
        "productName": "Standing Desk Electric",
        "brand": "ErgoWork",
        "category": "Furniture",
        "subcategory": "Office",
        "description": "Electric height-adjustable standing desk with memory presets and sturdy steel frame. Promotes healthy workspace.",
        "price": 499.99,
        "cost": 299.99,
        "stockQuantity": 30
    },
    {
        "productId": "PROD-010",
        "sku": "SKU-010",
        "productName": "Ergonomic Office Chair",
        "brand": "ErgoWork",
        "category": "Furniture",
        "subcategory": "Office",
        "description": "Premium ergonomic chair with lumbar support, adjustable armrests, and breathable mesh. All-day comfort.",
        "price": 349.99,
        "cost": 199.99,
        "stockQuantity": 45
    }
]

async def generate_products():
    """Generate products with embeddings in CosmosDB"""
    print("=" * 60)
    print("Generating Products with Embeddings in CosmosDB")
    print("=" * 60)
    
    # Initialize CosmosDB
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
    
    print(f"Creating {len(SAMPLE_PRODUCTS)} products with embeddings...\n")
    
    success_count = 0
    for product in SAMPLE_PRODUCTS:
        try:
            # Generate embedding for product description
            description = f"{product['productName']} - {product['description']}"
            embeddings = generate_embeddings([description], embedding_service, use_azure=True)
            
            if not embeddings or len(embeddings) == 0:
                print(f"  ❌ Failed to generate embedding for: {product['productName']}")
                continue
            
            # Add metadata
            product_data = {
                **product,
                'isActive': True,
                'createdAt': datetime.utcnow().isoformat(),
                'updatedAt': datetime.utcnow().isoformat()
            }
            
            # Create product in CosmosDB
            cosmos_conn.create_product(product_data, embeddings[0])
            success_count += 1
            print(f"  ✓ Created: {product['productName']}")
            
        except Exception as e:
            if "Conflict" in str(e):
                print(f"  ⚠️  Already exists: {product['productName']}")
                success_count += 1
            else:
                print(f"  ❌ Error creating {product['productName']}: {e}")
    
    print(f"\n{'='*60}")
    print(f"✅ Successfully created {success_count}/{len(SAMPLE_PRODUCTS)} products")
    print(f"{'='*60}\n")
    print("You can now use semantic search in the Product Recommendations page!")


if __name__ == "__main__":
    asyncio.run(generate_products())
