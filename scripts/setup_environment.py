"""
Comprehensive Setup Script for IntelliCA Microsoft Fabric Environment
===================================================================
This script initializes all required data for a new environment:
1. Fabric SQL Database: Customers, Products, Orders, OrderItems
2. Fabric CosmosDB NoSQL: Products with embeddings, Reviews with embeddings, Sessions

Run this script once when setting up a new environment.
"""
import sys
import os
from datetime import datetime, timedelta
import random
from faker import Faker
import json

# Add parent directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from dotenv import load_dotenv
load_dotenv()

from src.database.fabric_sql import FabricSQLConnector
from src.database.fabric_cosmos import FabricCosmosDBConnector
from src.agent_integration import get_embedding_service, generate_embeddings

fake = Faker()


# ============================================================================
# FABRIC SQL DATABASE - Data Generation
# ============================================================================

def setup_sql_products(sql_conn):
    """Generate sample products in SQL Database."""
    print("\n" + "="*60)
    print("Setting up SQL Products")
    print("="*60)
    
    products = [
        ("Laptop Pro 15", "SKU-001", "Electronics", "Computers", 1299.99, 50),
        ("Wireless Mouse", "SKU-002", "Electronics", "Accessories", 29.99, 150),
        ("USB-C Hub", "SKU-003", "Electronics", "Accessories", 49.99, 200),
        ("4K Monitor", "SKU-004", "Electronics", "Displays", 399.99, 75),
        ("Mechanical Keyboard", "SKU-005", "Electronics", "Accessories", 129.99, 100),
        ("Noise-Canceling Headphones", "SKU-006", "Electronics", "Audio", 249.99, 60),
        ("Portable SSD 1TB", "SKU-007", "Electronics", "Storage", 119.99, 120),
        ("Webcam HD", "SKU-008", "Electronics", "Accessories", 79.99, 90),
        ("Standing Desk", "SKU-009", "Furniture", "Office", 499.99, 30),
        ("Ergonomic Chair", "SKU-010", "Furniture", "Office", 349.99, 45),
        ("Desk Lamp LED", "SKU-011", "Furniture", "Lighting", 39.99, 150),
        ("Monitor Arm", "SKU-012", "Furniture", "Accessories", 89.99, 80),
        ("Smart Watch", "SKU-013", "Electronics", "Wearables", 299.99, 100),
        ("Fitness Tracker", "SKU-014", "Electronics", "Wearables", 79.99, 150),
        ("Tablet 10inch", "SKU-015", "Electronics", "Tablets", 449.99, 70),
        ("Bluetooth Speaker", "SKU-016", "Electronics", "Audio", 99.99, 120),
        ("Power Bank 20000mAh", "SKU-017", "Electronics", "Accessories", 39.99, 200),
        ("Wireless Charger", "SKU-018", "Electronics", "Accessories", 29.99, 180),
        ("Gaming Mouse Pad", "SKU-019", "Electronics", "Accessories", 24.99, 250),
        ("Cable Management Box", "SKU-020", "Furniture", "Organization", 19.99, 300)
    ]
    
    inserted = 0
    for product in products:
        try:
            query = """
                INSERT INTO ca.Products 
                (ProductName, SKU, Category, SubCategory, UnitPrice, StockQuantity, IsActive)
                VALUES (?, ?, ?, ?, ?, ?, 1)
            """
            sql_conn.execute_non_query(query, product)
            inserted += 1
            print(f"  ✓ Inserted: {product[0]}")
        except Exception as e:
            if "duplicate key" in str(e).lower() or "violation" in str(e).lower():
                print(f"  ⏭️  Skipped (exists): {product[0]}")
            else:
                print(f"  ✗ Error inserting {product[0]}: {e}")
    
    print(f"\n✓ SQL Products setup complete: {inserted} products inserted\n")
    return inserted


def setup_sql_customers(sql_conn, num_customers=100):
    """Generate sample customers in SQL Database."""
    print("\n" + "="*60)
    print(f"Setting up SQL Customers ({num_customers} records)")
    print("="*60)
    
    segments = ['Bronze', 'Silver', 'Gold', 'Premium']
    inserted = 0
    
    for i in range(num_customers):
        try:
            query = """
                INSERT INTO ca.Customers 
                (FirstName, LastName, Email, Phone, DateOfBirth, Country, City, CustomerSegment, IsActive)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1)
            """
            customer_data = (
                fake.first_name(),
                fake.last_name(),
                fake.email(),
                fake.phone_number()[:20],
                fake.date_of_birth(minimum_age=18, maximum_age=80),
                fake.country()[:100],
                fake.city()[:100],
                random.choice(segments)
            )
            sql_conn.execute_non_query(query, customer_data)
            inserted += 1
            if (i + 1) % 20 == 0:
                print(f"  ✓ Inserted {i + 1} customers...")
        except Exception as e:
            if "duplicate key" in str(e).lower() or "violation" in str(e).lower():
                continue
            else:
                print(f"  ✗ Error inserting customer {i+1}: {e}")
    
    print(f"\n✓ SQL Customers setup complete: {inserted} customers inserted\n")
    return inserted


def setup_sql_orders(sql_conn, num_orders=200):
    """Generate sample orders in SQL Database."""
    print("\n" + "="*60)
    print(f"Setting up SQL Orders ({num_orders} records)")
    print("="*60)
    
    # Get customer IDs
    customers_df = sql_conn.execute_query("SELECT TOP 100 CustomerID FROM ca.Customers WHERE IsActive = 1")
    if customers_df.empty:
        print("  ⚠️  No customers found. Please run setup_sql_customers first.")
        return 0
    
    # Convert to native Python int to avoid numpy.int64 issues
    customer_ids = [int(cid) for cid in customers_df['CustomerID'].tolist()]
    
    # Get product IDs and prices
    products_df = sql_conn.execute_query("SELECT ProductID, UnitPrice FROM ca.Products WHERE IsActive = 1")
    if products_df.empty:
        print("  ⚠️  No products found. Please run setup_sql_products first.")
        return 0
    
    inserted = 0
    for i in range(num_orders):
        try:
            customer_id = int(random.choice(customer_ids))  # Ensure int
            order_date = fake.date_time_between(start_date='-2y', end_date='now')
            
            # Insert order
            order_query = """
                INSERT INTO ca.Orders (CustomerID, OrderDate, TotalAmount, OrderStatus)
                OUTPUT INSERTED.OrderID
                VALUES (?, ?, ?, ?)
            """
            order_status = random.choice(['Pending', 'Processing', 'Shipped', 'Delivered', 'Cancelled'])
            result = sql_conn.execute_query(order_query, (customer_id, order_date, 0.0, order_status))
            
            if not result.empty:
                # Convert order_id to native Python int
                order_id = int(result.iloc[0]['OrderID'])
                
                # Insert order details (1-5 products per order)
                num_items = random.randint(1, 5)
                total_amount = 0.0
                
                for _ in range(num_items):
                    product = products_df.sample(1).iloc[0]
                    quantity = int(random.randint(1, 3))  # Ensure int
                    # Convert product_id to native Python int
                    product_id = int(product['ProductID'])
                    unit_price = float(product['UnitPrice'])
                    line_total = float(quantity * unit_price)  # Ensure float
                    total_amount += line_total
                    
                    detail_query = """
                        INSERT INTO ca.OrderItems (OrderID, ProductID, Quantity, UnitPrice, LineTotal)
                        VALUES (?, ?, ?, ?, ?)
                    """
                    sql_conn.execute_non_query(detail_query, (order_id, product_id, quantity, unit_price, line_total))
                
                # Update order total - ensure float
                update_query = "UPDATE ca.Orders SET TotalAmount = ? WHERE OrderID = ?"
                sql_conn.execute_non_query(update_query, (float(total_amount), order_id))
                
                inserted += 1
                if (i + 1) % 50 == 0:
                    print(f"  ✓ Inserted {i + 1} orders...")
        except Exception as e:
            print(f"  ✗ Error inserting order {i+1}: {e}")
    
    print(f"\n✓ SQL Orders setup complete: {inserted} orders inserted\n")
    return inserted


# ============================================================================
# FABRIC COSMOSDB - Data Generation
# ============================================================================

def setup_cosmos_products(cosmos_conn, embedding_service):
    """Generate products with embeddings in CosmosDB."""
    print("\n" + "="*60)
    print("Setting up CosmosDB Products with Embeddings")
    print("="*60)
    
    products = [
        {
            "id": "product-00001", "productId": "product-00001", "name": "Laptop Pro 15", "brand": "TechBrand",
            "category": "Electronics", "subcategory": "Computers", 
            "description": "High-performance 15-inch laptop with Intel i7 processor, 16GB RAM, and 512GB SSD. Perfect for professionals and content creators.",
            "price": 1299.99, "stockQuantity": 50
        },
        {
            "id": "product-00002", "productId": "product-00002", "name": "Wireless Mouse", "brand": "TechBrand",
            "category": "Electronics", "subcategory": "Accessories",
            "description": "Ergonomic wireless mouse with precision tracking and long battery life. Comfortable for all-day use.",
            "price": 29.99, "stockQuantity": 150
        },
        {
            "id": "product-00003", "productId": "product-00003", "name": "USB-C Hub", "brand": "TechBrand",
            "category": "Electronics", "subcategory": "Accessories",
            "description": "Multi-port USB-C hub with HDMI, USB 3.0, and SD card reader. Essential for modern laptops.",
            "price": 49.99, "stockQuantity": 200
        },
        {
            "id": "product-00004", "productId": "product-00004", "name": "4K Monitor", "brand": "ViewTech",
            "category": "Electronics", "subcategory": "Displays",
            "description": "Stunning 27-inch 4K UHD monitor with HDR support and adjustable stand. Crystal clear display for work and entertainment.",
            "price": 399.99, "stockQuantity": 75
        },
        {
            "id": "product-00005", "productId": "product-00005", "name": "Mechanical Keyboard", "brand": "TechBrand",
            "category": "Electronics", "subcategory": "Accessories",
            "description": "Premium mechanical keyboard with RGB backlighting and customizable keys. Cherry MX switches for superior typing experience.",
            "price": 129.99, "stockQuantity": 100
        },
        {
            "id": "product-00006", "productId": "product-00006", "name": "Noise-Canceling Headphones", "brand": "AudioMax",
            "category": "Electronics", "subcategory": "Audio",
            "description": "Premium over-ear headphones with active noise cancellation and 30-hour battery life. Studio-quality sound.",
            "price": 249.99, "stockQuantity": 60
        },
        {
            "id": "product-00007", "productId": "product-00007", "name": "Portable SSD 1TB", "brand": "DataStore",
            "category": "Electronics", "subcategory": "Storage",
            "description": "Ultra-fast portable SSD with 1TB capacity. USB 3.2 Gen 2 for lightning-fast file transfers.",
            "price": 119.99, "stockQuantity": 120
        },
        {
            "id": "product-00008", "productId": "product-00008", "name": "Webcam HD", "brand": "ViewTech",
            "category": "Electronics", "subcategory": "Accessories",
            "description": "1080p HD webcam with auto-focus and built-in microphone. Perfect for video calls and streaming.",
            "price": 79.99, "stockQuantity": 90
        },
        {
            "id": "product-00009", "productId": "product-00009", "name": "Standing Desk", "brand": "ErgoWork",
            "category": "Furniture", "subcategory": "Office",
            "description": "Adjustable height standing desk with electric motor. Promotes healthy posture and productivity.",
            "price": 499.99, "stockQuantity": 30
        },
        {
            "id": "product-00010", "productId": "product-00010", "name": "Ergonomic Chair", "brand": "ErgoWork",
            "category": "Furniture", "subcategory": "Office",
            "description": "Premium ergonomic office chair with lumbar support and adjustable armrests. All-day comfort.",
            "price": 349.99, "stockQuantity": 45
        }
    ]
    
    inserted = 0
    for product in products:
        try:
            # Generate embedding for product
            text = f"{product['name']} {product['description']}"
            embeddings = generate_embeddings([text], embedding_service, use_azure=True)
            
            if embeddings and len(embeddings) > 0:
                product['embedding'] = embeddings[0]
                product['type'] = 'product'
                product['createdAt'] = datetime.utcnow().isoformat()
                
                cosmos_conn.products_container.upsert_item(body=product)
                inserted += 1
                print(f"  ✓ Inserted: {product['name']}")
            else:
                print(f"  ⚠️  Skipped {product['name']}: Could not generate embedding")
        except Exception as e:
            if "Conflict" in str(e) or "409" in str(e):
                print(f"  ⏭️  Skipped (exists): {product['name']}")
            else:
                print(f"  ✗ Error inserting {product['name']}: {e}")
    
    print(f"\n✓ CosmosDB Products setup complete: {inserted} products inserted\n")
    return inserted


def setup_cosmos_reviews(cosmos_conn, embedding_service):
    """Generate reviews with embeddings for SQL products in CosmosDB."""
    print("\n" + "="*60)
    print("Setting up CosmosDB Reviews with Embeddings")
    print("="*60)
    
    # Review templates
    positive_templates = [
        "Excellent {product}! {detail} Highly recommend for anyone looking for quality.",
        "Love this {product}! {detail} Best purchase I've made in a while.",
        "Outstanding {product}. {detail} Worth every penny!",
        "Perfect {product} for my needs. {detail} Very satisfied with this purchase.",
        "Amazing quality {product}! {detail} Exceeded my expectations."
    ]
    
    negative_templates = [
        "Disappointed with this {product}. {detail} Would not recommend.",
        "Not happy with this {product}. {detail} Expected better quality.",
        "Poor quality {product}. {detail} Returning this item.",
        "Unsatisfied with {product}. {detail} Not worth the price.",
        "Regret buying this {product}. {detail} Many issues."
    ]
    
    neutral_templates = [
        "Decent {product}. {detail} Gets the job done.",
        "Average {product}. {detail} Nothing special but works fine.",
        "Okay {product}. {detail} Meets basic expectations.",
        "Standard {product}. {detail} Does what it's supposed to do.",
        "Fair {product}. {detail} Good enough for the price."
    ]
    
    # Product details for reviews
    product_details = {
        "laptop": ["Fast performance and great battery life.", "Display is crisp and keyboard comfortable.", "Runs smoothly for work and gaming."],
        "mouse": ["Comfortable grip and responsive.", "Battery lasts long.", "Smooth tracking on all surfaces."],
        "keyboard": ["Keys feel great to type on.", "RGB lighting is customizable.", "Build quality is solid."],
        "headphones": ["Sound quality is excellent.", "Noise cancellation works perfectly.", "Very comfortable for long use."],
        "monitor": ["Colors are vibrant and accurate.", "No dead pixels.", "Stand is sturdy and adjustable."],
        "default": ["Works as expected.", "Good build quality.", "Delivery was fast."]
    }
    
    products = [
        {"id": "PROD-001", "name": "Laptop Pro 15", "type": "laptop"},
        {"id": "PROD-002", "name": "Wireless Mouse", "type": "mouse"},
        {"id": "PROD-005", "name": "Mechanical Keyboard", "type": "keyboard"},
        {"id": "PROD-006", "name": "Noise-Canceling Headphones", "type": "headphones"},
        {"id": "PROD-004", "name": "4K Monitor", "type": "monitor"},
    ]
    
    inserted = 0
    for product in products:
        for review_num in range(1, 6):  # 5 reviews per product
            try:
                # Determine sentiment
                rand = random.random()
                if rand < 0.6:  # 60% positive
                    sentiment = "positive"
                    rating = random.randint(4, 5)
                    template = random.choice(positive_templates)
                elif rand < 0.85:  # 25% neutral
                    sentiment = "neutral"
                    rating = 3
                    template = random.choice(neutral_templates)
                else:  # 15% negative
                    sentiment = "negative"
                    rating = random.randint(1, 2)
                    template = random.choice(negative_templates)
                
                # Get product-specific detail
                detail = random.choice(product_details.get(product['type'], product_details['default']))
                review_text = template.format(product=product['name'], detail=detail)
                
                # Generate embedding
                embeddings = generate_embeddings([review_text], embedding_service, use_azure=True)
                
                if embeddings and len(embeddings) > 0:
                    review_data = {
                        "id": f"review-{product['id']}-{review_num:03d}",
                        "productId": product['id'],
                        "reviewText": review_text,
                        "rating": rating,
                        "sentimentLabel": sentiment,
                        "sentimentScore": random.uniform(0.7, 0.99) if sentiment == "positive" else random.uniform(0.01, 0.3),
                        "reviewDate": (datetime.utcnow() - timedelta(days=random.randint(1, 365))).isoformat(),
                        "type": "review",
                        "embedding": embeddings[0]
                    }
                    
                    cosmos_conn.reviews_container.upsert_item(body=review_data)
                    inserted += 1
                    if inserted % 10 == 0:
                        print(f"  ✓ Inserted {inserted} reviews...")
                else:
                    print(f"  ⚠️  Skipped review for {product['name']}: Could not generate embedding")
            except Exception as e:
                if "Conflict" in str(e) or "409" in str(e):
                    continue
                else:
                    print(f"  ✗ Error inserting review: {e}")
    
    print(f"\n✓ CosmosDB Reviews setup complete: {inserted} reviews inserted\n")
    return inserted


# ============================================================================
# MAIN SETUP FUNCTION
# ============================================================================

def main():
    """Main setup function."""
    print("\n" + "="*70)
    print(" IntelliCA Microsoft Fabric Environment Setup")
    print("="*70)
    print("\nThis script will initialize all required data for your environment.")
    print("\nComponents:")
    print("  • Fabric SQL Database: Customers, Products, Orders")
    print("  • Fabric CosmosDB: Products & Reviews (with embeddings)")
    print("\n" + "="*70 + "\n")
    
    try:
        # Initialize SQL connector
        print("Connecting to Fabric SQL Database...")
        sql_conn = FabricSQLConnector(
            endpoint=os.getenv("FABRIC_SQL_ENDPOINT"),
            database=os.getenv("FABRIC_SQL_DATABASE"),
            driver=f"{{{os.getenv('FABRIC_SQL_DRIVER', 'ODBC Driver 18 for SQL Server')}}}"
        )
        print("✓ Fabric SQL Database connected\n")
        
        # Initialize CosmosDB connector
        print("Connecting to Fabric CosmosDB...")
        cosmos_conn = FabricCosmosDBConnector(
            endpoint=os.getenv("FABRIC_COSMOSDB_ENDPOINT"),
            database_name=os.getenv("FABRIC_COSMOSDB_DATABASE", "IntelliCAPDB"),
            container_name=os.getenv("FABRIC_COSMOSDB_SESSIONS_CONTAINER", "Sessions"),
            products_container_name=os.getenv("FABRIC_COSMOSDB_PRODUCTS_CONTAINER", "Products"),
            reviews_container_name=os.getenv("FABRIC_COSMOSDB_REVIEWS_CONTAINER", "Reviews")
        )
        cosmos_conn.initialize()
        print("✓ Fabric CosmosDB connected\n")
        
        # Initialize embedding service
        print("Initializing Azure OpenAI embedding service...")
        embedding_service = get_embedding_service(use_azure=True)
        print("✓ Embedding service initialized\n")
        
        # Setup SQL Database
        sql_products = setup_sql_products(sql_conn)
        sql_customers = setup_sql_customers(sql_conn, num_customers=100)
        sql_orders = setup_sql_orders(sql_conn, num_orders=200)
        
        # Setup CosmosDB
        cosmos_products = setup_cosmos_products(cosmos_conn, embedding_service)
        cosmos_reviews = setup_cosmos_reviews(cosmos_conn, embedding_service)
        
        # Summary
        print("\n" + "="*70)
        print(" Setup Complete!")
        print("="*70)
        print(f"\nFabric SQL Database:")
        print(f"  • Products: {sql_products} records")
        print(f"  • Customers: {sql_customers} records")
        print(f"  • Orders: {sql_orders} records")
        print(f"\nFabric CosmosDB:")
        print(f"  • Products: {cosmos_products} records")
        print(f"  • Reviews: {cosmos_reviews} records")
        print("\n✓ Your IntelliCA environment is ready to use!")
        print("="*70 + "\n")
        
    except Exception as e:
        print(f"\n❌ Setup failed: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
