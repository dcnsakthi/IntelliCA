"""
Generate sample data for the Customer Analytics Platform using Microsoft Fabric
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

from src.database import AzureSQLConnector, CosmosDBConnector
from src.agent_integration import get_embedding_service, generate_embeddings

fake = Faker()


def generate_customer_data(sql_conn, num_customers=100):
    """Generate sample customer data."""
    print(f"Generating {num_customers} customers...")
    
    segments = ['Bronze', 'Silver', 'Gold', 'Premium']
    
    for i in range(num_customers):
        customer_data = {
            'FirstName': fake.first_name(),
            'LastName': fake.last_name(),
            'Email': fake.email(),
            'Phone': fake.phone_number()[:20],
            'DateOfBirth': fake.date_of_birth(minimum_age=18, maximum_age=80),
            'Country': fake.country()[:100],
            'City': fake.city()[:100],
            'CustomerSegment': random.choice(segments)
        }
        
        try:
            sql_conn.insert_customer(customer_data)
        except Exception as e:
            print(f"Error inserting customer {i+1}: {e}")
    
    print(f"✓ Generated {num_customers} customers")


def generate_product_data(cosmos_conn, embedding_service, num_products=50):
    """Generate sample product data with embeddings for CosmosDB NoSQL."""
    print(f"Generating {num_products} products with embeddings...")
    
    categories = {
        'Electronics': ['Smartphones', 'Laptops', 'Tablets', 'Accessories'],
        'Clothing': ['Men', 'Women', 'Kids', 'Accessories'],
        'Home': ['Furniture', 'Decor', 'Kitchen', 'Bedding'],
        'Sports': ['Fitness', 'Outdoor', 'Team Sports', 'Water Sports']
    }
    
    brands = ['TechPro', 'StyleMax', 'HomeComfort', 'SportFit', 'Premium Choice']
    
    for i in range(num_products):
        category = random.choice(list(categories.keys()))
        subcategory = random.choice(categories[category])
        brand = random.choice(brands)
        
        product_name = f"{brand} {subcategory} Item {i+1}"
        description = f"High-quality {subcategory.lower()} product from {brand}. " \
                     f"Perfect for everyday use with excellent features and durability."
        
        # Generate embedding
        try:
            embedding = generate_embeddings([description], embedding_service)
            if isinstance(embedding, list) and len(embedding) > 0:
                embedding_vector = embedding[0] if isinstance(embedding[0], list) else embedding
            else:
                embedding_vector = embedding
        except Exception as e:
            print(f"Warning: Could not generate embedding for product {i+1}: {e}")
            # Create a dummy embedding with 1536 dimensions (OpenAI ada-002 size)
            embedding_vector = [0.0] * 1536
        
        product_data = {
            'id': f"product-{i+1:05d}",
            'productId': f"product-{i+1:05d}",
            'sku': f"SKU-{i+1:05d}",
            'name': product_name,
            'brand': brand,
            'category': category,
            'subcategory': subcategory,
            'description': description,
            'longDescription': description + " Features include premium materials, modern design, and reliable performance.",
            'price': round(random.uniform(10.0, 500.0), 2),
            'cost': round(random.uniform(5.0, 250.0), 2),
            'stockQuantity': random.randint(0, 100),
            'isActive': True,
            'rating': round(random.uniform(3.0, 5.0), 1),
            'reviewCount': random.randint(0, 500),
            'createdAt': datetime.utcnow().isoformat(),
            'updatedAt': datetime.utcnow().isoformat()
        }
        
        try:
            cosmos_conn.create_product(product_data, embedding_vector)
        except Exception as e:
            print(f"Error inserting product {i+1}: {e}")
    
    print(f"✓ Generated {num_products} products with embeddings")


def generate_order_data(sql_conn, num_orders=500):
    """Generate sample order data."""
    print(f"Generating {num_orders} orders...")
    
    # First, get list of actual customer IDs from database
    customer_ids = []
    try:
        with sql_conn.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT CustomerID FROM ca.Customers")
                customer_ids = [row[0] for row in cursor.fetchall()]
    except Exception as e:
        print(f"Error fetching customer IDs: {e}")
        return
    
    if not customer_ids:
        print("No customers found in database. Please run generate_customer_data first.")
        return
    
    order_statuses = ['Pending', 'Processing', 'Shipped', 'Delivered', 'Cancelled']
    
    for i in range(num_orders):
        customer_id = random.choice(customer_ids)  # Use actual customer IDs
        order_date = datetime.now() - timedelta(days=random.randint(0, 365))
        
        # Generate random order amount
        total_amount = round(random.uniform(20.0, 1000.0), 2)
        
        order_data = {
            'customer_id': customer_id,
            'order_date': order_date,
            'total_amount': total_amount,
            'order_status': random.choice(order_statuses)
        }
        
        try:
            # Insert order into database
            query = """
            INSERT INTO ca.Orders (CustomerID, OrderDate, TotalAmount, OrderStatus)
            VALUES (?, ?, ?, ?)
            """
            with sql_conn.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query, (
                        order_data['customer_id'],
                        order_data['order_date'],
                        order_data['total_amount'],
                        order_data['order_status']
                    ))
                    conn.commit()
        except Exception as e:
            print(f"Error inserting order {i+1}: {e}")
    
    print(f"✓ Generated {num_orders} orders")


def generate_review_data(cosmos_conn, embedding_service, num_reviews=200):
    """Generate sample product review data for CosmosDB NoSQL."""
    print(f"Generating {num_reviews} reviews...")
    
    sentiment_labels = ['positive', 'neutral', 'negative']
    review_templates = {
        'positive': [
            "Excellent product! Highly recommend.",
            "Great quality and fast shipping. Very satisfied!",
            "Amazing! Exceeded my expectations.",
            "Perfect! Exactly what I needed.",
            "Love it! Will definitely buy again."
        ],
        'neutral': [
            "It's okay. Does the job but nothing special.",
            "Decent product for the price.",
            "Average quality. Met expectations.",
            "Not bad, not great. It's fine.",
            "Works as expected."
        ],
        'negative': [
            "Disappointed. Not as described.",
            "Poor quality. Would not recommend.",
            "Broke after a few uses. Waste of money.",
            "Not satisfied with this purchase.",
            "Below expectations. Returning it."
        ]
    }
    
    # Get product IDs from CosmosDB
    try:
        products = cosmos_conn.get_all_products(limit=100)
        product_ids = [p['productId'] for p in products if 'productId' in p]
    except Exception as e:
        print(f"Error fetching products: {e}")
        # Fallback to generated IDs
        product_ids = [f"product-{i:05d}" for i in range(1, 51)]
    
    if not product_ids:
        print("⚠️  No products found. Please generate products first.")
        return
    
    for i in range(num_reviews):
        product_id = random.choice(product_ids)
        customer_id = random.randint(1, 100)  # Assuming 100 customers
        
        # Determine sentiment and rating
        sentiment = random.choice(sentiment_labels)
        if sentiment == 'positive':
            rating = random.randint(4, 5)
            sentiment_score = round(random.uniform(0.5, 1.0), 2)
        elif sentiment == 'neutral':
            rating = 3
            sentiment_score = round(random.uniform(-0.3, 0.3), 2)
        else:  # negative
            rating = random.randint(1, 2)
            sentiment_score = round(random.uniform(-1.0, -0.5), 2)
        
        review_text = random.choice(review_templates[sentiment])
        review_date = datetime.now() - timedelta(days=random.randint(0, 180))
        verified_purchase = random.choice([True, False])
        
        # Generate embedding for review text
        try:
            embedding = generate_embeddings([review_text], embedding_service)
            if isinstance(embedding, list) and len(embedding) > 0:
                embedding_vector = embedding[0] if isinstance(embedding[0], list) else embedding
            else:
                embedding_vector = embedding
        except Exception as e:
            print(f"Warning: Could not generate embedding for review {i+1}: {e}")
            # Create a dummy embedding with 1536 dimensions (OpenAI ada-002 size)
            embedding_vector = [0.0] * 1536
        
        review_data = {
            'id': f"review-{i+1:05d}",
            'reviewId': f"review-{i+1:05d}",
            'productId': product_id,
            'customerId': str(customer_id),
            'rating': rating,
            'reviewText': review_text,
            'reviewDate': review_date.isoformat(),
            'verifiedPurchase': verified_purchase,
            'sentimentLabel': sentiment,
            'sentimentScore': sentiment_score,
            'helpfulCount': random.randint(0, 50),
            'createdAt': datetime.utcnow().isoformat()
        }
        
        try:
            cosmos_conn.create_review(review_data, embedding_vector)
        except Exception as e:
            print(f"Error inserting review {i+1}: {e}")
    
    print(f"✓ Generated {num_reviews} product reviews")


def generate_session_data(cosmos_conn, num_sessions=50):
    """Generate sample session data."""
    print(f"Generating {num_sessions} sessions...")
    
    pages = ['/home', '/products', '/cart', '/checkout', '/account']
    
    for i in range(num_sessions):
        customer_id = random.randint(1, 100)
        session_id = f"session-{fake.uuid4()}"
        
        session_data = {
            'id': session_id,
            'sessionId': session_id,
            'customerId': str(customer_id),
            'startTime': (datetime.utcnow() - timedelta(days=random.randint(0, 30))).isoformat(),
            'landingPage': random.choice(pages),
            'status': 'completed',
            'deviceType': random.choice(['desktop', 'mobile', 'tablet']),
            'browser': random.choice(['Chrome', 'Firefox', 'Safari', 'Edge']),
            'duration': random.randint(60, 3600),
            'events': []
        }
        
        # Add some events
        num_events = random.randint(3, 10)
        for j in range(num_events):
            event = {
                'eventType': random.choice(['pageView', 'productView', 'addToCart', 'search']),
                'timestamp': (datetime.utcnow() - timedelta(minutes=random.randint(0, 60))).isoformat(),
                'page': random.choice(pages)
            }
            session_data['events'].append(event)
        
        session_data['eventCount'] = num_events
        
        try:
            cosmos_conn.create_session(session_data)
        except Exception as e:
            print(f"Error inserting session {i+1}: {e}")
    
    print(f"✓ Generated {num_sessions} sessions")


def main():
    """Main function to generate all sample data."""
    print("=" * 60)
    print("Sample Data Generator for Customer Analytics Platform")
    print("Microsoft Fabric SQL Database + Fabric CosmosDB NoSQL")
    print("=" * 60)
    
    # Initialize connectors
    print("\n📡 Initializing database connections...")
    
    try:
        # Fabric SQL Database (Entra ID authentication)
        driver = os.getenv("FABRIC_SQL_DRIVER", "ODBC Driver 18 for SQL Server")
        print(f"Using ODBC driver: {driver}")

        sql_conn = AzureSQLConnector(
            endpoint=os.getenv("FABRIC_SQL_ENDPOINT"),
            database=os.getenv("FABRIC_SQL_DATABASE"),
            driver=f"{{{driver}}}"
        )
        print("✓ Fabric SQL Database connected")
        
        # Fabric CosmosDB NoSQL (Entra ID authentication)
        cosmos_conn = CosmosDBConnector(
            endpoint=os.getenv("FABRIC_COSMOSDB_ENDPOINT"),
            database_name=os.getenv("FABRIC_COSMOSDB_DATABASE", "IntelliCAPDB"),
            container_name=os.getenv("FABRIC_COSMOSDB_SESSIONS_CONTAINER", "Sessions"),
            products_container_name=os.getenv("FABRIC_COSMOSDB_PRODUCTS_CONTAINER", "Products"),
            reviews_container_name=os.getenv("FABRIC_COSMOSDB_REVIEWS_CONTAINER", "Reviews")
        )
        cosmos_conn.initialize()
        print("✓ Fabric CosmosDB NoSQL connected")
        
        # Initialize embedding service
        embedding_service = get_embedding_service(use_azure=True)
        print("✓ Embedding service initialized")
        
    except Exception as e:
        print(f"❌ Error initializing connections: {e}")
        import traceback
        traceback.print_exc()
        return
    
    print("\n📊 Generating sample data...")
    print("-" * 60)
    
    # Generate data
    try:
        generate_customer_data(sql_conn, num_customers=20)  # Reduced for faster testing
        generate_order_data(sql_conn, num_orders=50)  # Reduced for faster testing
        generate_product_data(cosmos_conn, embedding_service, num_products=10)  # Reduced for faster testing
        generate_review_data(cosmos_conn, embedding_service, num_reviews=20)  # Reduced for faster testing
        generate_session_data(cosmos_conn, num_sessions=10)  # Reduced for faster testing
    except Exception as e:
        print(f"❌ Error generating data: {e}")
        import traceback
        traceback.print_exc()
        return
    
    print("\n" + "=" * 60)
    print("✅ Sample data generation completed successfully!")
    print("=" * 60)
    print("\nSummary:")
    print("  - Fabric SQL Database: 20 customers, 50 orders")
    print("  - Fabric CosmosDB NoSQL: 10 products, 20 reviews, 10 sessions")
    print("\nYou can now run the Streamlit application:")
    print("  $env:OTEL_SDK_DISABLED=\"true\"; streamlit run Home.py")


if __name__ == "__main__":
    main()
