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

from src.database.fabric_sql import FabricSQLConnector
from src.database.fabric_cosmos import FabricCosmosDBConnector
from src.agent_integration import get_embedding_service, generate_embeddings

fake = Faker()


def generate_customer_data(sql_conn, num_customers=100, bulk_insert=False):
    """Generate sample customer data."""
    print(f"Generating {num_customers} customers...")
    
    segments = ['Bronze', 'Silver', 'Gold', 'Premium']
    
    if bulk_insert:
        # Generate all data first
        customers = []
        for i in range(num_customers):
            customers.append((
                fake.first_name(),
                fake.last_name(),
                fake.email(),
                fake.phone_number()[:20],
                fake.date_of_birth(minimum_age=18, maximum_age=80),
                fake.country()[:100],
                fake.city()[:100],
                random.choice(segments)
            ))
        
        # Bulk insert
        try:
            query = """
            INSERT INTO ca.Customers 
            (FirstName, LastName, Email, Phone, DateOfBirth, Country, City, CustomerSegment)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """
            with sql_conn.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.executemany(query, customers)
                    conn.commit()
            print(f"✓ Generated {num_customers} customers (bulk insert)")
        except Exception as e:
            print(f"Error bulk inserting customers: {e}")
    else:
        # Row by row insert
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


def generate_sql_product_data(sql_conn, num_products=20, bulk_insert=False):
    """Generate sample product data for SQL Database."""
    print(f"Generating {num_products} products...")
    
    categories = {
        'Electronics': ['Smartphones', 'Laptops', 'Tablets', 'Accessories'],
        'Clothing': ['Men', 'Women', 'Kids', 'Accessories'],
        'Home': ['Furniture', 'Decor', 'Kitchen', 'Bedding'],
        'Sports': ['Fitness', 'Outdoor', 'Team Sports', 'Water Sports']
    }
    
    if bulk_insert:
        # Generate all data first
        products = []
        for i in range(num_products):
            category = random.choice(list(categories.keys()))
            subcategory = random.choice(categories[category])
            products.append((
                f"{subcategory} Item {i+1}",
                f"SKU-{i+1:05d}",
                category,
                subcategory,
                round(random.uniform(10.0, 500.0), 2),
                random.randint(0, 200)
            ))
        
        # Bulk insert
        try:
            query = """
            INSERT INTO ca.Products (ProductName, SKU, Category, SubCategory, UnitPrice, StockQuantity)
            VALUES (?, ?, ?, ?, ?, ?)
            """
            with sql_conn.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.executemany(query, products)
                    conn.commit()
            print(f"✓ Generated {num_products} products (bulk insert)")
        except Exception as e:
            print(f"Error bulk inserting products: {e}")
    else:
        # Row by row insert
        for i in range(num_products):
            category = random.choice(list(categories.keys()))
            subcategory = random.choice(categories[category])
            
            product_name = f"{subcategory} Item {i+1}"
            sku = f"SKU-{i+1:05d}"
            unit_price = round(random.uniform(10.0, 500.0), 2)
            stock_quantity = random.randint(0, 200)
            
            try:
                query = """
                INSERT INTO ca.Products (ProductName, SKU, Category, SubCategory, UnitPrice, StockQuantity)
                VALUES (?, ?, ?, ?, ?, ?)
                """
                with sql_conn.get_connection() as conn:
                    with conn.cursor() as cursor:
                        cursor.execute(query, (
                            product_name,
                            sku,
                            category,
                            subcategory,
                            unit_price,
                            stock_quantity
                        ))
                        conn.commit()
            except Exception as e:
                print(f"Error inserting product {i+1}: {e}")
        
        print(f"✓ Generated {num_products} products")


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


def generate_order_data(sql_conn, num_orders=500, bulk_insert=False):
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
    
    if bulk_insert:
        # Generate all data first
        orders = []
        for i in range(num_orders):
            customer_id = random.choice(customer_ids)
            order_date = datetime.now() - timedelta(days=random.randint(0, 365))
            total_amount = round(random.uniform(20.0, 1000.0), 2)
            order_status = random.choice(order_statuses)
            orders.append((customer_id, order_date, total_amount, order_status))
        
        # Bulk insert
        try:
            query = """
            INSERT INTO ca.Orders (CustomerID, OrderDate, TotalAmount, OrderStatus)
            VALUES (?, ?, ?, ?)
            """
            with sql_conn.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.executemany(query, orders)
                    conn.commit()
            print(f"✓ Generated {num_orders} orders (bulk insert)")
        except Exception as e:
            print(f"Error bulk inserting orders: {e}")
    else:
        # Row by row insert
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


def generate_order_items_data(sql_conn, num_items_per_order=None, bulk_insert=False):
    """Generate sample order items data."""
    print(f"Generating order items...")
    
    # Get all orders
    try:
        with sql_conn.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT OrderID FROM ca.Orders")
                order_ids = [row[0] for row in cursor.fetchall()]
    except Exception as e:
        print(f"Error fetching order IDs: {e}")
        return
    
    if not order_ids:
        print("No orders found in database. Please run generate_order_data first.")
        return
    
    # Get all products
    try:
        with sql_conn.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT ProductID, UnitPrice FROM ca.Products")
                products = cursor.fetchall()
    except Exception as e:
        print(f"Error fetching products: {e}")
        return
    
    if not products:
        print("No products found in database. Please run generate_sql_product_data first.")
        return
    
    if bulk_insert:
        # Generate all data first
        order_items = []
        for order_id in order_ids:
            # Random number of items per order (1-5)
            num_items = num_items_per_order if num_items_per_order else random.randint(1, 5)
            
            # Select random products for this order
            selected_products = random.sample(products, min(num_items, len(products)))
            
            for product_id, unit_price in selected_products:
                quantity = random.randint(1, 3)
                discount = random.choice([0.0, 0.0, 0.0, 5.0, 10.0, 15.0])  # Mostly no discount
                # Ensure proper types: int, int, int, float, float
                order_items.append((
                    int(order_id), 
                    int(product_id), 
                    quantity, 
                    float(unit_price), 
                    discount
                ))
        
        # Bulk insert
        try:
            query = """
            INSERT INTO ca.OrderItems (OrderID, ProductID, Quantity, UnitPrice, Discount)
            VALUES (?, ?, ?, ?, ?)
            """
            with sql_conn.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.executemany(query, order_items)
                    conn.commit()
            print(f"✓ Generated {len(order_items)} order items (bulk insert)")
        except Exception as e:
            print(f"Error bulk inserting order items: {e}")
    else:
        # Row by row insert
        total_items = 0
        for order_id in order_ids:
            # Random number of items per order (1-5)
            num_items = num_items_per_order if num_items_per_order else random.randint(1, 5)
            
            # Select random products for this order
            selected_products = random.sample(products, min(num_items, len(products)))
            
            for product_id, unit_price in selected_products:
                quantity = random.randint(1, 3)
                discount = random.choice([0.0, 0.0, 0.0, 5.0, 10.0, 15.0])  # Mostly no discount
                
                try:
                    query = """
                    INSERT INTO ca.OrderItems (OrderID, ProductID, Quantity, UnitPrice, Discount)
                    VALUES (?, ?, ?, ?, ?)
                    """
                    with sql_conn.get_connection() as conn:
                        with conn.cursor() as cursor:
                            cursor.execute(query, (
                                int(order_id),
                                int(product_id),
                                quantity,
                                float(unit_price),
                                discount
                            ))
                            conn.commit()
                    total_items += 1
                except Exception as e:
                    print(f"Error inserting order item for order {order_id}: {e}")
        
        print(f"✓ Generated {total_items} order items")


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


def truncate_sql_table(sql_conn, table_name):
    """Truncate a SQL table."""
    try:
        # Check if table has foreign key constraints
        if table_name.lower() == 'ca.customers':
            # Truncate related tables first
            print(f"  Truncating related tables first...")
            sql_conn.execute_non_query("DELETE FROM ca.OrderItems")
            sql_conn.execute_non_query("DELETE FROM ca.Orders")
            sql_conn.execute_non_query("DELETE FROM ca.Customers")
        elif table_name.lower() == 'ca.products':
            # Truncate order items first (has FK to Products)
            sql_conn.execute_non_query("DELETE FROM ca.OrderItems")
            sql_conn.execute_non_query("DELETE FROM ca.Products")
        elif table_name.lower() == 'ca.orders':
            sql_conn.execute_non_query("DELETE FROM ca.OrderItems")
            sql_conn.execute_non_query("DELETE FROM ca.Orders")
        elif table_name.lower() == 'ca.orderitems':
            sql_conn.execute_non_query("DELETE FROM ca.OrderItems")
        else:
            sql_conn.execute_non_query(f"TRUNCATE TABLE {table_name}")
        print(f"  ✓ Truncated {table_name}")
        return True
    except Exception as e:
        print(f"  ❌ Error truncating {table_name}: {e}")
        return False


def truncate_cosmos_container(cosmos_conn, container_name):
    """Delete all items from a CosmosDB container."""
    try:
        container = None
        if container_name.lower() == 'products':
            container = cosmos_conn.products_container
        elif container_name.lower() == 'reviews':
            container = cosmos_conn.reviews_container
        elif container_name.lower() == 'sessions':
            container = cosmos_conn.container
        
        if not container:
            print(f"  ❌ Unknown container: {container_name}")
            return False
        
        # Query all items
        items = list(container.query_items(
            query="SELECT c.id, c._partitionKey FROM c",
            enable_cross_partition_query=True
        ))
        
        print(f"  Deleting {len(items)} items from {container_name}...")
        for item in items:
            try:
                container.delete_item(item=item['id'], partition_key=item.get('_partitionKey'))
            except:
                pass  # Item might already be deleted
        
        print(f"  ✓ Cleared {container_name} container")
        return True
    except Exception as e:
        print(f"  ❌ Error clearing {container_name}: {e}")
        return False


def get_user_choice(prompt, options):
    """Get user choice from a list of options."""
    while True:
        print(f"\n{prompt}")
        for i, option in enumerate(options, 1):
            print(f"  {i}. {option}")
        try:
            choice = input("\nEnter your choice (number): ").strip()
            choice_num = int(choice)
            if 1 <= choice_num <= len(options):
                return choice_num - 1
            else:
                print(f"❌ Please enter a number between 1 and {len(options)}")
        except (ValueError, KeyboardInterrupt):
            print("\n❌ Invalid input. Please enter a number.")
        except EOFError:
            print("\n❌ No input received.")
            return None


def get_yes_no(prompt):
    """Get yes/no response from user."""
    while True:
        try:
            response = input(f"\n{prompt} (y/n): ").strip().lower()
            if response in ['y', 'yes']:
                return True
            elif response in ['n', 'no']:
                return False
            else:
                print("❌ Please enter 'y' or 'n'")
        except (KeyboardInterrupt, EOFError):
            print("\n❌ Operation cancelled.")
            return False


def main():
    """Main function to generate sample data with interactive options."""
    print("=" * 70)
    print("Sample Data Generator for Customer Analytics Platform")
    print("Microsoft Fabric SQL Database + Fabric CosmosDB NoSQL")
    print("=" * 70)
    
    # =========================================================================
    # INTERACTIVE MODE SELECTION
    # =========================================================================
    print("\n📋 SELECT TABLES/CONTAINERS TO LOAD")
    print("-" * 70)
    
    tables_options = [
        "Load ALL tables and containers",
        "Select specific SQL tables only",
        "Select specific CosmosDB containers only",
        "Custom selection (both SQL and CosmosDB)"
    ]
    
    load_mode = get_user_choice("What would you like to load?", tables_options)
    if load_mode is None:
        return
    
    # Determine what to load
    load_sql_customers = False
    load_sql_products = False
    load_sql_orders = False
    load_sql_order_items = False
    load_cosmos_products = False
    load_cosmos_reviews = False
    load_cosmos_sessions = False
    
    if load_mode == 0:  # Load all
        load_sql_customers = True
        load_sql_products = True
        load_sql_orders = True
        load_sql_order_items = True
        load_cosmos_products = True
        load_cosmos_reviews = True
        load_cosmos_sessions = True
    elif load_mode == 1:  # SQL only
        print("\n📊 SQL Tables:")
        load_sql_customers = get_yes_no("  Load Customers?")
        load_sql_products = get_yes_no("  Load Products?")
        load_sql_orders = get_yes_no("  Load Orders?")
        load_sql_order_items = get_yes_no("  Load Order Items?")
    elif load_mode == 2:  # CosmosDB only
        print("\n📊 CosmosDB Containers:")
        load_cosmos_products = get_yes_no("  Load Products?")
        load_cosmos_reviews = get_yes_no("  Load Reviews?")
        load_cosmos_sessions = get_yes_no("  Load Sessions?")
    elif load_mode == 3:  # Custom
        print("\n📊 SQL Tables:")
        load_sql_customers = get_yes_no("  Load Customers?")
        load_sql_products = get_yes_no("  Load Products?")
        load_sql_orders = get_yes_no("  Load Orders?")
        load_sql_order_items = get_yes_no("  Load Order Items?")
        print("\n📊 CosmosDB Containers:")
        load_cosmos_products = get_yes_no("  Load Products?")
        load_cosmos_reviews = get_yes_no("  Load Reviews?")
        load_cosmos_sessions = get_yes_no("  Load Sessions?")
    
    # =========================================================================
    # TRUNCATE SELECTION
    # =========================================================================
    print("\n🗑️ TRUNCATE TABLES/CONTAINERS?")
    print("-" * 70)
    truncate_first = get_yes_no("Clear existing data before loading?")
    
    # =========================================================================
    # INSERT MODE SELECTION
    # =========================================================================
    print("\n⚙️ SELECT INSERT MODE")
    print("-" * 70)
    
    insert_modes = [
        "Row by row insert (slower, better error handling)",
        "Bulk insert (faster, for large datasets)"
    ]
    
    insert_mode = get_user_choice("How would you like to insert data?", insert_modes)
    if insert_mode is None:
        return
    
    bulk_insert = insert_mode == 1
    
    # =========================================================================
    # RECORD COUNT CONFIGURATION
    # =========================================================================
    print("\n🔢 CONFIGURE RECORD COUNTS")
    print("-" * 70)
    
    # Initialize all counts
    num_customers = 100
    num_sql_products = 20
    num_orders = 200
    num_cosmos_products = 50
    num_reviews = 100
    num_sessions = 50
    
    # Check if any SQL tables are selected
    if load_sql_customers or load_sql_products or load_sql_orders or load_sql_order_items:
        use_defaults = get_yes_no("Use default record counts for SQL tables? (Customers:100, Products:20, Orders:200)")
        
        if not use_defaults:
            print("\n📊 Enter record counts for SQL tables:")
            try:
                if load_sql_customers:
                    num_customers = int(input("  Number of Customers: ").strip() or "100")
                if load_sql_products:
                    num_sql_products = int(input("  Number of Products: ").strip() or "20")
                if load_sql_orders:
                    num_orders = int(input("  Number of Orders: ").strip() or "200")
                if load_sql_order_items:
                    print("  Order Items: auto-generated (1-5 items per order)")
            except (ValueError, KeyboardInterrupt, EOFError):
                print("\n❌ Invalid input. Using defaults for SQL tables.")
                num_customers = 100
                num_sql_products = 20
                num_orders = 200
    
    # Check if any CosmosDB containers are selected
    if load_cosmos_products or load_cosmos_reviews or load_cosmos_sessions:
        use_defaults = get_yes_no("Use default record counts for CosmosDB containers? (Products:50, Reviews:100, Sessions:50)")
        
        if not use_defaults:
            print("\n📊 Enter record counts for CosmosDB containers:")
            try:
                if load_cosmos_products:
                    num_cosmos_products = int(input("  Number of Products: ").strip() or "50")
                if load_cosmos_reviews:
                    num_reviews = int(input("  Number of Reviews: ").strip() or "100")
                if load_cosmos_sessions:
                    num_sessions = int(input("  Number of Sessions: ").strip() or "50")
            except (ValueError, KeyboardInterrupt, EOFError):
                print("\n❌ Invalid input. Using defaults for CosmosDB containers.")
                num_cosmos_products = 50
                num_reviews = 100
                num_sessions = 50
    
    # =========================================================================
    # CONFIRMATION
    # =========================================================================
    print("\n" + "=" * 70)
    print("📋 CONFIGURATION SUMMARY")
    print("=" * 70)
    print(f"\nTruncate First: {'Yes' if truncate_first else 'No'}")
    print(f"Insert Mode: {'Bulk Insert' if bulk_insert else 'Row by Row'}")
    print("\nTables/Containers to load:")
    if load_sql_customers:
        print(f"  ✓ SQL Customers ({num_customers} records)")
    if load_sql_products:
        print(f"  ✓ SQL Products ({num_sql_products} records)")
    if load_sql_orders:
        print(f"  ✓ SQL Orders ({num_orders} records)")
    if load_sql_order_items:
        print(f"  ✓ SQL Order Items (auto-generated for orders)")
    if load_cosmos_products:
        print(f"  ✓ CosmosDB Products ({num_cosmos_products} records)")
    if load_cosmos_reviews:
        print(f"  ✓ CosmosDB Reviews ({num_reviews} records)")
    if load_cosmos_sessions:
        print(f"  ✓ CosmosDB Sessions ({num_sessions} records)")
    
    if not get_yes_no("\nProceed with data generation?"):
        print("\n❌ Operation cancelled.")
        return
    
    # =========================================================================
    # INITIALIZE CONNECTIONS
    # =========================================================================
    print("\n" + "=" * 70)
    print("📡 INITIALIZING DATABASE CONNECTIONS")
    print("=" * 70)
    
    sql_conn = None
    cosmos_conn = None
    embedding_service = None
    
    try:
        if load_sql_customers or load_sql_products or load_sql_orders or load_sql_order_items:
            driver = os.getenv("FABRIC_SQL_DRIVER", "ODBC Driver 18 for SQL Server")
            sql_conn = FabricSQLConnector(
                endpoint=os.getenv("FABRIC_SQL_ENDPOINT"),
                database=os.getenv("FABRIC_SQL_DATABASE"),
                driver=f"{{{driver}}}"
            )
            print("✓ Fabric SQL Database connected")
        
        if load_cosmos_products or load_cosmos_reviews or load_cosmos_sessions:
            cosmos_conn = FabricCosmosDBConnector(
                endpoint=os.getenv("FABRIC_COSMOSDB_ENDPOINT"),
                database_name=os.getenv("FABRIC_COSMOSDB_DATABASE", "IntelliCAPDB"),
                container_name=os.getenv("FABRIC_COSMOSDB_SESSIONS_CONTAINER", "Sessions"),
                products_container_name=os.getenv("FABRIC_COSMOSDB_PRODUCTS_CONTAINER", "Products"),
                reviews_container_name=os.getenv("FABRIC_COSMOSDB_REVIEWS_CONTAINER", "Reviews")
            )
            cosmos_conn.initialize()
            print("✓ Fabric CosmosDB NoSQL connected")
        
        if load_cosmos_products or load_cosmos_reviews:
            embedding_service = get_embedding_service(use_azure=True)
            print("✓ Embedding service initialized")
        
    except Exception as e:
        print(f"❌ Error initializing connections: {e}")
        import traceback
        traceback.print_exc()
        return
    
    # =========================================================================
    # TRUNCATE TABLES IF REQUESTED
    # =========================================================================
    if truncate_first:
        print("\n" + "=" * 70)
        print("🗑️  TRUNCATING TABLES/CONTAINERS")
        print("=" * 70)
        
        if load_sql_customers and sql_conn:
            truncate_sql_table(sql_conn, "ca.Customers")
        if load_sql_products and sql_conn:
            truncate_sql_table(sql_conn, "ca.Products")
        if load_sql_orders and sql_conn:
            truncate_sql_table(sql_conn, "ca.Orders")
        if load_sql_order_items and sql_conn:
            truncate_sql_table(sql_conn, "ca.OrderItems")
        if load_cosmos_products and cosmos_conn:
            truncate_cosmos_container(cosmos_conn, "Products")
        if load_cosmos_reviews and cosmos_conn:
            truncate_cosmos_container(cosmos_conn, "Reviews")
        if load_cosmos_sessions and cosmos_conn:
            truncate_cosmos_container(cosmos_conn, "Sessions")
    
    # =========================================================================
    # GENERATE DATA
    # =========================================================================
    print("\n" + "=" * 70)
    print("📊 GENERATING SAMPLE DATA")
    print("=" * 70)
    
    try:
        if load_sql_customers and sql_conn:
            print(f"\nGenerating Customers...")
            generate_customer_data(sql_conn, num_customers=num_customers, bulk_insert=bulk_insert)
        
        if load_sql_products and sql_conn:
            print(f"\nGenerating SQL Products...")
            generate_sql_product_data(sql_conn, num_products=num_sql_products, bulk_insert=bulk_insert)
        
        if load_sql_orders and sql_conn:
            print(f"\nGenerating Orders...")
            generate_order_data(sql_conn, num_orders=num_orders, bulk_insert=bulk_insert)
        
        if load_sql_order_items and sql_conn:
            print(f"\nGenerating Order Items...")
            generate_order_items_data(sql_conn, bulk_insert=bulk_insert)
        
        if load_cosmos_products and cosmos_conn and embedding_service:
            print(f"\nGenerating CosmosDB Products...")
            generate_product_data(cosmos_conn, embedding_service, num_products=num_cosmos_products)
        
        if load_cosmos_reviews and cosmos_conn and embedding_service:
            print(f"\nGenerating Reviews...")
            generate_review_data(cosmos_conn, embedding_service, num_reviews=num_reviews)
        
        if load_cosmos_sessions and cosmos_conn:
            print(f"\nGenerating Sessions...")
            generate_session_data(cosmos_conn, num_sessions=num_sessions)
        
    except Exception as e:
        print(f"\n❌ Error generating data: {e}")
        import traceback
        traceback.print_exc()
        return
    
    # =========================================================================
    # SUMMARY
    # =========================================================================
    print("\n" + "=" * 70)
    print("✅ SAMPLE DATA GENERATION COMPLETED!")
    print("=" * 70)
    print("\nGenerated:")
    if load_sql_customers:
        print(f"  ✓ SQL Customers: {num_customers} records")
    if load_sql_products:
        print(f"  ✓ SQL Products: {num_sql_products} records")
    if load_sql_orders:
        print(f"  ✓ SQL Orders: {num_orders} records")
    if load_sql_order_items:
        print(f"  ✓ SQL Order Items: auto-generated")
    if load_cosmos_products:
        print(f"  ✓ CosmosDB Products: {num_cosmos_products} records")
    if load_cosmos_reviews:
        print(f"  ✓ CosmosDB Reviews: {num_reviews} records")
    if load_cosmos_sessions:
        print(f"  ✓ CosmosDB Sessions: {num_sessions} records")
    
    print("\n💡 Next Steps:")
    print("  • Run: python scripts/database_summary.py  (to verify data)")
    print("  • Run: streamlit run Home.py  (to launch the application)")
    print("=" * 70)


if __name__ == "__main__":
    main()
