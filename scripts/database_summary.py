"""
Quick script to summarize all data in both Fabric databases
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from dotenv import load_dotenv
load_dotenv()

from src.database.fabric_sql import FabricSQLConnector
from src.database.fabric_cosmos import FabricCosmosDBConnector

def main():
    print("=" * 70)
    print("DATABASE SUMMARY - Microsoft Fabric")
    print("=" * 70)
    
    # =========================================================================
    # FABRIC SQL DATABASE
    # =========================================================================
    print("\n📊 FABRIC SQL DATABASE")
    print("-" * 70)
    
    # Initialize SQL connector
    try:
        sql_conn = FabricSQLConnector(
            endpoint=os.getenv("FABRIC_SQL_ENDPOINT"),
            database=os.getenv("FABRIC_SQL_DATABASE")
        )
        if not sql_conn.test_connection():
            raise Exception("Connection test failed")
        print("✓ Connected\n")
    except Exception as e:
        print(f"❌ Error connecting to SQL: {e}\n")
        sql_conn = None
    
    # Count records in each SQL table
    if sql_conn:
        tables = {
            'Products': 'ca.Products',
            'Customers': 'ca.Customers',
            'Orders': 'ca.Orders',
            'OrderItems': 'ca.OrderItems'
        }
        
        sql_total = 0
        print("Table Record Counts:")
        for table_name, table_path in tables.items():
            try:
                query = f"SELECT COUNT(*) as count FROM {table_path}"
                df = sql_conn.execute_query(query)
                count = df['count'].iloc[0]
                sql_total += count
                print(f"  {table_name:20} : {count:>6} records")
            except Exception as e:
                print(f"  {table_name:20} : ERROR - {e}")
        
        print(f"\n{'Total SQL Records':20} : {sql_total:>6}")
    
    # =========================================================================
    # FABRIC COSMOSDB NOSQL
    # =========================================================================
    print("\n" + "=" * 70)
    print("📊 FABRIC COSMOSDB NOSQL")
    print("-" * 70)
    
    # Initialize CosmosDB connector
    try:
        cosmos_conn = FabricCosmosDBConnector(
            endpoint=os.getenv("FABRIC_COSMOSDB_ENDPOINT"),
            database_name=os.getenv("FABRIC_COSMOSDB_DATABASE", "IntelliCAPDB"),
            container_name=os.getenv("FABRIC_COSMOSDB_SESSIONS_CONTAINER", "Sessions"),
            products_container_name=os.getenv("FABRIC_COSMOSDB_PRODUCTS_CONTAINER", "Products"),
            reviews_container_name=os.getenv("FABRIC_COSMOSDB_REVIEWS_CONTAINER", "Reviews")
        )
        cosmos_conn.initialize()
        print("✓ Connected\n")
    except Exception as e:
        print(f"❌ Error connecting to CosmosDB: {e}\n")
        cosmos_conn = None
    
    # Count records in each CosmosDB container
    cosmos_total = 0
    if cosmos_conn:
        print("Container Record Counts:")
        
        # Products
        try:
            products = cosmos_conn.get_all_products(limit=10000)
            product_count = len(products)
            cosmos_total += product_count
            print(f"  {'Products':20} : {product_count:>6} records")
            
            # Check for embeddings
            if products:
                has_embeddings = 'embedding' in products[0] or 'descriptionEmbedding' in products[0]
                print(f"    └─ Embeddings: {'✓ Yes' if has_embeddings else '✗ No'}")
        except Exception as e:
            print(f"  {'Products':20} : ERROR - {e}")
        
        # Reviews
        try:
            query = "SELECT * FROM c WHERE c.type = 'review'"
            reviews = list(cosmos_conn.reviews_container.query_items(
                query=query,
                enable_cross_partition_query=True
            ))
            review_count = len(reviews)
            cosmos_total += review_count
            print(f"  {'Reviews':20} : {review_count:>6} records")
            
            # Check for embeddings
            if reviews:
                has_embeddings = 'embedding' in reviews[0]
                print(f"    └─ Embeddings: {'✓ Yes' if has_embeddings else '✗ No'}")
        except Exception as e:
            print(f"  {'Reviews':20} : ERROR - {e}")
        
        # Sessions
        try:
            query = "SELECT VALUE COUNT(1) FROM c"
            result = list(cosmos_conn.container.query_items(
                query=query,
                enable_cross_partition_query=True
            ))
            session_count = result[0] if result else 0
            cosmos_total += session_count
            print(f"  {'Sessions':20} : {session_count:>6} records")
        except Exception as e:
            print(f"  {'Sessions':20} : ERROR - {e}")
        
        print(f"\n{'Total CosmosDB Records':20} : {cosmos_total:>6}")
    
    # =========================================================================
    # SUMMARY
    # =========================================================================
    print("\n" + "=" * 70)
    print("✅ SUMMARY")
    print("=" * 70)
    
    if sql_conn and cosmos_conn:
        print(f"\nFabric SQL Database    : {sql_total:>6} records")
        print(f"Fabric CosmosDB NoSQL  : {cosmos_total:>6} records")
        print(f"{'─' * 40}")
        print(f"Total Across Fabric    : {sql_total + cosmos_total:>6} records")
        print("\n✓ Both databases are operational!")
        print("✓ Ready for testing and production use!")
    else:
        print("\n⚠️  Some database connections failed.")
        print("   Run setup_environment.py to initialize your environment.")
    
    print("=" * 70)


if __name__ == "__main__":
    main()
