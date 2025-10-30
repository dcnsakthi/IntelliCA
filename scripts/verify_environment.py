"""
Verify IntelliCA Microsoft Fabric Environment
==============================================
This script checks the status of all data in your Fabric environment:
- Fabric SQL Database: Customers, Products, Orders, OrderDetails
- Fabric CosmosDB NoSQL: Products, Reviews, Sessions

Run this script to verify your environment is properly set up.
"""
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from dotenv import load_dotenv
load_dotenv()

from src.database.fabric_sql import FabricSQLConnector
from src.database.fabric_cosmos import FabricCosmosDBConnector


def check_sql_data(sql_conn):
    """Check data in Fabric SQL Database."""
    print("\n" + "="*70)
    print(" Fabric SQL Database Status")
    print("="*70)
    
    tables = [
        ("ca.Customers", "CustomerID", "FirstName, LastName, Email"),
        ("ca.Products", "ProductID", "ProductName, Category, UnitPrice"),
        ("ca.Orders", "OrderID", "OrderDate, TotalAmount, OrderStatus"),
        ("ca.OrderDetails", "OrderDetailID", "OrderID, ProductID, Quantity")
    ]
    
    total_records = 0
    for table, id_col, sample_cols in tables:
        try:
            # Count records
            count_query = f"SELECT COUNT(*) as count FROM {table}"
            count_df = sql_conn.execute_query(count_query)
            count = count_df.iloc[0]['count'] if not count_df.empty else 0
            total_records += count
            
            # Get sample record
            sample_query = f"SELECT TOP 1 {sample_cols} FROM {table}"
            sample_df = sql_conn.execute_query(sample_query)
            
            print(f"\n{table}:")
            print(f"  Records: {count}")
            if not sample_df.empty and count > 0:
                print(f"  Sample: {sample_df.iloc[0].to_dict()}")
            
        except Exception as e:
            print(f"\n{table}:")
            print(f"  ✗ Error: {e}")
    
    print(f"\nTotal SQL Records: {total_records}")
    return total_records


def check_cosmos_data(cosmos_conn):
    """Check data in Fabric CosmosDB."""
    print("\n" + "="*70)
    print(" Fabric CosmosDB NoSQL Status")
    print("="*70)
    
    total_records = 0
    
    # Check Products
    try:
        products = cosmos_conn.get_all_products(limit=1000)
        print(f"\nProducts Container:")
        print(f"  Records: {len(products)}")
        if products:
            sample = products[0]
            print(f"  Sample: {sample.get('name', 'N/A')} - {sample.get('category', 'N/A')}")
            print(f"  Has embeddings: {'embedding' in sample}")
        total_records += len(products)
    except Exception as e:
        print(f"\nProducts Container:")
        print(f"  ✗ Error: {e}")
    
    # Check Reviews
    try:
        query = "SELECT * FROM c WHERE c.type = 'review'"
        reviews = list(cosmos_conn.reviews_container.query_items(
            query=query,
            enable_cross_partition_query=True
        ))
        print(f"\nReviews Container:")
        print(f"  Records: {len(reviews)}")
        if reviews:
            sample = reviews[0]
            print(f"  Sample: Product {sample.get('productId', 'N/A')} - Rating: {sample.get('rating', 'N/A')}")
            print(f"  Has embeddings: {'embedding' in sample}")
        total_records += len(reviews)
    except Exception as e:
        print(f"\nReviews Container:")
        print(f"  ✗ Error: {e}")
    
    # Check Sessions
    try:
        query = "SELECT * FROM c WHERE c.type = 'session'"
        sessions = list(cosmos_conn.container.query_items(
            query=query,
            enable_cross_partition_query=True,
            max_item_count=10
        ))
        print(f"\nSessions Container:")
        print(f"  Records: {len(sessions)} (showing first 10)")
        if sessions:
            print(f"  Sample: Session {sessions[0].get('id', 'N/A')}")
        total_records += len(sessions)
    except Exception as e:
        print(f"\nSessions Container:")
        print(f"  ✗ Error: {e}")
    
    print(f"\nTotal CosmosDB Records: {total_records}")
    return total_records


def main():
    """Main verification function."""
    print("\n" + "="*70)
    print(" IntelliCA Environment Verification")
    print("="*70)
    
    try:
        # Connect to SQL
        print("\nConnecting to Fabric SQL Database...")
        sql_conn = FabricSQLConnector(
            endpoint=os.getenv("FABRIC_SQL_ENDPOINT"),
            database=os.getenv("FABRIC_SQL_DATABASE"),
            driver=f"{{{os.getenv('FABRIC_SQL_DRIVER', 'ODBC Driver 18 for SQL Server')}}}"
        )
        print("✓ Connected to Fabric SQL Database")
        
        # Connect to CosmosDB
        print("Connecting to Fabric CosmosDB...")
        cosmos_conn = FabricCosmosDBConnector(
            endpoint=os.getenv("FABRIC_COSMOSDB_ENDPOINT"),
            database_name=os.getenv("FABRIC_COSMOSDB_DATABASE", "IntelliCAPDB"),
            container_name=os.getenv("FABRIC_COSMOSDB_SESSIONS_CONTAINER", "Sessions"),
            products_container_name=os.getenv("FABRIC_COSMOSDB_PRODUCTS_CONTAINER", "Products"),
            reviews_container_name=os.getenv("FABRIC_COSMOSDB_REVIEWS_CONTAINER", "Reviews")
        )
        cosmos_conn.initialize()
        print("✓ Connected to Fabric CosmosDB")
        
        # Check data
        sql_records = check_sql_data(sql_conn)
        cosmos_records = check_cosmos_data(cosmos_conn)
        
        # Summary
        print("\n" + "="*70)
        print(" Verification Summary")
        print("="*70)
        print(f"\nFabric SQL Database: {sql_records} total records")
        print(f"Fabric CosmosDB: {cosmos_records} total records")
        
        if sql_records > 0 and cosmos_records > 0:
            print("\n✓ Environment is properly configured!")
        else:
            print("\n⚠️  Some data is missing. Run setup_environment.py to initialize.")
        
        print("="*70 + "\n")
        
    except Exception as e:
        print(f"\n❌ Verification failed: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
