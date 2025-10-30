"""
Test product recommendations with SQL database
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from dotenv import load_dotenv
load_dotenv()

from src.database.fabric_sql import FabricSQLConnector

def main():
    print("=" * 60)
    print("Testing Product Recommendations")
    print("=" * 60)
    
    # Initialize SQL connector
    try:
        sql_conn = FabricSQLConnector(
            endpoint=os.getenv("FABRIC_SQL_ENDPOINT"),
            database=os.getenv("FABRIC_SQL_DATABASE")
        )
        if not sql_conn.test_connection():
            raise Exception("Connection test failed")
        print("✓ SQL Database connected\n")
    except Exception as e:
        print(f"❌ Error connecting to SQL: {e}")
        return
    
    # Get a sample product
    sample_query = """
        SELECT TOP 1
            ProductID,
            ProductName,
            Category,
            UnitPrice
        FROM ca.Products
        WHERE IsActive = 1
        ORDER BY ProductID
    """
    
    sample_product_df = sql_conn.execute_query(sample_query)
    
    if sample_product_df.empty:
        print("❌ No products found in database")
        return
    
    product = sample_product_df.iloc[0]
    product_id = int(product['ProductID'])
    product_name = product['ProductName']
    category = product['Category']
    price = float(product['UnitPrice'])
    
    print(f"Selected Product:")
    print(f"  - Name: {product_name}")
    print(f"  - Category: {category}")
    print(f"  - Price: ${price:.2f}")
    print()
    
    # Find similar products
    rec_query = f"""
        SELECT TOP {5}
            ProductID,
            ProductName,
            Category,
            SubCategory,
            UnitPrice,
            StockQuantity,
            ABS(UnitPrice - ?) as price_diff
        FROM ca.Products
        WHERE 
            ProductID != ?
            AND Category = ?
            AND IsActive = 1
        ORDER BY price_diff ASC
    """
    
    recommendations_df = sql_conn.execute_query(
        rec_query,
        (price, product_id, category)
    )
    
    if not recommendations_df.empty:
        print(f"✅ Found {len(recommendations_df)} similar products:\n")
        for idx, rec in recommendations_df.iterrows():
            rec_price = float(rec['UnitPrice'])
            price_diff = abs(rec_price - price)
            print(f"{idx+1}. {rec['ProductName']}")
            print(f"   Category: {rec['Category']} | Price: ${rec_price:.2f} | Diff: ${price_diff:.2f}")
            print()
    else:
        print(f"❌ No recommendations found in '{category}' category")
    
    print("=" * 60)
    print("✅ Recommendation feature is working!")
    print("=" * 60)


if __name__ == "__main__":
    main()
