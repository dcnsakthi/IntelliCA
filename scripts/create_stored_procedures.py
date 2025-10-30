"""
Create or update stored procedures in Fabric SQL Database
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from dotenv import load_dotenv
load_dotenv()

from src.database.fabric_sql import FabricSQLConnector

def create_stored_procedures():
    """Create stored procedures in Fabric SQL Database."""
    print("=" * 60)
    print("Creating Stored Procedures")
    print("=" * 60)
    
    try:
        sql_conn = FabricSQLConnector(
            endpoint=os.getenv("FABRIC_SQL_ENDPOINT"),
            database=os.getenv("FABRIC_SQL_DATABASE")
        )
        
        if not sql_conn.test_connection():
            raise Exception("Connection test failed")
        
        print("✓ Fabric SQL Database connected\n")
        
        # Create sp_UpdateCustomerLifetimeValue
        print("Creating ca.sp_UpdateCustomerLifetimeValue...")
        sp1 = """
        IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'ca.sp_UpdateCustomerLifetimeValue') AND type in (N'P', N'PC'))
            DROP PROCEDURE ca.sp_UpdateCustomerLifetimeValue;
        """
        sql_conn.execute_non_query(sp1)
        
        sp1_create = """
        CREATE PROCEDURE ca.sp_UpdateCustomerLifetimeValue
            @CustomerID INT
        AS
        BEGIN
            UPDATE ca.Customers
            SET TotalLifetimeValue = (
                SELECT ISNULL(SUM(TotalAmount), 0)
                FROM ca.Orders
                WHERE CustomerID = @CustomerID
                AND OrderStatus NOT IN ('Cancelled')
            ),
            LastPurchaseDate = (
                SELECT MAX(OrderDate)
                FROM ca.Orders
                WHERE CustomerID = @CustomerID
            )
            WHERE CustomerID = @CustomerID;
        END;
        """
        sql_conn.execute_non_query(sp1_create)
        print("✓ ca.sp_UpdateCustomerLifetimeValue created\n")
        
        # Create sp_UpdateCustomerSegmentation
        print("Creating ca.sp_UpdateCustomerSegmentation...")
        sp2 = """
        IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'ca.sp_UpdateCustomerSegmentation') AND type in (N'P', N'PC'))
            DROP PROCEDURE ca.sp_UpdateCustomerSegmentation;
        """
        sql_conn.execute_non_query(sp2)
        
        sp2_create = """
        CREATE PROCEDURE ca.sp_UpdateCustomerSegmentation
        AS
        BEGIN
            UPDATE ca.Customers
            SET CustomerSegment = 
                CASE 
                    WHEN TotalLifetimeValue >= 10000 THEN 'Premium'
                    WHEN TotalLifetimeValue >= 5000 THEN 'Gold'
                    WHEN TotalLifetimeValue >= 1000 THEN 'Silver'
                    ELSE 'Bronze'
                END
            WHERE IsActive = 1;
        END;
        """
        sql_conn.execute_non_query(sp2_create)
        print("✓ ca.sp_UpdateCustomerSegmentation created\n")
        
        print("=" * 60)
        print("✅ All stored procedures created successfully!")
        print("=" * 60)
        
    except Exception as e:
        print(f"❌ Error creating stored procedures: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(create_stored_procedures())
