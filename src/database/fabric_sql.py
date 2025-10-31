"""Microsoft Fabric SQL Database connector for customer and order data."""
import mssql_python
import pandas as pd
from typing import Optional, List, Dict, Any
import logging
from contextlib import contextmanager
from azure.identity import DefaultAzureCredential
import struct

logger = logging.getLogger(__name__)


class FabricSQLConnector:
    """Connector for Microsoft Fabric SQL Database operations using Entra ID authentication."""
    
    def __init__(
        self,
        endpoint: str,
        database: str,
        driver: str = "{ODBC Driver 18 for SQL Server}"  # Keep for compatibility, not used
    ):
        """
        Initialize Microsoft Fabric SQL connector with Entra ID authentication.
        
        Args:
            endpoint: Fabric SQL endpoint (e.g., 'workspace.datawarehouse.fabric.microsoft.com')
            database: Database name
            driver: Kept for compatibility, mssql-python handles driver internally
            
        Note:
            Authentication uses Azure Entra ID (DefaultAzureCredential).
            Supported authentication methods:
            - Environment variables (AZURE_TENANT_ID, AZURE_CLIENT_ID, AZURE_CLIENT_SECRET)
            - Azure CLI (az login)
            - Managed Identity (when running in Azure)
            - Visual Studio Code
            - Interactive browser
        """
        self.endpoint = endpoint
        self.database = database
        self.credential = DefaultAzureCredential()
    
    def _get_access_token(self) -> str:
        """
        Get access token for Azure SQL Database using Entra ID.
        
        Returns:
            Access token string
        """
        # Scope for Azure SQL Database
        token = self.credential.get_token("https://database.windows.net/.default")
        return token.token
        
    @contextmanager
    def get_connection(self):
        """
        Context manager for database connections using Entra ID authentication.
        
        Yields:
            mssql_python.Connection: Database connection
        """
        conn = None
        try:
            # Get access token
            token = self._get_access_token()
            
            # Build connection string
            connection_string = (
                f"DRIVER={{ODBC Driver 18 for SQL Server}};"
                f"SERVER={self.endpoint};"
                f"DATABASE={self.database};"
                f"Encrypt=yes;"
                f"TrustServerCertificate=no;"
            )
            
            # SQL_COPT_SS_ACCESS_TOKEN constant for access token authentication
            SQL_COPT_SS_ACCESS_TOKEN = 1256
            
            # Encode token for SQL Server (same format as pyodbc)
            token_bytes = token.encode("utf-16-le")
            token_struct = struct.pack(f"<I{len(token_bytes)}s", len(token_bytes), token_bytes)
            
            # Connect with access token using mssql-python
            conn = mssql_python.connect(
                connection_string,
                attrs_before={SQL_COPT_SS_ACCESS_TOKEN: token_struct},
                timeout=30
            )
            yield conn
        except Exception as e:
            logger.error(f"Database connection error: {e}")
            raise
        finally:
            if conn:
                conn.close()
    
    def execute_query(self, query: str, params: Optional[tuple] = None) -> pd.DataFrame:
        """
        Execute a SELECT query and return results as DataFrame.
        
        Args:
            query: SQL query to execute
            params: Optional query parameters
            
        Returns:
            DataFrame with query results
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)
                
                # Fetch column names
                columns = [column[0] for column in cursor.description]
                
                # Fetch all rows
                rows = cursor.fetchall()
                
                # Convert to DataFrame
                df = pd.DataFrame.from_records(rows, columns=columns)
                
            logger.info(f"Query executed successfully, returned {len(df)} rows")
            return df
        except Exception as e:
            logger.error(f"Query execution error: {e}")
            raise
    
    def execute_non_query(self, query: str, params: Optional[tuple] = None) -> int:
        """
        Execute INSERT, UPDATE, DELETE queries.
        
        Args:
            query: SQL query to execute
            params: Optional query parameters
            
        Returns:
            Number of affected rows
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(query, params or ())
                conn.commit()
                rowcount = cursor.rowcount
                cursor.close()
            logger.info(f"Non-query executed successfully, {rowcount} rows affected")
            return rowcount
        except Exception as e:
            logger.error(f"Non-query execution error: {e}")
            raise
    
    def execute_stored_procedure(
        self, 
        proc_name: str, 
        params: Optional[Dict[str, Any]] = None
    ) -> Optional[pd.DataFrame]:
        """
        Execute a stored procedure.
        
        Args:
            proc_name: Name of the stored procedure
            params: Dictionary of parameter names and values
            
        Returns:
            DataFrame if the procedure returns results, None otherwise
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                if params:
                    param_placeholders = ", ".join([f"@{k}=?" for k in params.keys()])
                    query = f"EXEC {proc_name} {param_placeholders}"
                    cursor.execute(query, list(params.values()))
                else:
                    cursor.execute(f"EXEC {proc_name}")
                
                # Check if there are results to fetch
                if cursor.description:
                    columns = [column[0] for column in cursor.description]
                    results = cursor.fetchall()
                    df = pd.DataFrame.from_records(results, columns=columns)
                    cursor.close()
                    return df
                else:
                    conn.commit()
                    cursor.close()
                    return None
                    
        except Exception as e:
            logger.error(f"Stored procedure execution error: {e}")
            raise
    
    # Customer-related queries
    
    def get_customer_by_id(self, customer_id: int) -> Optional[pd.DataFrame]:
        """Get customer details by ID."""
        query = "SELECT * FROM ca.Customers WHERE CustomerID = ?"
        return self.execute_query(query, (customer_id,))
    
    def get_customer_360_view(self, customer_id: int) -> Optional[pd.DataFrame]:
        """Get comprehensive customer view with orders and interactions."""
        query = "SELECT * FROM ca.vw_Customer360 WHERE CustomerID = ?"
        return self.execute_query(query, (customer_id,))
    
    def get_top_customers(self, limit: int = 10) -> pd.DataFrame:
        """Get top customers by lifetime value."""
        query = f"""
        SELECT TOP {limit} 
            CustomerID, FirstName, LastName, Email, 
            TotalLifetimeValue, CustomerSegment, 
            LastPurchaseDate
        FROM ca.Customers
        WHERE IsActive = 1
        ORDER BY TotalLifetimeValue DESC
        """
        return self.execute_query(query)
    
    def search_customers(self, search_term: str) -> pd.DataFrame:
        """Search customers by name or email."""
        query = """
        SELECT CustomerID, FirstName, LastName, Email, 
               CustomerSegment, TotalLifetimeValue
        FROM ca.Customers
        WHERE FirstName LIKE ? OR LastName LIKE ? OR Email LIKE ?
        """
        search_pattern = f"%{search_term}%"
        return self.execute_query(query, (search_pattern, search_pattern, search_pattern))
    
    # Order-related queries
    
    def get_customer_orders(self, customer_id: int) -> pd.DataFrame:
        """Get all orders for a customer."""
        query = """
        SELECT o.OrderID, o.OrderDate, o.TotalAmount, o.OrderStatus,
               COUNT(oi.OrderItemID) as ItemCount
        FROM ca.Orders o
        LEFT JOIN ca.OrderItems oi ON o.OrderID = oi.OrderID
        WHERE o.CustomerID = ?
        GROUP BY o.OrderID, o.OrderDate, o.TotalAmount, o.OrderStatus
        ORDER BY o.OrderDate DESC
        """
        return self.execute_query(query, (customer_id,))
    
    def get_order_details(self, order_id: int) -> pd.DataFrame:
        """Get detailed information about an order."""
        query = """
        SELECT 
            o.OrderID, o.OrderDate, o.TotalAmount, o.OrderStatus,
            oi.ProductID, p.ProductName, p.SKU,
            oi.Quantity, oi.UnitPrice, oi.Discount, oi.LineTotal
        FROM ca.Orders o
        INNER JOIN ca.OrderItems oi ON o.OrderID = oi.OrderID
        INNER JOIN ca.Products p ON oi.ProductID = p.ProductID
        WHERE o.OrderID = ?
        """
        return self.execute_query(query, (order_id,))
    
    def get_recent_orders(self, days: int = 30, limit: int = 100) -> pd.DataFrame:
        """Get recent orders within specified days."""
        query = f"""
        SELECT TOP {limit} * FROM ca.vw_RecentOrders
        WHERE OrderDate >= DATEADD(day, -{days}, GETDATE())
        ORDER BY OrderDate DESC
        """
        return self.execute_query(query)
    
    # Product-related queries
    
    def get_product_performance(self, limit: int = 20) -> pd.DataFrame:
        """Get top performing products."""
        query = f"""
        SELECT TOP {limit} * FROM vw_ProductPerformance
        ORDER BY TotalRevenue DESC
        """
        return self.execute_query(query)
    
    def get_products_by_category(self, category: str) -> pd.DataFrame:
        """Get products by category."""
        query = """
        SELECT ProductID, ProductName, SKU, Category, 
               SubCategory, UnitPrice, StockQuantity
        FROM Products
        WHERE Category = ? AND IsActive = 1
        ORDER BY ProductName
        """
        return self.execute_query(query, (category,))
    
    # Analytics queries
    
    def get_sales_by_period(
        self, 
        start_date: str, 
        end_date: str, 
        group_by: str = 'day'
    ) -> pd.DataFrame:
        """
        Get sales aggregated by period.
        
        Args:
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            group_by: 'day', 'week', 'month'
        """
        date_format = {
            'day': 'YYYY-MM-DD',
            'week': 'YYYY-WW',
            'month': 'YYYY-MM'
        }.get(group_by, 'YYYY-MM-DD')
        
        query = f"""
        SELECT 
            FORMAT(OrderDate, '{date_format}') as Period,
            COUNT(DISTINCT OrderID) as OrderCount,
            COUNT(DISTINCT CustomerID) as UniqueCustomers,
            SUM(TotalAmount) as TotalRevenue,
            AVG(TotalAmount) as AvgOrderValue
        FROM ca.Orders
        WHERE OrderDate BETWEEN ? AND ?
            AND OrderStatus NOT IN ('Cancelled')
        GROUP BY FORMAT(OrderDate, '{date_format}')
        ORDER BY Period
        """
        return self.execute_query(query, (start_date, end_date))
    
    def get_customer_segments_distribution(self) -> pd.DataFrame:
        """Get distribution of customers by segment."""
        query = """
        SELECT 
            CustomerSegment,
            COUNT(*) as CustomerCount,
            SUM(TotalLifetimeValue) as TotalValue,
            AVG(TotalLifetimeValue) as AvgValue
        FROM ca.Customers
        WHERE IsActive = 1
        GROUP BY CustomerSegment
        ORDER BY TotalValue DESC
        """
        return self.execute_query(query)
    
    def get_churn_risk_customers(self, risk_threshold: float = 70.0) -> pd.DataFrame:
        """Get customers with high churn risk."""
        query = """
        SELECT 
            CustomerID, FirstName, LastName, Email,
            CustomerSegment, TotalLifetimeValue,
            LastPurchaseDate, ChurnRiskScore
        FROM ca.Customers
        WHERE ChurnRiskScore >= ? AND IsActive = 1
        ORDER BY ChurnRiskScore DESC
        """
        return self.execute_query(query, (risk_threshold,))
    
    # Data modification methods
    
    def insert_customer(self, customer_data: Dict[str, Any]) -> int:
        """Insert a new customer."""
        query = """
        INSERT INTO ca.Customers 
        (FirstName, LastName, Email, Phone, DateOfBirth, 
         Country, City, CustomerSegment)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?);
        """
        query_id = "SELECT SCOPE_IDENTITY() as CustomerID;"
        
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, (
                customer_data['FirstName'],
                customer_data['LastName'],
                customer_data['Email'],
                customer_data.get('Phone'),
                customer_data.get('DateOfBirth'),
                customer_data.get('Country'),
                customer_data.get('City'),
                customer_data.get('CustomerSegment', 'Bronze')
            ))
            # Execute separate query to get the ID
            cursor.execute(query_id)
            result = cursor.fetchone()
            customer_id = result[0] if result else None
            conn.commit()
            cursor.close()
        return int(customer_id) if customer_id else 0
    
    def update_customer_lifetime_value(self, customer_id: int) -> None:
        """Update customer lifetime value using stored procedure."""
        self.execute_stored_procedure(
            'ca.sp_UpdateCustomerLifetimeValue',
            {'CustomerID': customer_id}
        )
    
    def update_customer_segmentation(self) -> None:
        """Update all customer segments based on lifetime value."""
        self.execute_stored_procedure('ca.sp_UpdateCustomerSegmentation')
    
    def test_connection(self) -> bool:
        """Test database connection."""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
                cursor.close()
            logger.info("Database connection test successful")
            return True
        except Exception as e:
            logger.error(f"Database connection test failed: {e}")
            return False
