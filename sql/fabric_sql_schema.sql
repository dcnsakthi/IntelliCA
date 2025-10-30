-- Azure SQL Database Schema for Customer Analytics Platform
-- Transactional data: Customers, Orders, Products, Transactions
CREATE SCHEMA ca;

-- Create Customers table
CREATE TABLE ca.Customers (
    CustomerID INT PRIMARY KEY IDENTITY(1,1),
    FirstName NVARCHAR(100) NOT NULL,
    LastName NVARCHAR(100) NOT NULL,
    Email NVARCHAR(255) UNIQUE NOT NULL,
    Phone NVARCHAR(20),
    DateOfBirth DATE,
    RegistrationDate DATETIME DEFAULT GETDATE(),
    Country NVARCHAR(100),
    City NVARCHAR(100),
    CustomerSegment NVARCHAR(50), -- e.g., 'Premium', 'Standard', 'Basic'
    IsActive BIT DEFAULT 1,
    LastPurchaseDate DATETIME,
    TotalLifetimeValue DECIMAL(18,2) DEFAULT 0,
    ChurnRiskScore DECIMAL(5,2) DEFAULT 0 -- 0-100 scale
);

-- Create Products table (basic info, detailed catalog in PostgreSQL)
CREATE TABLE ca.Products (
    ProductID INT PRIMARY KEY IDENTITY(1,1),
    ProductName NVARCHAR(255) NOT NULL,
    SKU NVARCHAR(50) UNIQUE NOT NULL,
    Category NVARCHAR(100),
    SubCategory NVARCHAR(100),
    UnitPrice DECIMAL(18,2) NOT NULL,
    StockQuantity INT DEFAULT 0,
    IsActive BIT DEFAULT 1,
    CreatedDate DATETIME DEFAULT GETDATE()
);

-- Create Orders table
CREATE TABLE ca.Orders (
    OrderID INT PRIMARY KEY IDENTITY(1,1),
    CustomerID INT NOT NULL,
    OrderDate DATETIME DEFAULT GETDATE(),
    TotalAmount DECIMAL(18,2) NOT NULL,
    OrderStatus NVARCHAR(50) NOT NULL, -- 'Pending', 'Confirmed', 'Shipped', 'Delivered', 'Cancelled'
    ShippingAddress NVARCHAR(500),
    PaymentMethod NVARCHAR(50),
    PaymentStatus NVARCHAR(50),
    FOREIGN KEY (CustomerID) REFERENCES ca.Customers(CustomerID)
);

-- Create OrderItems table
CREATE TABLE ca.OrderItems (
    OrderItemID INT PRIMARY KEY IDENTITY(1,1),
    OrderID INT NOT NULL,
    ProductID INT NOT NULL,
    Quantity INT NOT NULL,
    UnitPrice DECIMAL(18,2) NOT NULL,
    Discount DECIMAL(5,2) DEFAULT 0,
    LineTotal AS (Quantity * UnitPrice * (1 - Discount/100)) PERSISTED,
    FOREIGN KEY (OrderID) REFERENCES ca.Orders(OrderID),
    FOREIGN KEY (ProductID) REFERENCES ca.Products(ProductID)
);

-- Create CustomerInteractions table for tracking customer touchpoints
CREATE TABLE ca.CustomerInteractions (
    InteractionID INT PRIMARY KEY IDENTITY(1,1),
    CustomerID INT NOT NULL,
    InteractionType NVARCHAR(50) NOT NULL, -- 'Support', 'Marketing', 'Sales', 'Feedback'
    InteractionDate DATETIME DEFAULT GETDATE(),
    Channel NVARCHAR(50), -- 'Email', 'Phone', 'Chat', 'Social'
    Subject NVARCHAR(255),
    Notes NVARCHAR(MAX),
    SentimentScore DECIMAL(5,2), -- -1 to 1
    FOREIGN KEY (CustomerID) REFERENCES ca.Customers(CustomerID)
);

-- Create CustomerSegmentHistory for tracking segment changes
CREATE TABLE ca.CustomerSegmentHistory (
    HistoryID INT PRIMARY KEY IDENTITY(1,1),
    CustomerID INT NOT NULL,
    PreviousSegment NVARCHAR(50),
    NewSegment NVARCHAR(50) NOT NULL,
    ChangeDate DATETIME DEFAULT GETDATE(),
    Reason NVARCHAR(255),
    FOREIGN KEY (CustomerID) REFERENCES ca.Customers(CustomerID)
);

-- Create indexes for performance
CREATE INDEX IX_Customers_Email ON ca.Customers(Email);
CREATE INDEX IX_Customers_Segment ON ca.Customers(CustomerSegment);
CREATE INDEX IX_Orders_CustomerID ON ca.Orders(CustomerID);
CREATE INDEX IX_Orders_OrderDate ON ca.Orders(OrderDate);
CREATE INDEX IX_OrderItems_OrderID ON ca.OrderItems(OrderID);
CREATE INDEX IX_OrderItems_ProductID ON ca.OrderItems(ProductID);
CREATE INDEX IX_CustomerInteractions_CustomerID ON ca.CustomerInteractions(CustomerID);
CREATE INDEX IX_CustomerInteractions_Date ON ca.CustomerInteractions(InteractionDate);

-- Create views for analytics
GO

-- Customer 360 View
CREATE VIEW ca.vw_Customer360 AS
SELECT 
    c.CustomerID,
    c.FirstName,
    c.LastName,
    c.Email,
    c.CustomerSegment,
    c.RegistrationDate,
    c.TotalLifetimeValue,
    c.ChurnRiskScore,
    COUNT(DISTINCT o.OrderID) as TotalOrders,
    SUM(o.TotalAmount) as TotalSpent,
    MAX(o.OrderDate) as LastOrderDate,
    AVG(CAST(o.TotalAmount as FLOAT)) as AvgOrderValue,
    DATEDIFF(DAY, MAX(o.OrderDate), GETDATE()) as DaysSinceLastOrder
FROM ca.Customers c
LEFT JOIN ca.Orders o ON c.CustomerID = o.CustomerID
GROUP BY 
    c.CustomerID, c.FirstName, c.LastName, c.Email, 
    c.CustomerSegment, c.RegistrationDate, c.TotalLifetimeValue, 
    c.ChurnRiskScore;

GO

-- Product Performance View
CREATE VIEW ca.vw_ProductPerformance AS
SELECT 
    p.ProductID,
    p.ProductName,
    p.Category,
    p.SubCategory,
    p.UnitPrice,
    COUNT(DISTINCT oi.OrderID) as TotalOrders,
    SUM(oi.Quantity) as TotalQuantitySold,
    SUM(oi.LineTotal) as TotalRevenue,
    AVG(oi.LineTotal) as AvgRevenuePerOrder
FROM ca.Products p
LEFT JOIN ca.OrderItems oi ON p.ProductID = oi.ProductID
GROUP BY 
    p.ProductID, p.ProductName, p.Category, 
    p.SubCategory, p.UnitPrice;

GO

-- Recent Orders View
CREATE VIEW ca.vw_RecentOrders AS
SELECT TOP 1000
    o.OrderID,
    o.OrderDate,
    c.CustomerID,
    c.FirstName + ' ' + c.LastName as CustomerName,
    c.Email,
    o.TotalAmount,
    o.OrderStatus,
    COUNT(oi.OrderItemID) as ItemCount
FROM ca.Orders o
INNER JOIN ca.Customers c ON o.CustomerID = c.CustomerID
LEFT JOIN ca.OrderItems oi ON o.OrderID = oi.OrderID
GROUP BY 
    o.OrderID, o.OrderDate, c.CustomerID, 
    c.FirstName, c.LastName, c.Email, 
    o.TotalAmount, o.OrderStatus
ORDER BY o.OrderDate DESC;

GO

-- Stored procedure for updating customer lifetime value
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

GO

-- Stored procedure for customer segmentation
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
