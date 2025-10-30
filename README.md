# Intelligent Customer Analytics Platform

A real-world AI-powered customer analytics solution showcasing integration of **Microsoft Agent Framework**, **Streamlit**, and multiple **Microsoft Fabric** database technologies.

> **🆕 Recently Updated**: 
> - Migrated from Semantic Kernel to **Microsoft Agent Framework**
> - Migrated from PostgreSQL+pgvector to **CosmosDB NoSQL with native vector search**

## 🎯 Real-World Scenario

This solution simulates an enterprise customer analytics platform that:
- Manages customer transactions and orders (Microsoft Fabric SQL Database)
- Provides semantic search on product catalogs and reviews (Microsoft Fabric CosmosDB NoSQL with native vector search)
- Tracks real-time user sessions and behavior (Microsoft Fabric CosmosDB NoSQL)
- Uses AI agents to generate insights, recommendations, and customer sentiment analysis

## 🏗️ Architecture

```
┌────────────────────────────────────────────────────────┐
│                    Streamlit UI                        │
│  (Interactive Dashboard, Chat Interface, Analytics)    │
└────────────────────────────┬───────────────────────────┘
                             │
┌────────────────────────────▼───────────────────────────┐
│          Microsoft Agent Framework Layer               │
│  - AI Orchestrator Agent                               │
│  - Customer Insights Plugin                            │
│  - Recommendation Engine Plugin (Vector Search)        │
│  - Sentiment Analysis Plugin                           │
└──────┬────────────────────────────────────┬────────────┘
       │                                    │
┌──────▼──────────┐              ┌──────────▼────────────┐
│ Fabric SQL DB   │              │ Fabric CosmosDB NoSQL │
│                 │              │  (with Vector Search) │
│ Customers       │              │                       │
│ Orders          │              │ Products + Embeddings │
│ Transactions    │              │ Reviews + Embeddings │
│                 │              │ User Sessions         │
│                 │              │ Click Streams         │
└─────────────────┘              └───────────────────────┘
```

## 🚀 Features

### 1. Multi-Database Integration (Microsoft Fabric)
- **Fabric SQL Database**: Structured transactional data (customers, orders)
- **Fabric CosmosDB NoSQL with Vector Search**: 
  - Product catalog with semantic search (1536-dim embeddings)
  - Customer reviews with sentiment embeddings
  - Real-time session tracking and analytics
  - Native `VectorDistance()` queries with quantizedFlat indexing

### 2. AI-Powered Capabilities (via Microsoft Agent Framework)
- **Intelligent Query Agent**: Natural language to SQL/NoSQL queries
- **Customer Insights**: AI-generated analysis of customer behavior
- **Product Recommendations**: Semantic vector search + collaborative filtering
- **Sentiment Analysis**: Analyze customer reviews and feedback with embeddings
- **Predictive Analytics**: Forecast trends and customer churn

### 3. Interactive Streamlit Dashboard
- Real-time analytics visualization
- Natural language chat interface
- Customer 360° view
- Product recommendation engine with vector search
- Session analytics and heatmaps

## 📋 Prerequisites

- Python 3.10 or higher
- Microsoft Fabric workspace with access to:
  - Microsoft Fabric SQL Database
  - Microsoft Fabric CosmosDB NoSQL (with vector search enabled)
- Azure OpenAI Service or OpenAI API key

## 📚 Documentation

- **[Quick Start Guide](docs/QUICKSTART.md)** - Installation and verification steps

## 🛠️ Setup Instructions

### 1. Clone and Install Dependencies

```bash
# Install Python dependencies
pip install -r requirements.txt
```

**Note**: Agent Framework is pre-release. Installation may take a few moments.

### 2. Configure Environment Variables

Copy `.env.example` to `.env` and fill in your credentials:

```bash
cp .env.example .env
```

Edit `.env` with your Microsoft Fabric and Azure credentials:
```bash
# Azure OpenAI
AZURE_OPENAI_ENDPOINT=https://your-resource.openai.azure.com/
AZURE_OPENAI_API_KEY=your-api-key
AZURE_OPENAI_DEPLOYMENT_NAME=gpt-4
AZURE_OPENAI_EMBEDDING_DEPLOYMENT=text-embedding-ada-002

# Microsoft Fabric SQL Database (uses Entra ID authentication)
FABRIC_SQL_ENDPOINT=your-workspace.datawarehouse.fabric.microsoft.com
FABRIC_SQL_DATABASE=CustomerDB
FABRIC_SQL_DRIVER="ODBC Driver 18 for SQL Server"

# Microsoft Fabric CosmosDB NoSQL (uses Entra ID authentication)
FABRIC_COSMOSDB_ENDPOINT=https://your-account.documents.azure.com:443/
FABRIC_COSMOSDB_DATABASE=IntelliCAPDB
FABRIC_COSMOSDB_SESSIONS_CONTAINER=Sessions
FABRIC_COSMOSDB_PRODUCTS_CONTAINER=Products
FABRIC_COSMOSDB_REVIEWS_CONTAINER=Reviews

# Optional: Azure Entra ID Service Principal (for production)
# If not set, will use Azure CLI credentials (az login)
# AZURE_TENANT_ID=your-tenant-id
# AZURE_CLIENT_ID=your-client-id
# AZURE_CLIENT_SECRET=your-client-secret
```

### 3. Configure Authentication

Microsoft Fabric requires **Azure Entra ID authentication**. Choose one method:

#### Option 1: Azure CLI (Recommended for Local Development)
```bash
az login
```

#### Option 2: Service Principal (Recommended for Production)
Set these in your `.env`:
```properties
AZURE_TENANT_ID=your-tenant-id
AZURE_CLIENT_ID=your-client-id
AZURE_CLIENT_SECRET=your-client-secret
```

#### Option 3: Managed Identity (When running in Azure)
No configuration needed - automatically detected.

### 4. Load Sample Data

Generate sample data for your databases:

```bash
python scripts/generate_sample_data.py
```

This will create:
- Customers and orders in Fabric SQL Database
- Products with embeddings in CosmosDB NoSQL
- Reviews with embeddings in CosmosDB NoSQL
- Sample user sessions in CosmosDB NoSQL

### 5. Run the Application

```bash
streamlit run Home.py
```

The application will open in your browser at `http://localhost:8501`

## 📁 Project Structure

```
IntelliCAP/
├── Home.py                         # Main Streamlit application
├── requirements.txt                # Python dependencies (includes agent-framework)
├── .env                            # Environment variables
├── README.md                       # This file
│
├── src/
│   ├── __init__.py
│   ├── database/
│   │   ├── __init__.py
│   │   ├── fabric_sql.py          # Microsoft Fabric SQL Database connector
│   │   └── fabric_cosmos.py       # Fabric CosmosDB NoSQL connector (with vector search)
│   │
│   ├── agent_integration/         # Microsoft Agent Framework integration
│   │   ├── __init__.py
│   │   ├── agent_config.py        # Agent creation and embedding service
│   │   └── plugins/
│   │       ├── __init__.py
│   │       ├── customer_insights.py     # Customer analytics tools
│   │       ├── recommendation_engine.py # Product recommendations with vector search
│   │       └── sentiment_analysis.py    # Review sentiment analysis
│   │
│   └── utils/
│       ├── __init__.py
│       └── config.py              # Configuration management
│
├── scripts/
│   ├── check_azure_deployments.py  # Validate Azure OpenAI deployments
│   ├── check_odbc_drivers.py       # ODBC driver validation
│   ├── create_stored_procedures.py # Create SQL stored procedures
│   ├── database_summary.py         # Database summary report
│   ├── generate_cosmos_products.py # Generate CosmosDB products
│   ├── generate_sample_data.py     # Main data generation script
│   ├── setup_environment.py        # Environment setup script
│   └── verify_environment.py       # Verify installation and configuration
│
├── sql/
│   └── fabric_sql_schema.sql       # Fabric SQL schema definitions
│
├── docs/
│   └── QUICKSTART.md              # Quick start guide
│
└── pages/
    ├── 1_Customer_Analytics.py    # Customer 360° view
    ├── 2_Product_Recommendations.py  # Semantic product search
    ├── 3_Sentiment_Analysis.py    # Review sentiment dashboard
    └── 4_AI_Chat.py               # Natural language agent interface
```

## 🎮 Usage Examples

### 1. Customer Insights
Navigate to the "Customer Analytics" page to:
- View customer purchase history from Microsoft Fabric SQL Database
- See AI-generated insights about customer behavior
- Predict customer churn risk
- View customer lifetime value

### 2. Product Recommendations
Use the "Product Recommendations" page to:
- Perform semantic search on product catalog (CosmosDB NoSQL with vector search)
- Get AI-powered product recommendations
- Analyze product review sentiments

### 3. Real-Time Analytics
Monitor session data from Fabric CosmosDB NoSQL:
- Track user click streams
- View real-time session analytics
- Analyze user behavior patterns

### 4. AI Chat Interface
Interact with the AI agent using natural language:
- "Show me top 10 customers by revenue"
- "Find similar products to running shoes"
- "What's the sentiment trend for product X?"
- "Predict which customers are likely to churn"

## 🔑 Key Technologies

- **Microsoft Fabric**: Unified data platform for all database services
- **Microsoft Agent Framework**: AI orchestration and plugin architecture
- **Streamlit**: Interactive web UI
- **Fabric SQL Database**: Relational database for transactional data
- **Fabric CosmosDB NoSQL**: NoSQL database with native vector search for semantic operations
- **Azure OpenAI**: GPT-4 for insights, text-embedding-ada-002 for semantic search
- **pandas & plotly**: Data analysis and visualization

## 📊 AI Capabilities Demonstrated

1. **Natural Language to SQL**: Convert user questions to database queries
2. **Semantic Search**: Vector similarity search using embeddings
3. **Sentiment Analysis**: Analyze customer feedback and reviews
4. **Predictive Analytics**: Customer churn prediction, sales forecasting
5. **Personalized Recommendations**: AI-driven product suggestions
6. **Data Insights**: Automated insight generation from multi-database queries

## 🔒 Security Best Practices

- Store credentials in `.env` file (never commit to git)
- Use Azure Managed Identity when deploying to Azure
- Implement proper connection pooling
- Use parameterized queries to prevent SQL injection
- Enable Microsoft Fabric workspace security settings
- Use SSL/TLS for all database connections

## 📈 Scaling Considerations

- Implement caching for frequent queries
- Use connection pooling for database connections
- Leverage Microsoft Fabric capacity scaling
- Leverage Fabric CosmosDB partitioning for high-throughput scenarios
- Use Azure Functions for background processing

## 🤝 Contributing

This is a demonstration project for educational purposes. Feel free to extend and customize for your needs.

## 📝 License

MIT License - See LICENSE file for details

## 🙏 Acknowledgments

- Microsoft Agent Framework team
- Microsoft Fabric services documentation
- Streamlit community
