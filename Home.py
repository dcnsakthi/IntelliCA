"""
Intelligent Customer Analytics Platform
Main Streamlit Application
"""
import streamlit as st
import sys
import os
from dotenv import load_dotenv
import logging
import asyncio

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Import connectors and Agent Framework
from src.database import AzureSQLConnector, PostgreSQLConnector, CosmosDBConnector
from src.agent_integration import create_agent, get_embedding_service
from src.agent_integration.plugins.customer_insights import CustomerInsightsPlugin
from src.agent_integration.plugins.recommendation_engine import RecommendationEnginePlugin
from src.agent_integration.plugins.sentiment_analysis import SentimentAnalysisPlugin

# Page configuration
st.set_page_config(
    page_title="Customer Analytics Platform",
    page_icon="🎯",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #0078D4;
        text-align: center;
        padding: 1rem 0;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1.5rem;
        border-radius: 0.5rem;
        border-left: 4px solid #0078D4;
    }
    .success-box {
        background-color: #d4edda;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #28a745;
    }
    .warning-box {
        background-color: #fff3cd;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #ffc107;
    }
    .error-box {
        background-color: #f8d7da;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #dc3545;
    }
</style>
""", unsafe_allow_html=True)


@st.cache_resource
def initialize_database_connectors():
    """Initialize database connectors with Entra ID authentication (cached)."""
    try:
        # Microsoft Fabric SQL Database (Entra ID auth)
        driver = os.getenv("FABRIC_SQL_DRIVER", "ODBC Driver 18 for SQL Server")

        sql_connector = AzureSQLConnector(
            endpoint=os.getenv("FABRIC_SQL_ENDPOINT"),
            database=os.getenv("FABRIC_SQL_DATABASE"),
            driver=f"{{{driver}}}"
        )        
        
        # Microsoft Fabric CosmosDB NoSQL API (Entra ID auth)
        # Unified connector for sessions, products, and reviews with vector search
        cosmos_connector = CosmosDBConnector(
            endpoint=os.getenv("FABRIC_COSMOSDB_ENDPOINT"),
            database_name=os.getenv("FABRIC_COSMOSDB_DATABASE", "IntelliCAPDB"),
            container_name=os.getenv("FABRIC_COSMOSDB_SESSIONS_CONTAINER", "Sessions"),
            products_container_name=os.getenv("FABRIC_COSMOSDB_PRODUCTS_CONTAINER", "Products"),
            reviews_container_name=os.getenv("FABRIC_COSMOSDB_REVIEWS_CONTAINER", "Reviews")
        )
        cosmos_connector.initialize()
        
        logger.info("Microsoft Fabric database connectors initialized successfully with Entra ID authentication")
        logger.info("Using CosmosDB NoSQL with native vector search for products and reviews")
        return sql_connector, cosmos_connector
        
    except Exception as e:
        logger.error(f"Failed to initialize database connectors: {e}")
        st.error(f"Database initialization error: {e}")
        return None, None


@st.cache_resource
def initialize_agent_framework(_sql_conn, _cosmos_conn):
    """Initialize Agent Framework with tools (cached)."""
    try:
        # Get embedding service first
        embedding_service = get_embedding_service(use_azure=True)
        
        # Create plugin instances
        customer_plugin = CustomerInsightsPlugin(_sql_conn, _cosmos_conn)
        recommendation_plugin = RecommendationEnginePlugin(_cosmos_conn, embedding_service)
        sentiment_plugin = SentimentAnalysisPlugin(_cosmos_conn)
        
        # Collect all AI functions from plugins
        # In Agent Framework with @ai_function, functions are automatically registered
        # We just need to create the agent - the decorated functions will be available
        agent = create_agent(use_azure=True)
        
        # Store plugin references for direct method calls if needed
        agent._customer_plugin = customer_plugin
        agent._recommendation_plugin = recommendation_plugin
        agent._sentiment_plugin = sentiment_plugin
        
        logger.info("Agent Framework initialized with all plugins")
        return agent, embedding_service
        
    except Exception as e:
        logger.error(f"Failed to initialize Agent Framework: {e}")
        st.error(f"Agent Framework initialization error: {e}")
        return None, None


def check_environment():
    """Check if environment variables are properly configured."""
    required_vars = [
        "AZURE_OPENAI_ENDPOINT",
        "AZURE_OPENAI_API_KEY",
        "FABRIC_SQL_ENDPOINT",
        "FABRIC_COSMOSDB_ENDPOINT"
    ]
    
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        st.error("⚠️ Missing required environment variables:")
        for var in missing_vars:
            st.error(f"  - {var}")
        st.info("Please configure your .env file with the required credentials.")
        return False
    
    return True


def main():
    """Main application entry point."""
    
    # Header
    st.markdown('<div class="main-header">🎯 Intelligent Customer Analytics Platform</div>', 
                unsafe_allow_html=True)
    
    st.markdown("---")
    
    # Check environment
    if not check_environment():
        st.stop()
    
    # Initialize connections
    with st.spinner("Initializing database connections..."):
        sql_conn, cosmos_conn = initialize_database_connectors()
    
    if not all([sql_conn, cosmos_conn]):
        st.error("Failed to initialize database connectors. Please check your configuration.")
        st.stop()
    
    # Initialize Agent Framework
    with st.spinner("Initializing AI components..."):
        agent, embedding_service = initialize_agent_framework(sql_conn, cosmos_conn)
    
    if not agent:
        st.error("Failed to initialize Agent Framework. Please check your configuration.")
        st.stop()
    
    # Store in session state
    if 'sql_conn' not in st.session_state:
        st.session_state.sql_conn = sql_conn
        st.session_state.cosmos_conn = cosmos_conn
        st.session_state.agent = agent
        st.session_state.embedding_service = embedding_service
    
    # Sidebar
    with st.sidebar:
        # st.image("https://via.placeholder.com/150x50/0078D4/FFFFFF?text=Analytics+AI", width=150)
        st.markdown("### 🎯 Navigation")
        st.info("""
        Navigate using the pages in the sidebar:
        - **Customer Analytics**: 360° customer view
        - **Product Recommendations**: AI-powered suggestions
        - **Sentiment Analysis**: Review analytics
        - **AI Chat**: Natural language interface
        """)
        
        st.markdown("---")
        st.markdown("### 🔌 System Status")
        
        # Test connections
        if st.button("🔄 Test Connections"):
            with st.spinner("Testing connections..."):
                col1, col2 = st.columns(2)
                
                with col1:
                    if sql_conn.test_connection():
                        st.success("✅ Fabric SQL")
                    else:
                        st.error("❌ Fabric SQL")
                
                with col2:
                    if cosmos_conn.container:
                        st.success("✅ Fabric CosmosDB")
                    else:
                        st.error("❌ Fabric CosmosDB")
        
        st.markdown("---")
        st.markdown("### 📊 Tech Stack")
        st.markdown("""
        - **AI Framework**: Microsoft Agent Framework
        - **Databases**: 
          - Microsoft Fabric SQL Database
          - Microsoft Fabric CosmosDB (NoSQL API)
        - **Authentication**: Microsoft Entra ID (formerly Azure AD)
        - **UI**: Streamlit
        - **AI Model**: Azure OpenAI GPT-4
        """)
    
    # Main content
    st.markdown("## 🏠 Welcome to the Customer Analytics Platform")
    
    st.markdown("""
    This platform demonstrates the integration of **AI and multi-database technologies** 
    to provide intelligent customer analytics, product recommendations, and sentiment analysis.
    """)
    
    # Architecture diagram
    st.markdown("### 🏗️ System Architecture")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("""
        <div class="metric-card">
        <h4>🗄️ Microsoft Fabric SQL Database</h4>
        <p>Transactional data including customers, orders, and products.</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("""
        <div class="metric-card">
        <h4>🔍 Microsoft Fabric CosmosDB</h4>
        <p><strong>NoSQL API:</strong> Product catalog with semantic search using OpenAI embeddings.</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        st.markdown("""
        <div class="metric-card">
        <h4>🌐 Microsoft Fabric CosmosDB</h4>
        <p><strong>NoSQL API:</strong> Real-time session tracking and behavioral analytics.</p>
        </div>
        """, unsafe_allow_html=True)
    
    st.markdown("---")
    
    # Quick stats
    st.markdown("### 📈 Quick Statistics")
    
    try:
        # Get customer count
        customer_df = sql_conn.execute_query("SELECT COUNT(*) as count FROM ca.Customers WHERE IsActive = 1")
        customer_count = int(customer_df.iloc[0]['count']) if not customer_df.empty else 0
        
        # Get product count from CosmosDB
        try:
            products = cosmos_conn.get_all_products(limit=1000)
            product_count = len(products)
        except:
            product_count = 0
        
        # Get recent orders
        orders_df = sql_conn.execute_query("SELECT COUNT(*) as count FROM ca.Orders WHERE OrderDate >= DATEADD(day, -30, GETDATE())")
        orders_count = int(orders_df.iloc[0]['count']) if not orders_df.empty else 0
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Active Customers", f"{customer_count:,}")
        
        with col2:
            st.metric("Products", f"{product_count:,}")
        
        with col3:
            st.metric("Orders (30d)", f"{orders_count:,}")
        
        with col4:
            st.metric("AI Models", "GPT-4 + Ada-002")
    
    except Exception as e:
        st.warning(f"Unable to load statistics: {e}")
    
    st.markdown("---")
    
    # Features overview
    st.markdown("### ✨ Key Features")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        **🎯 Customer Analytics**
        - 360° customer view
        - Purchase history analysis
        - Churn risk prediction
        - Customer segmentation
        
        **🛍️ Product Recommendations**
        - Semantic search using AI
        - Similar product discovery
        - Personalized suggestions
        - Vector similarity matching
        """)
    
    with col2:
        st.markdown("""
        **💬 Sentiment Analysis**
        - Product review analysis
        - Sentiment scoring
        - Theme extraction
        - Trend identification
        
        **🤖 AI Chat Interface**
        - Natural language queries
        - Multi-database orchestration
        - Intelligent insights generation
        - Contextual recommendations
        """)
    
    st.markdown("---")
    
    # Getting started
    st.markdown("### 🚀 Getting Started")
    
    st.info("""
    **Navigate through the pages using the sidebar:**
    
    1. **Customer Analytics** - Explore customer data, view profiles, and analyze behavior
    2. **Product Recommendations** - Search products semantically and get recommendations
    3. **Sentiment Analysis** - Analyze product reviews and sentiment trends
    4. **AI Chat** - Interact with the AI assistant using natural language
    
    Each page demonstrates integration with multiple Azure databases and AI capabilities!
    """)
    
    # Demo mode
    if st.checkbox("🎮 Show Demo Mode"):
        st.markdown("### 🎮 Demo Mode")
        st.success("""
        **Demo features enabled!**
        
        Try these sample queries in the AI Chat page:
        - "Show me top 10 customers by revenue"
        - "Find products similar to running shoes"
        - "What's the sentiment for product reviews?"
        - "Identify customers at risk of churning"
        """)


if __name__ == "__main__":
    main()
