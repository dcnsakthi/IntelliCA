# Quick Start Guide

## 🚀 Installation & Setup

### Step 1: Install Dependencies

```powershell
# Navigate to project directory
cd IntelliCA

# Install all dependencies (including Agent Framework)
pip install -r requirements.txt
```

**Note**: Agent Framework is pre-release, so it may take a few moments to install all packages.

### Step 2: Configure Environment Variables

Copy `.env.example` to `.env` and configure your settings:

```powershell
# Copy the example file
Copy-Item .env.example .env

# Edit .env with your credentials
notepad .env
```

Required configuration:
```env
# Azure OpenAI Configuration
AZURE_OPENAI_ENDPOINT=https://your-resource.openai.azure.com/
AZURE_OPENAI_API_KEY=your-api-key-here
AZURE_OPENAI_DEPLOYMENT_NAME=gpt-4
AZURE_OPENAI_EMBEDDING_DEPLOYMENT=text-embedding-ada-002

# Microsoft Fabric SQL Database (uses Entra ID authentication)
FABRIC_SQL_ENDPOINT=your-workspace.datawarehouse.fabric.microsoft.com
FABRIC_SQL_DATABASE=CustomerDB

# Microsoft Fabric CosmosDB NoSQL (uses Entra ID authentication)
FABRIC_COSMOSDB_ENDPOINT=https://your-account.documents.azure.com:443/
FABRIC_COSMOSDB_DATABASE=IntelliCAPDB
FABRIC_COSMOSDB_PRODUCTS_CONTAINER=Products
FABRIC_COSMOSDB_REVIEWS_CONTAINER=Reviews
FABRIC_COSMOSDB_SESSIONS_CONTAINER=Sessions
```

### Step 3: Verify Installation

```powershell
# Check Agent Framework version
python -c "import agent_framework; print(f'Agent Framework version: {agent_framework.__version__}')"

# Check if all imports work
python -c "from src.agent_integration import create_agent, get_embedding_service; print('✅ Imports successful')"
```

### Step 4: Authenticate with Azure

**Option A: Azure CLI (Recommended for Local Development)**
```powershell
# Login to Azure
az login

# Verify authentication
az account show
```

**Option B: Service Principal (For Production)**

Update your `.env` file:
```env
AZURE_TENANT_ID=your-tenant-id
AZURE_CLIENT_ID=your-client-id
AZURE_CLIENT_SECRET=your-client-secret
```

### Step 5: Test Database Connections

```powershell
# Test Fabric SQL Database connection
python -c "
from src.database import AzureSQLConnector
import os
from dotenv import load_dotenv

load_dotenv()
sql = AzureSQLConnector(
    endpoint=os.getenv('FABRIC_SQL_ENDPOINT'),
    database=os.getenv('FABRIC_SQL_DATABASE')
)
if sql.test_connection():
    print('✅ Fabric SQL Database connection successful')
else:
    print('❌ Fabric SQL Database connection failed')
"

# Test CosmosDB connection
python -c "
from src.database import CosmosDBConnector
import os
from dotenv import load_dotenv

load_dotenv()
cosmos = CosmosDBConnector(
    endpoint=os.getenv('FABRIC_COSMOSDB_ENDPOINT'),
    database_name=os.getenv('FABRIC_COSMOSDB_DATABASE')
)
cosmos.initialize()
print('✅ CosmosDB NoSQL connection successful')
"
```

### Step 6: Generate Sample Data (Optional)

If your databases are empty, generate sample data:

```powershell
# Generate sample data for all databases
python scripts/generate_sample_data.py
```

This will create:
- Customers and orders in Fabric SQL Database
- Products with embeddings in CosmosDB NoSQL
- Reviews with embeddings in CosmosDB NoSQL
- Sample user sessions in CosmosDB NoSQL

### Step 7: Run the Application

```powershell
# Start the Streamlit application
streamlit run Home.py
```

Your browser should automatically open to `http://localhost:8501`

## ✅ Verification Checklist

After the app starts, verify:

- [ ] **Homepage loads** without errors
- [ ] **Sidebar shows** "System Status" with test connection button
- [ ] **Click "Test Connections"** - both Fabric SQL and CosmosDB should show ✅
- [ ] Navigate to **"Customer Analytics"** page - should load customer data from Fabric SQL
- [ ] Navigate to **"Product Recommendations"** page - should show semantic search from CosmosDB
- [ ] Navigate to **"Sentiment Analysis"** page - should display review analytics from CosmosDB
- [ ] Navigate to **"AI Chat"** page - should allow natural language queries

## 🐛 Troubleshooting

### Import Errors

If you see import errors:
```powershell
# Reinstall agent-framework
pip uninstall agent-framework -y
pip install agent-framework[all] --pre

# Verify installation
pip list | Select-String "agent-framework"
```

### Authentication Errors

If you see "DefaultAzureCredential failed" errors:
```powershell
# Check Azure CLI login
az account show

# If not logged in
az login

# Test credential
python -c "from azure.identity import AzureCliCredential; cred = AzureCliCredential(); token = cred.get_token('https://database.windows.net/.default'); print('✅ Credential working')"
```

### Database Connection Errors

If database connections fail:

1. **Check `.env` file**: Ensure all FABRIC_* variables are set correctly
2. **Verify Entra ID permissions**: Your Azure identity needs database access
3. **Check network**: Ensure you can reach Microsoft Fabric endpoints
4. **Review logs**: Check terminal output for specific error messages
5. **Check Azure OpenAI**: Verify your deployments exist using `python scripts/check_azure_deployments.py`

Example SQL permissions for Fabric SQL Database:

```sql
-- Run in Fabric SQL Database
CREATE USER [your-email@domain.com] FROM EXTERNAL PROVIDER;
ALTER ROLE db_datareader ADD MEMBER [your-email@domain.com];
ALTER ROLE db_datawriter ADD MEMBER [your-email@domain.com];
```

For CosmosDB NoSQL, ensure your Azure identity has:
- **Cosmos DB Account Reader** role
- **Cosmos DB Data Contributor** role

### Streamlit Port Conflict

If port 8501 is already in use:
```powershell
# Run on different port
streamlit run Home.py --server.port 8502
```

## 📊 Expected Output

When you run `streamlit run Home.py`, you should see:

```text
2025-10-30 10:30:15 - __main__ - INFO - Microsoft Fabric database connectors initialized successfully with Entra ID authentication
2025-10-30 10:30:16 - __main__ - INFO - Agent Framework initialized with all tools

  You can now view your Streamlit app in your browser.

  Local URL: http://localhost:8501
  Network URL: http://192.168.1.100:8501
```

## 🎯 Next Steps

### 1. Explore the Application

- Test each page and feature
- Try the AI Chat interface
- Experiment with product recommendations
- Analyze customer insights

### 2. Explore Database Scripts

Useful scripts for database management:

```powershell
# Verify environment and connections
python scripts/verify_environment.py

# Check Azure OpenAI deployments
python scripts/check_azure_deployments.py

# Generate sample data
python scripts/generate_sample_data.py

# Generate CosmosDB products
python scripts/generate_cosmos_products.py

# Get database summary
python scripts/database_summary.py
```

### 3. Optional: Enable Tracing

Add OpenTelemetry tracing for observability:

```python
# Add to Home.py after logging configuration
from agent_framework.telemetry import setup_telemetry
setup_telemetry(
    otlp_endpoint="http://localhost:4317",
    enable_sensitive_data=True
)
```

### 4. Customize for Your Needs

- Modify agent instructions in `src/agent_integration/agent_config.py`
- Add new tools/plugins for your specific use cases
- Customize the Streamlit UI
- Add more database queries

## 📚 Documentation

- **Quick Start Guide**: This file
- **Main README**: See [README.md](../README.md) for architecture overview
- **Agent Framework Docs**: [Microsoft Learn](https://learn.microsoft.com/agent-framework/)

## 🆘 Getting Help

If you encounter issues:

1. **Check the documentation** files in this repository
2. **Review logs** in the terminal for specific error messages
3. **Agent Framework Discord**: [Join here](https://discord.gg/b5zjErwbQM)
4. **GitHub Issues**: [Report issues](https://github.com/microsoft/agent-framework/issues)
5. **Microsoft Learn Q&A**: Tag with `agent-framework`

## 🎉 Success

If all checks pass, you've successfully set up the Intelligent Customer Analytics Platform!

Your application now benefits from:

- ✅ Modern agent architecture with Microsoft Agent Framework
- ✅ Native Entra ID authentication for secure access
- ✅ Microsoft Fabric SQL Database for transactional data
- ✅ CosmosDB NoSQL with native vector search for semantic operations
- ✅ Built-in observability support with OpenTelemetry
- ✅ Cleaner, more maintainable code structure

---

**Ready to build amazing AI applications with Microsoft Fabric!** 🚀
