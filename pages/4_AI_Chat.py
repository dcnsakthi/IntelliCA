"""
AI Chat Interface
Natural language interface for querying customer and product data
"""
import streamlit as st
import sys
import os
import asyncio
from datetime import datetime
import html

# Add parent directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

st.set_page_config(
    page_title="AI Chat",
    page_icon="🤖",
    layout="wide"
)

def main():
    st.title("🤖 AI Chat Interface")
    st.markdown("Ask questions about customers, products, and analytics in natural language")
    st.markdown("---")
    
    # Check if connectors are initialized
    if 'agent' not in st.session_state:
        st.error("AI agent not initialized. Please go back to the home page.")
        st.stop()
    
    agent = st.session_state.agent
    sql_conn = st.session_state.sql_conn
    cosmos_conn = st.session_state.cosmos_conn
    
    # Initialize chat history
    if 'chat_history' not in st.session_state:
        st.session_state.chat_history = []
    
    # Sidebar with sample queries
    with st.sidebar:
        st.markdown("### 💡 Sample Queries")
        st.markdown("""
        Try asking:
        
        **Customer Analytics:**
        - "Show me top 10 customers by revenue"
        - "Find customers at risk of churning"
        - "What's the average lifetime value?"
        
        **Product Insights:**
        - "Find products similar to running shoes"
        - "Which products have the best reviews?"
        - "Show products with low stock"
        
        **Sentiment Analysis:**
        - "What's the sentiment for laptop reviews?"
        - "Show negative reviews from last month"
        - "Which categories have best sentiment?"
        
        **General Analytics:**
        - "How many orders in the last 30 days?"
        - "What's the most popular product category?"
        - "Show customer segmentation breakdown"
        """)
        
        st.markdown("---")
        
        if st.button("🗑️ Clear Chat History"):
            st.session_state.chat_history = []
            st.rerun()
    
    # Chat interface
    st.markdown("### 💬 Chat")
    
    # Display chat history using native Streamlit chat components
    chat_container = st.container()
    with chat_container:
        for idx, message in enumerate(st.session_state.chat_history):
            if message['role'] == 'user':
                with st.chat_message("user"):
                    st.write(message['content'])
            else:
                with st.chat_message("assistant"):
                    st.write(message['content'])
    
    # Input area
    st.markdown("---")
    
    # Use a form to handle Enter key submission and clear the input
    with st.form(key="chat_form", clear_on_submit=True):
        col1, col2 = st.columns([5, 1])
        
        with col1:
            user_input = st.text_input(
                "Ask a question",
                placeholder="Type your question here...",
                key="user_input_field",
                label_visibility="collapsed"
            )
        
        with col2:
            send_button = st.form_submit_button("Send 📤", use_container_width=True)
    
    # Quick action buttons
    st.markdown("### ⚡ Quick Actions")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        if st.button("👥 Top Customers", use_container_width=True):
            user_input = "Show me the top 10 customers by lifetime value"
            send_button = True
    
    with col2:
        if st.button("📊 Sales Summary", use_container_width=True):
            user_input = "Give me a summary of sales in the last 30 days"
            send_button = True
    
    with col3:
        if st.button("⚠️ Churn Risk", use_container_width=True):
            user_input = "Which customers are at high risk of churning?"
            send_button = True
    
    with col4:
        if st.button("💬 Sentiment", use_container_width=True):
            user_input = "What's the overall sentiment of product reviews?"
            send_button = True
    
    # Process input
    if send_button and user_input:
        # Add user message to history
        st.session_state.chat_history.append({
            'role': 'user',
            'content': user_input,
            'timestamp': datetime.now()
        })
        
        with st.spinner("🤔 Thinking..."):
            try:
                # Route query to appropriate plugin based on keywords
                response = process_query(user_input, agent, sql_conn, cosmos_conn)
                
                # Add AI response to history
                st.session_state.chat_history.append({
                    'role': 'assistant',
                    'content': response,
                    'timestamp': datetime.now()
                })
                
                st.rerun()
            
            except Exception as e:
                error_msg = f"Sorry, I encountered an error: {str(e)}"
                st.session_state.chat_history.append({
                    'role': 'assistant',
                    'content': error_msg,
                    'timestamp': datetime.now()
                })
                st.rerun()


def process_query(query: str, agent, sql_conn, cosmos_conn) -> str:
    """
    Process user query and route to appropriate plugin.
    
    Args:
        query: User's natural language query
        agent: Agent Framework instance
        sql_conn: SQL connector
        cosmos_conn: Cosmos DB connector
    
    Returns:
        Response string
    """
    query_lower = query.lower()
    
    # Sentiment-related queries (Check FIRST - highest priority for review/sentiment queries)
    if any(word in query_lower for word in ['sentiment', 'review', 'reviews', 'feedback', 'rating']):
        try:
            # Get reviews from CosmosDB
            reviews_query = "SELECT c.sentimentLabel, c.rating, c.productId, c.reviewText FROM c"
            reviews_results = list(cosmos_conn.reviews_container.query_items(
                query=reviews_query,
                enable_cross_partition_query=True
            ))
            
            if reviews_results:
                import pandas as pd
                reviews_df = pd.DataFrame(reviews_results)
                
                # Check if asking about specific category sentiment
                if any(cat in query_lower for cat in ['clothing', 'electronics', 'home', 'sports']):
                    # Find the category mentioned
                    category = None
                    if 'clothing' in query_lower:
                        category = 'Clothing'
                    elif 'electronics' in query_lower:
                        category = 'Electronics'
                    elif 'home' in query_lower:
                        category = 'Home'
                    elif 'sports' in query_lower:
                        category = 'Sports'
                    
                    # Get products from that category
                    products = cosmos_conn.get_all_products(limit=1000)
                    category_products = [p for p in products if p.get('category') == category]
                    category_product_ids = [p.get('productId') or p.get('id') for p in category_products]
                    
                    # Filter reviews for that category
                    category_reviews = reviews_df[reviews_df['productId'].isin(category_product_ids)]
                    
                    if not category_reviews.empty and ('best' in query_lower or 'highest' in query_lower):
                        # Find products with best sentiment in that category
                        positive_reviews = category_reviews[category_reviews['sentimentLabel'] == 'positive']
                        if not positive_reviews.empty:
                            product_ratings = positive_reviews.groupby('productId').agg(
                                count=('rating', 'size'),
                                avg_rating=('rating', 'mean')
                            ).sort_values('avg_rating', ascending=False)
                            
                            response = f"Best {category} products by sentiment:\n\n"
                            for product_id, row in product_ratings.head(5).iterrows():
                                # Get product name
                                product = next((p for p in category_products if (p.get('productId') or p.get('id')) == product_id), None)
                                product_name = product.get('name', product_id) if product else product_id
                                response += f"- {product_name}: Avg Rating {float(row['avg_rating']):.2f} ⭐ ({int(row['count'])} positive reviews)\n"
                            return response
                        else:
                            return f"No positive reviews found for {category} products."
                    elif not category_reviews.empty:
                        # Overall sentiment for category
                        sentiment_df = category_reviews.groupby('sentimentLabel', as_index=False).agg(
                            count=('sentimentLabel', 'size'),
                            avg_rating=('rating', 'mean')
                        )
                        total = sentiment_df['count'].sum()
                        response = f"Sentiment analysis for {category} products ({int(total)} reviews):\n\n"
                        for idx, row in sentiment_df.iterrows():
                            percentage = (row['count'] / total * 100)
                            response += f"- {row['sentimentLabel'].capitalize()}: {percentage:.1f}% ({int(row['count'])} reviews, Avg: {float(row['avg_rating']):.2f}⭐)\n"
                        return response
                    else:
                        return f"No reviews found for {category} products."
                
                if 'best' in query_lower:
                    # Products with best reviews overall
                    positive_reviews = reviews_df[reviews_df['sentimentLabel'] == 'positive']
                    if not positive_reviews.empty:
                        product_ratings = positive_reviews.groupby('productId').agg(
                            count=('rating', 'size'),
                            avg_rating=('rating', 'mean')
                        ).sort_values('avg_rating', ascending=False)
                        
                        # Get product names
                        products = cosmos_conn.get_all_products(limit=1000)
                        product_dict = {(p.get('productId') or p.get('id')): p.get('name', 'Unknown') for p in products}
                        
                        response = "Products with the best reviews:\n\n"
                        for product_id, row in product_ratings.head(10).iterrows():
                            product_name = product_dict.get(product_id, product_id)
                            response += f"- {product_name}: Avg Rating {float(row['avg_rating']):.2f}⭐ ({int(row['count'])} positive reviews)\n"
                        return response
                    else:
                        return "No positive reviews found."
                
                else:
                    # Overall sentiment summary
                    sentiment_df = reviews_df.groupby('sentimentLabel', as_index=False).agg(
                        count=('sentimentLabel', 'size'),
                        avg_rating=('rating', 'mean')
                    )
                    sentiment_df = sentiment_df.rename(columns={'sentimentLabel': 'sentiment_label'})
                    
                    total = sentiment_df['count'].sum()
                    response = f"Overall sentiment analysis from Fabric CosmosDB (Total reviews: {int(total)}):\n\n"
                    for idx, row in sentiment_df.iterrows():
                        percentage = (row['count'] / total * 100)
                        response += f"- {row['sentiment_label'].capitalize()}: {percentage:.1f}% ({int(row['count'])} reviews, Avg: {float(row['avg_rating']):.2f}⭐)\n"
                    return response
            else:
                return "No review data available in Fabric CosmosDB."
        
        except Exception as e:
            return f"Error processing sentiment query: {str(e)}"
    
    # Customer-related queries
    elif any(word in query_lower for word in ['customer', 'customers', 'churn', 'lifetime value', 'ltv', 'segment']):
        try:
            # Use CustomerInsights plugin
            if 'churn' in query_lower:
                churn_df = sql_conn.get_churn_risk_customers(risk_threshold=70.0)
                if not churn_df.empty:
                    response = f"Found {len(churn_df)} customers at high risk of churning:\n\n"
                    for idx, row in churn_df.head(10).iterrows():
                        response += f"- {row['FirstName']} {row['LastName']} (ID: {row['CustomerID']}) - Risk Score: {row['ChurnRiskScore']}%\n"
                    return response
                else:
                    return "No customers found at high risk of churning."
            
            elif 'top' in query_lower and 'customer' in query_lower:
                top_df = sql_conn.get_top_customers(limit=10)
                if not top_df.empty:
                    response = "Top 10 customers by lifetime value:\n\n"
                    for idx, row in top_df.iterrows():
                        response += f"{idx+1}. {row['FirstName']} {row['LastName']} - ${float(row['TotalLifetimeValue']):,.2f}\n"
                    return response
                else:
                    return "No customer data available."
            
            elif 'segment' in query_lower and 'breakdown' in query_lower:
                segments_df = sql_conn.get_customer_segments_distribution()
                if not segments_df.empty:
                    response = "Customer Segmentation Breakdown:\n\n"
                    for idx, row in segments_df.iterrows():
                        response += f"- {row['CustomerSegment']}: {int(row['CustomerCount'])} customers (${float(row['TotalValue']):,.2f} total value)\n"
                    return response
                else:
                    return "No segmentation data available."
            
            elif 'average' in query_lower and ('lifetime value' in query_lower or 'ltv' in query_lower):
                avg_df = sql_conn.execute_query(
                    "SELECT AVG(TotalLifetimeValue) as avg FROM ca.Customers WHERE IsActive = 1"
                )
                if not avg_df.empty and avg_df.iloc[0]['avg']:
                    avg_ltv = float(avg_df.iloc[0]['avg'])
                    return f"The average customer lifetime value is ${avg_ltv:,.2f}"
                else:
                    return "Unable to calculate average lifetime value."
        
        except Exception as e:
            return f"Error processing customer query: {str(e)}"
    
    # Product-related queries (Fetch from Fabric CosmosDB)
    elif any(word in query_lower for word in ['product', 'products', 'similar', 'recommendation', 'category', 'categories']):
        try:
            # Fetch products from CosmosDB
            products = cosmos_conn.get_all_products(limit=1000)
            
            if 'low stock' in query_lower or 'out of stock' in query_lower:
                # Filter low stock products
                low_stock = [p for p in products if p.get('stockQuantity', 0) < 10]
                if low_stock:
                    response = "Products with low stock (from Fabric CosmosDB):\n\n"
                    for product in low_stock[:10]:
                        response += f"- {product.get('name', 'Unknown')} ({product.get('category', 'N/A')}) - Stock: {int(product.get('stockQuantity', 0))}\n"
                    return response
                else:
                    return "All products have adequate stock levels."
            
            elif 'popular' in query_lower and 'category' in query_lower:
                # Count products by category
                from collections import Counter
                categories = [p.get('category') for p in products if p.get('category')]
                if categories:
                    category_counts = Counter(categories)
                    most_popular = category_counts.most_common(1)[0]
                    response = f"Most popular product category: **{most_popular[0]}** ({most_popular[1]} products)\n\nCategory breakdown:\n"
                    for category, count in category_counts.most_common():
                        response += f"- {category}: {count} products\n"
                    return response
                else:
                    return "No category data available."
            
            elif 'category' in query_lower or 'categories' in query_lower:
                # List all categories
                categories = set(p.get('category') for p in products if p.get('category'))
                if categories:
                    response = f"Product categories available ({len(categories)} categories):\n\n"
                    for category in sorted(categories):
                        count = sum(1 for p in products if p.get('category') == category)
                        response += f"- {category}: {count} products\n"
                    return response
                else:
                    return "No category data available."
            
            else:
                # Generic product query
                if products:
                    response = f"There are currently {len(products)} products in the Fabric CosmosDB catalog.\n\n"
                    response += "Sample products:\n"
                    for product in products[:5]:
                        response += f"- {product.get('name', 'Unknown')} ({product.get('category', 'N/A')}) - ${float(product.get('price', 0)):,.2f}\n"
                    response += "\nYou can search for products or get recommendations in the Product Recommendations page."
                    return response
                else:
                    return "No product data available in CosmosDB."
        
        except Exception as e:
            return f"Error processing product query: {str(e)}"
    
    # Sales/Orders queries
    elif any(word in query_lower for word in ['order', 'orders', 'sales', 'revenue']):
        try:
            if 'last 30 days' in query_lower or 'last month' in query_lower:
                orders_df = sql_conn.execute_query("""
                    SELECT 
                        COUNT(*) as order_count,
                        SUM(TotalAmount) as total_revenue,
                        AVG(TotalAmount) as avg_order_value
                    FROM ca.Orders
                    WHERE OrderDate >= DATEADD(day, -30, GETDATE())
                """)
                if not orders_df.empty:
                    row = orders_df.iloc[0]
                    order_count = int(row['order_count']) if row['order_count'] else 0
                    total_revenue = float(row['total_revenue']) if row['total_revenue'] else 0
                    avg_value = float(row['avg_order_value']) if row['avg_order_value'] else 0
                    
                    return f"""Sales Summary (Last 30 Days):
                    
- Total Orders: {order_count:,}
- Total Revenue: ${total_revenue:,.2f}
- Average Order Value: ${avg_value:,.2f}
"""
                else:
                    return "No order data available for the last 30 days."
            else:
                # Generic orders query
                count_df = sql_conn.execute_query("SELECT COUNT(*) as count FROM ca.Orders")
                if not count_df.empty:
                    count = int(count_df.iloc[0]['count'])
                    return f"There are {count:,} total orders in the system."
                else:
                    return "No order data available."
        
        except Exception as e:
            return f"Error processing order query: {str(e)}"
    
    # Generic response
    else:
        return """I can help you with:
        
- **Customer Analytics**: Top customers, churn risk, lifetime value
- **Product Insights**: Product catalog, stock levels, recommendations
- **Sentiment Analysis**: Review sentiment, ratings analysis
- **Sales Data**: Order history, revenue summaries

Please ask a specific question, or use the quick action buttons below!"""


if __name__ == "__main__":
    main()
