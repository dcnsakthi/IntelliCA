"""
Product Recommendations Page
AI-powered product search and recommendations using Microsoft Fabric CosmosDB for PostgreSQL + pgvector
"""
import streamlit as st
import pandas as pd
import sys
import os
import asyncio

# Add parent directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

st.set_page_config(
    page_title="Product Recommendations",
    page_icon="🛍️",
    layout="wide"
)

def main():
    st.title("🛍️ Product Recommendations")
    st.markdown("AI-powered product search and recommendations")
    st.markdown("---")
    
    # Check if connectors are initialized
    if 'sql_conn' not in st.session_state:
        st.error("Database connections not initialized. Please go back to the home page.")
        st.stop()
    
    sql_conn = st.session_state.sql_conn
    agent = st.session_state.agent
    embedding_service = st.session_state.embedding_service
    
    # Tabs
    tab1, tab2, tab3 = st.tabs([
        "🔍 Semantic Search", 
        "🎯 Recommendations",
        "📦 Product Catalog"
    ])
    
    # Tab 1: Semantic Search
    with tab1:
        st.subheader("🔍 Semantic Product Search")
        st.info("Search for products using natural language. AI will find semantically similar products.")
        
        col1, col2 = st.columns([4, 1])
        
        with col1:
            search_query = st.text_input(
                "Search for products",
                placeholder="e.g., comfortable running shoes, wireless headphones, gaming laptop..."
            )
        
        with col2:
            top_k = st.number_input("Results", min_value=1, max_value=20, value=5)
        
        if st.button("🔍 Search", use_container_width=True):
            if search_query:
                with st.spinner("Searching for similar products..."):
                    try:
                        # Get embedding service from session state
                        embedding_service = st.session_state.get('embedding_service')
                        cosmos_conn = st.session_state.get('cosmos_conn')
                        
                        if not embedding_service or not cosmos_conn:
                            st.error("Embedding service or CosmosDB connection not initialized. Please restart the app.")
                        else:
                            try:
                                # Generate embedding for search query
                                import asyncio
                                from src.agent_integration import generate_embeddings
                                
                                query_embedding = generate_embeddings([search_query], embedding_service, use_azure=True)
                                
                                if query_embedding and len(query_embedding) > 0:
                                    # Perform semantic search in CosmosDB
                                    results_df = cosmos_conn.search_products_by_embedding(
                                        query_embedding=query_embedding[0],
                                        limit=top_k,
                                        similarity_threshold=0.15  # Adjusted threshold for vector distance (lower = more similar)
                                    )
                                    
                                    if not results_df.empty:
                                        st.success(f"Found {len(results_df)} similar products!")
                                        
                                        # Display products in a nice format
                                        for idx, row in results_df.iterrows():
                                            # Access fields directly from the row (pandas Series)
                                            product_name = row.get('name', 'Unknown Product')
                                            category = row.get('category', 'N/A')
                                            brand = row.get('brand', 'N/A')
                                            price = row.get('price', 0)
                                            stock = row.get('stockQuantity', 0)
                                            description = row.get('description', 'No description available')
                                            similarity = row.get('similarity', 0)
                                            
                                            with st.expander(f"📦 {product_name}", expanded=True):
                                                col1, col2 = st.columns(2)
                                                with col1:
                                                    st.markdown(f"**Category:** {category}")
                                                    st.markdown(f"**Brand:** {brand}")
                                                    st.markdown(f"**Price:** ${float(price):.2f}")
                                                    st.markdown(f"**Description:** {description}")
                                                        
                                                with col2:
                                                    st.markdown(f"**Stock:** {int(stock)} units")
                                                    st.markdown(f"**Match Score:** {similarity:.2%}")
                                                    st.progress(min(similarity, 1.0))  # Cap at 100%
                                    else:
                                        st.info("No products found matching your search. Try a different query or lower the similarity threshold.")
                                else:
                                    st.error("Failed to generate embedding for search query.")
                            except Exception as embed_error:
                                # Fallback to text search if embedding fails
                                if "DeploymentNotFound" in str(embed_error) or "404" in str(embed_error):
                                    st.warning("⚠️ Semantic search is not available: Azure OpenAI embedding deployment not found.")
                                    st.info("💡 **Setup Required**: To enable semantic search, please deploy an embedding model in Azure OpenAI (e.g., text-embedding-ada-002) and update the AZURE_OPENAI_EMBEDDING_DEPLOYMENT environment variable.")
                                    st.info("📝 **Alternative**: You can use the Product Catalog tab below to browse all products.")
                                else:
                                    st.error(f"Error during semantic search: {embed_error}")
                        
                        # Old Semantic Kernel code (deprecated) - removed
                    
                    except Exception as e:
                        st.error(f"Error searching products: {e}")
            else:
                st.warning("Please enter a search query")
    
    # Tab 2: Recommendations
    with tab2:
        st.subheader("🎯 Product Recommendations")
        
        try:
            # Get all products
            products_df = sql_conn.execute_query(
                "SELECT TOP 50 ProductID as product_id, ProductName as product_name, Category as category, UnitPrice as price FROM ca.Products WHERE IsActive = 1"
            )
            
            if not products_df.empty:
                col1, col2 = st.columns([3, 1])
                
                with col1:
                    selected_product = st.selectbox(
                        "Select a product to find similar items",
                        options=products_df['product_name'].tolist()
                    )
                
                with col2:
                    num_recommendations = st.number_input(
                        "Number of recommendations",
                        min_value=1,
                        max_value=10,
                        value=5
                    )
                
                if st.button("Get Recommendations", use_container_width=True):
                    with st.spinner("Generating recommendations..."):
                        try:
                            # Get selected product details
                            selected_product_row = products_df[
                                products_df['product_name'] == selected_product
                            ].iloc[0]
                            
                            product_id = int(selected_product_row['product_id'])
                            category = selected_product_row['category']
                            price = float(selected_product_row['price'])
                            
                            # Get product from CosmosDB if available (for semantic similarity)
                            cosmos_conn = st.session_state.get('cosmos_conn')
                            embedding_service = st.session_state.get('embedding_service')
                            
                            recommendations_found = False
                            
                            # Try semantic recommendations first (if CosmosDB has products with embeddings)
                            if cosmos_conn and embedding_service:
                                try:
                                    # Check if product exists in CosmosDB with embeddings
                                    cosmos_products = cosmos_conn.get_all_products(limit=5)
                                    if cosmos_products and len(cosmos_products) > 0:
                                        # Try to find similar products
                                        similar_df = cosmos_conn.find_similar_products(
                                            product_id=f"PROD-{product_id:03d}",
                                            category=category,
                                            limit=num_recommendations
                                        )
                                        
                                        if not similar_df.empty:
                                            st.success(f"✨ Found {len(similar_df)} AI-recommended products based on semantic similarity!")
                                            
                                            for _, product in similar_df.iterrows():
                                                with st.expander(f"📦 {product.get('productName', 'Unknown')}"):
                                                    col1, col2 = st.columns(2)
                                                    with col1:
                                                        st.markdown(f"**Category:** {product.get('category', 'N/A')}")
                                                        st.markdown(f"**Brand:** {product.get('brand', 'N/A')}")
                                                        st.markdown(f"**Price:** ${float(product.get('price', 0)):.2f}")
                                                    with col2:
                                                        st.markdown(f"**Stock:** {int(product.get('stockQuantity', 0))}")
                                                        if 'similarity' in product:
                                                            similarity_score = 1 - product['similarity']
                                                            st.markdown(f"**Match:** {float(similarity_score):.2%}")
                                                    
                                                    description = product.get('description', 'No description available')
                                                    st.markdown(f"**Description:** {description}")
                                            
                                            recommendations_found = True
                                except Exception as e:
                                    # Silently fall back to SQL-based recommendations
                                    pass
                            
                            # Fallback: Category and price-based recommendations from SQL
                            if not recommendations_found:
                                # Find similar products by category and price range
                                # Note: Using string formatting for TOP clause, ? for parameters
                                rec_query = f"""
                                    SELECT TOP {num_recommendations}
                                        ProductID as product_id,
                                        ProductName as product_name,
                                        Category as category,
                                        SubCategory as subcategory,
                                        UnitPrice as price,
                                        StockQuantity as stock,
                                        ABS(UnitPrice - ?) as price_diff
                                    FROM ca.Products
                                    WHERE 
                                        ProductID != ?
                                        AND Category = ?
                                        AND IsActive = 1
                                    ORDER BY price_diff ASC
                                """
                                
                                similar_products_df = sql_conn.execute_query(
                                    rec_query,
                                    (price, product_id, category)
                                )
                                
                                if not similar_products_df.empty:
                                    st.success(f"📦 Found {len(similar_products_df)} similar products in the same category!")
                                    st.info("💡 **Tip**: These recommendations are based on category and price similarity. For AI-powered semantic recommendations, products with embeddings need to be added to CosmosDB.")
                                    
                                    for _, product in similar_products_df.iterrows():
                                        with st.expander(f"📦 {product['product_name']}"):
                                            col1, col2 = st.columns(2)
                                            with col1:
                                                st.markdown(f"**Category:** {product['category']}")
                                                st.markdown(f"**SubCategory:** {product.get('subcategory', 'N/A')}")
                                                st.markdown(f"**Price:** ${float(product['price']):.2f}")
                                            with col2:
                                                st.markdown(f"**Stock:** {int(product['stock'])}")
                                                price_diff = abs(float(product['price']) - price)
                                                st.markdown(f"**Price Difference:** ${price_diff:.2f}")
                                else:
                                    st.info(f"No similar products found in the '{category}' category. Try selecting a product from a category with more items!")
                            
                            # Old Semantic Kernel code (deprecated) - removed
                            #         for idx, product in enumerate(similar_products[:num_recommendations], 1):
                            #             st.markdown(f"### {idx}. {product.get('product_name', 'Unknown')}")
                            #             col1, col2, col3 = st.columns(3)
                            #             with col1:
                            #                 st.markdown(f"**Category:** {product.get('category', 'N/A')}")
                            #                 st.markdown(f"**Brand:** {product.get('brand', 'N/A')}")
                            #             with col2:
                            #                 st.markdown(f"**Price:** ${float(product.get('price', 0)):.2f}")
                            #                 st.markdown(f"**Stock:** {int(product.get('stock_quantity', 0))}")
                            #             with col3:
                            #                 if 'similarity_score' in product:
                            #                     st.markdown(f"**Similarity:** {float(product['similarity_score']):.2%}")
                            #             st.markdown(f"{product.get('description', 'No description available')}")
                            #             st.markdown("---")
                            #     else:
                            #         st.info("No similar products found.")
                            # except json.JSONDecodeError:
                            #     # Not JSON, probably an error message or plain text
                            #     st.warning(result.value)
                        
                        except Exception as e:
                            st.error(f"Error generating recommendations: {e}")
            else:
                st.info("No products available in the catalog")
        
        except Exception as e:
            st.error(f"Error loading products: {e}")
    
    # Tab 3: Product Catalog
    with tab3:
        st.subheader("📦 Product Catalog")
        
        try:
            cosmos_conn = st.session_state.get('cosmos_conn')
            
            if not cosmos_conn:
                st.error("CosmosDB connection not initialized. Please restart the app.")
            else:
                # Filters
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    # Get unique categories from CosmosDB
                    all_products = cosmos_conn.get_all_products(limit=1000)
                    categories = ['All']
                    if all_products:
                        unique_categories = set(p.get('category', 'N/A') for p in all_products if p.get('category'))
                        categories.extend(sorted(unique_categories))
                    selected_category = st.selectbox("Category", categories)
                
                with col2:
                    price_range = st.slider(
                        "Price Range ($)",
                        min_value=0.0,
                        max_value=5000.0,
                        value=(0.0, 5000.0),
                        step=50.0
                    )
                
                with col3:
                    sort_by = st.selectbox(
                        "Sort by",
                        ["Name", "Price (Low to High)", "Price (High to Low)"]
                    )
                
                # Fetch products from CosmosDB
                products = cosmos_conn.get_all_products(limit=1000)
                
                if products:
                    # Convert to DataFrame for easier filtering
                    products_df = pd.DataFrame(products)
                    
                    # Apply filters
                    if 'price' in products_df.columns:
                        products_df = products_df[
                            (products_df['price'] >= price_range[0]) & 
                            (products_df['price'] <= price_range[1])
                        ]
                    
                    if selected_category != 'All' and 'category' in products_df.columns:
                        products_df = products_df[products_df['category'] == selected_category]
                    
                    # Apply sorting
                    if not products_df.empty:
                        if sort_by == "Name" and 'name' in products_df.columns:
                            products_df = products_df.sort_values('name')
                        elif sort_by == "Price (Low to High)" and 'price' in products_df.columns:
                            products_df = products_df.sort_values('price', ascending=True)
                        elif sort_by == "Price (High to Low)" and 'price' in products_df.columns:
                            products_df = products_df.sort_values('price', ascending=False)
                    
                    if not products_df.empty:
                        st.success(f"Found {len(products_df)} products")
                        
                        # Display as cards
                        for idx in range(0, len(products_df), 3):
                            cols = st.columns(3)
                            for i, col in enumerate(cols):
                                if idx + i < len(products_df):
                                    row = products_df.iloc[idx + i]
                                    with col:
                                        product_name = row.get('name', 'Unknown Product')
                                        category = row.get('category', 'N/A')
                                        brand = row.get('brand', 'N/A')
                                        price = row.get('price', 0)
                                        stock = row.get('stockQuantity', 0)
                                        
                                        st.markdown(f"""
                                        <div style="border: 1px solid #ddd; padding: 1rem; border-radius: 0.5rem; margin-bottom: 1rem;">
                                            <h4>{product_name}</h4>
                                            <p><strong>Category:</strong> {category}</p>
                                            <p><strong>Brand:</strong> {brand}</p>
                                            <p><strong>Price:</strong> ${float(price):.2f}</p>
                                            <p><strong>Stock:</strong> {int(stock)}</p>
                                        </div>
                                        """, unsafe_allow_html=True)
                    else:
                        st.info("No products found matching your criteria")
                else:
                    st.info("No products available in CosmosDB catalog")
        
        except Exception as e:
            st.error(f"Error loading product catalog: {e}")


if __name__ == "__main__":
    main()
