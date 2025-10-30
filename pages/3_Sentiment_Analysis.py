"""
Sentiment Analysis Page
Analyze product reviews and customer sentiment
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import sys
import os
import asyncio

# Add parent directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

st.set_page_config(
    page_title="Sentiment Analysis",
    page_icon="💬",
    layout="wide"
)

def main():
    st.title("💬 Sentiment Analysis")
    st.markdown("Analyze product reviews and customer sentiment trends")
    st.markdown("---")
    
    # Check if connectors are initialized
    if 'sql_conn' not in st.session_state:
        st.error("Database connections not initialized. Please go back to the home page.")
        st.stop()
    
    sql_conn = st.session_state.sql_conn
    cosmos_conn = st.session_state.get('cosmos_conn')
    agent = st.session_state.agent
    
    # Display message about reviews data
    st.info("💬 **Reviews Data**: Product reviews are stored in CosmosDB NoSQL. Sample review data needs to be generated.")
    
    # Tabs
    tab1, tab2, tab3 = st.tabs([
        "📊 Overview", 
        "🔍 Review Analysis",
        "📈 Trends"
    ])
    
    # Tab 1: Overview
    with tab1:
        st.subheader("📊 Sentiment Overview")
        
        try:
            # Check if we have reviews in CosmosDB
            if cosmos_conn:
                # Fetch all reviews (don't filter by type - reviews might not have this field)
                reviews_query = "SELECT c.sentimentLabel, c.rating FROM c"
                reviews_results = list(cosmos_conn.reviews_container.query_items(
                    query=reviews_query,
                    enable_cross_partition_query=True
                ))
                
                if not reviews_results:
                    st.warning(f"📭 **No reviews available yet**")
                    st.markdown("""
                    ### To populate review data:
                    1. Create a script to generate sample reviews with sentiment analysis
                    2. Reviews should include: `product_id`, `rating`, `review_text`, `sentiment_label`, `sentiment_score`
                    3. Store reviews in CosmosDB `CAReviews` container
                    
                    For now, the sentiment analysis feature is ready but waiting for review data.
                    """)
                    sentiment_df = pd.DataFrame()  # Empty dataframe
                else:
                    # Convert to DataFrame and do aggregation in Python
                    reviews_df = pd.DataFrame(reviews_results)
                    
                    # Group by sentiment_label and aggregate
                    sentiment_df = reviews_df.groupby('sentimentLabel', as_index=False).agg(
                        count=('sentimentLabel', 'size'),  # Count rows
                        avg_rating=('rating', 'mean')  # Average rating
                    )
                    
                    # Rename sentimentLabel column to sentiment_label
                    sentiment_df = sentiment_df.rename(columns={'sentimentLabel': 'sentiment_label'})
                    sentiment_df['avg_rating'] = sentiment_df['avg_rating'].round(2)
            else:
                st.error("CosmosDB connection not available")
                sentiment_df = pd.DataFrame()
            
            if not sentiment_df.empty:
                col1, col2, col3 = st.columns(3)
                
                # Calculate metrics
                total_reviews = sentiment_df['count'].sum()
                positive_reviews = sentiment_df[sentiment_df['sentiment_label'] == 'positive']['count'].iloc[0] if 'positive' in sentiment_df['sentiment_label'].values else 0
                negative_reviews = sentiment_df[sentiment_df['sentiment_label'] == 'negative']['count'].iloc[0] if 'negative' in sentiment_df['sentiment_label'].values else 0
                
                with col1:
                    st.metric("Total Reviews", f"{int(total_reviews):,}")
                
                with col2:
                    positive_pct = (positive_reviews / total_reviews * 100) if total_reviews > 0 else 0
                    st.metric("Positive Reviews", f"{positive_pct:.1f}%")
                
                with col3:
                    negative_pct = (negative_reviews / total_reviews * 100) if total_reviews > 0 else 0
                    st.metric("Negative Reviews", f"{negative_pct:.1f}%")
                
                st.markdown("---")
                
                # Visualizations
                col1, col2 = st.columns(2)
                
                with col1:
                    # Pie chart
                    fig = px.pie(
                        sentiment_df,
                        values='count',
                        names='sentiment_label',
                        title='Sentiment Distribution',
                        color='sentiment_label',
                        color_discrete_map={
                            'positive': '#28a745',
                            'neutral': '#ffc107',
                            'negative': '#dc3545'
                        }
                    )
                    st.plotly_chart(fig, use_container_width=True)
                
                with col2:
                    # Bar chart
                    fig = px.bar(
                        sentiment_df,
                        x='sentiment_label',
                        y='avg_rating',
                        title='Average Rating by Sentiment',
                        color='sentiment_label',
                        color_discrete_map={
                            'positive': '#28a745',
                            'neutral': '#ffc107',
                            'negative': '#dc3545'
                        }
                    )
                    fig.update_layout(showlegend=False)
                    st.plotly_chart(fig, use_container_width=True)
                
                # Data table
                st.dataframe(sentiment_df, use_container_width=True)
            else:
                st.info("No review data available")
        
        except Exception as e:
            st.error(f"Error loading sentiment overview: {e}")
    
    # Tab 2: Review Analysis
    with tab2:
        st.subheader("🔍 Product Review Analysis")
        
        st.info("📭 **Review analysis will be available once review data is populated in CosmosDB.**")
        
        try:
            # Get products list
            products_df = sql_conn.execute_query("""
                SELECT TOP 100
                    p.ProductID as product_id, 
                    p.ProductName as product_name, 
                    p.Category as category
                FROM ca.Products p
                WHERE p.IsActive = 1
                ORDER BY p.ProductName
            """)
            
            if not products_df.empty:
                col1, col2 = st.columns([3, 1])
                
                with col1:
                    selected_product = st.selectbox(
                        "Select a product to analyze",
                        options=products_df['product_name'].tolist()
                    )
                
                if st.button("Analyze Reviews", use_container_width=True):
                    with st.spinner("Analyzing product reviews..."):
                        try:
                            # Get product ID (convert numpy.int64 to Python int)
                            product_id = int(products_df[
                                products_df['product_name'] == selected_product
                            ]['product_id'].iloc[0])
                            
                            # Get product SKU (assuming format PROD-XXX based on ProductID)
                            product_sku = f"PROD-{product_id:03d}"
                            
                            # Get reviews from CosmosDB (don't filter by type)
                            if cosmos_conn:
                                reviews_query = f"""
                                SELECT TOP 50
                                    c.id as review_id,
                                    c.rating,
                                    c.reviewText as review_text,
                                    c.sentimentLabel as sentiment_label,
                                    c.sentimentScore as sentiment_score,
                                    c.reviewDate as review_date
                                FROM c
                                WHERE c.productId = '{product_sku}'
                                ORDER BY c.reviewDate DESC
                                """
                                reviews_results = list(cosmos_conn.reviews_container.query_items(
                                    query=reviews_query,
                                    enable_cross_partition_query=True
                                ))
                                reviews_df = pd.DataFrame(reviews_results) if reviews_results else pd.DataFrame()
                            else:
                                st.error("CosmosDB connection not available")
                                reviews_df = pd.DataFrame()
                            
                            if not reviews_df.empty:
                                st.success(f"Found {len(reviews_df)} reviews")
                                
                                # Summary metrics
                                col1, col2, col3, col4 = st.columns(4)
                                
                                with col1:
                                    avg_rating = reviews_df['rating'].mean()
                                    st.metric("Average Rating", f"{avg_rating:.2f}")
                                
                                with col2:
                                    positive_count = len(reviews_df[reviews_df['sentiment_label'] == 'positive'])
                                    st.metric("Positive", positive_count)
                                
                                with col3:
                                    neutral_count = len(reviews_df[reviews_df['sentiment_label'] == 'neutral'])
                                    st.metric("Neutral", neutral_count)
                                
                                with col4:
                                    negative_count = len(reviews_df[reviews_df['sentiment_label'] == 'negative'])
                                    st.metric("Negative", negative_count)
                                
                                st.markdown("---")
                                
                                # AI-powered sentiment analysis
                                st.markdown("### 🤖 AI Sentiment Analysis")
                                
                                if st.button("Generate Sentiment Insights", key="generate_sentiment"):
                                    with st.spinner("Analyzing sentiment patterns..."):
                                        try:
                                            # TODO: Update to use Agent Framework instead of Semantic Kernel
                                            st.info("Sentiment insights feature is being updated to use the new Agent Framework. Coming soon!")
                                            
                                            # Old Semantic Kernel code (deprecated):
                                            # sentiment_function = kernel.get_function(
                                            #     plugin_name="Sentiment",
                                            #     function_name="analyze_sentiment_trend"
                                            # )
                                            # result = asyncio.run(sentiment_function.invoke(
                                            #     kernel=kernel,
                                            #     product_id=int(product_id)
                                            # ))
                                            # st.success("Analysis complete!")
                                            # st.info(result.value)
                                        
                                        except Exception as e:
                                            st.error(f"Error generating insights: {e}")
                                
                                # Display reviews
                                st.markdown("### 📝 Recent Reviews")
                                
                                for idx, row in reviews_df.head(10).iterrows():
                                    sentiment_color = {
                                        'positive': '🟢',
                                        'neutral': '🟡',
                                        'negative': '🔴'
                                    }.get(row['sentiment_label'], '⚪')
                                    
                                    with st.expander(f"{sentiment_color} Rating: {int(row['rating'])}/5 - {row['review_date']}"):
                                        st.markdown(f"**Review:** {row['review_text']}")
                                        st.markdown(f"**Sentiment:** {row['sentiment_label']} (Score: {float(row['sentiment_score']):.2f})")
                            else:
                                st.info("No reviews found for this product")
                        
                        except Exception as e:
                            st.error(f"Error analyzing reviews: {e}")
            else:
                st.info("No products with reviews available")
        
        except Exception as e:
            st.error(f"Error loading products: {e}")
    
    # Tab 3: Trends
    with tab3:
        st.subheader("📈 Sentiment Trends")
        
        st.info("📭 **Sentiment trends will be available once review data is populated in CosmosDB.**")
        st.markdown("""
        ### What will be shown here:
        - **Sentiment trends over time** - Track how customer sentiment changes over days/weeks/months
        - **Rating distribution** - Visualize rating patterns across products  
        - **Category sentiment** - Compare sentiment across different product categories
        - **Trending topics** - AI-powered analysis of common themes in reviews
        
        Once review data is available, you'll see interactive charts and insights here.
        """)


if __name__ == "__main__":
    main()
