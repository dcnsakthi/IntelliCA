"""
Customer Analytics Page
Displays 360° customer view with AI-powered insights
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import asyncio
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

st.set_page_config(
    page_title="Customer Analytics",
    page_icon="👥",
    layout="wide"
)

# Custom CSS
st.markdown("""
<style>
    .metric-card {
        background-color: #f0f2f6;
        padding: 1.5rem;
        border-radius: 0.5rem;
        border-left: 4px solid #0078D4;
    }
</style>
""", unsafe_allow_html=True)


def main():
    st.title("👥 Customer Analytics")
    st.markdown("360° view of customer data with AI-powered insights")
    st.markdown("---")
    
    # Check if connectors are initialized
    if 'sql_conn' not in st.session_state:
        st.error("Database connections not initialized. Please go back to the home page.")
        st.stop()
    
    sql_conn = st.session_state.sql_conn
    cosmos_conn = st.session_state.cosmos_conn
    agent = st.session_state.agent
    
    # Tabs
    tab1, tab2, tab3, tab4 = st.tabs([
        "📊 Overview", 
        "🔍 Customer Search", 
        "⚠️ Churn Risk", 
        "📈 Segmentation"
    ])
    
    # Tab 1: Overview
    with tab1:
        st.subheader("📊 Customer Overview")
        
        try:
            # Get all statistics in a single optimized query
            stats_query = """
            SELECT 
                (SELECT COUNT(*) FROM ca.Customers WHERE IsActive = 1) as total_customers,
                (SELECT SUM(TotalLifetimeValue) FROM ca.Customers WHERE IsActive = 1) as total_revenue,
                (SELECT AVG(TotalLifetimeValue) FROM ca.Customers WHERE IsActive = 1) as avg_ltv,
                (SELECT COUNT(*) FROM ca.Orders WHERE OrderDate >= DATEADD(day, -30, GETDATE())) as recent_orders
            """
            stats_df = sql_conn.execute_query(stats_query)
            
            # Display metrics
            col1, col2, col3, col4 = st.columns(4)
            
            if not stats_df.empty:
                row = stats_df.iloc[0]
                
                with col1:
                    st.metric(
                        "Total Customers",
                        f"{int(row['total_customers']):,}" if row['total_customers'] else "0"
                    )
                
                with col2:
                    st.metric(
                        "Total Revenue",
                        f"${float(row['total_revenue']):,.2f}" if row['total_revenue'] else "$0"
                    )
                
                with col3:
                    st.metric(
                        "Avg Lifetime Value",
                        f"${float(row['avg_ltv']):,.2f}" if row['avg_ltv'] else "$0"
                    )
                
                with col4:
                    st.metric(
                        "Orders (30 days)",
                        f"{int(row['recent_orders']):,}" if row['recent_orders'] else "0"
                    )
            else:
                # Show zeros if no data
                with col1:
                    st.metric("Total Customers", "0")
                with col2:
                    st.metric("Total Revenue", "$0")
                with col3:
                    st.metric("Avg Lifetime Value", "$0")
                with col4:
                    st.metric("Orders (30 days)", "0")
            
            st.markdown("---")
            
            # Segment distribution
            st.subheader("Customer Segment Distribution")
            
            segments_df = sql_conn.get_customer_segments_distribution()
            
            if not segments_df.empty:
                col1, col2 = st.columns(2)
                
                with col1:
                    # Pie chart
                    fig = px.pie(
                        segments_df,
                        values='CustomerCount',
                        names='CustomerSegment',
                        title='Customers by Segment',
                        color_discrete_sequence=px.colors.qualitative.Set3
                    )
                    st.plotly_chart(fig, use_container_width=True)
                
                with col2:
                    # Bar chart
                    fig = px.bar(
                        segments_df,
                        x='CustomerSegment',
                        y='TotalValue',
                        title='Revenue by Segment',
                        color='CustomerSegment',
                        color_discrete_sequence=px.colors.qualitative.Set3
                    )
                    fig.update_layout(showlegend=False)
                    st.plotly_chart(fig, use_container_width=True)
                
                # Data table
                st.dataframe(segments_df, use_container_width=True)
            
            # Top customers
            st.markdown("---")
            st.subheader("🏆 Top Customers by Lifetime Value")
            
            top_customers_df = sql_conn.get_top_customers(limit=10)
            
            if not top_customers_df.empty:
                st.dataframe(
                    top_customers_df[[
                        'CustomerID', 'FirstName', 'LastName', 
                        'Email', 'CustomerSegment', 'TotalLifetimeValue'
                    ]],
                    use_container_width=True
                )
            else:
                st.info("No customer data available")
        
        except Exception as e:
            st.error(f"Error loading customer overview: {e}")
    
    # Tab 2: Customer Search
    with tab2:
        st.subheader("🔍 Search Customers")
        
        col1, col2 = st.columns([3, 1])
        
        with col1:
            search_term = st.text_input("Search by name or email", placeholder="Enter customer name or email...")
        
        with col2:
            st.markdown("<br>", unsafe_allow_html=True)
            search_button = st.button("🔍 Search", use_container_width=True)
        
        if search_button and search_term:
            try:
                results_df = sql_conn.search_customers(search_term)
                
                if not results_df.empty:
                    st.success(f"Found {len(results_df)} customer(s)")
                    
                    # Display results
                    for idx, row in results_df.iterrows():
                        with st.expander(f"👤 {row['FirstName']} {row['LastName']} - {row['Email']}"):
                            col1, col2, col3 = st.columns(3)
                            
                            with col1:
                                st.metric("Customer ID", row['CustomerID'])
                                st.metric("Segment", row['CustomerSegment'])
                            
                            with col2:
                                st.metric("Lifetime Value", f"${float(row['TotalLifetimeValue']):,.2f}")
                            
                            with col3:
                                if st.button(f"View Details", key=f"view_{row['CustomerID']}"):
                                    # Convert numpy.int64 to Python int
                                    st.session_state.selected_customer = int(row['CustomerID'])
                else:
                    st.warning("No customers found matching your search")
            
            except Exception as e:
                st.error(f"Error searching customers: {e}")
        
        # Customer detail view
        if 'selected_customer' in st.session_state:
            st.markdown("---")
            st.subheader("📋 Customer Details")
            
            customer_id = int(st.session_state.selected_customer)
            
            try:
                # Get customer 360 view
                customer_df = sql_conn.get_customer_360_view(customer_id)
                
                if not customer_df.empty:
                    customer = customer_df.iloc[0]
                    
                    # Basic info
                    col1, col2, col3, col4 = st.columns(4)
                    
                    with col1:
                        st.metric("Total Orders", int(customer['TotalOrders']))
                    
                    with col2:
                        st.metric("Total Spent", f"${float(customer['TotalSpent'] if customer['TotalSpent'] else 0):,.2f}")
                    
                    with col3:
                        st.metric("Avg Order Value", f"${float(customer['AvgOrderValue'] if customer['AvgOrderValue'] else 0):,.2f}")
                    
                    with col4:
                        days_since = int(customer['DaysSinceLastOrder']) if customer['DaysSinceLastOrder'] else 999
                        st.metric("Days Since Last Order", days_since)
                    
                    # Order history
                    st.markdown("### 📦 Order History")
                    orders_df = sql_conn.get_customer_orders(customer_id)
                    
                    if not orders_df.empty:
                        st.dataframe(orders_df, use_container_width=True)
                    else:
                        st.info("No order history available")
                    
                    # AI Insights
                    st.markdown("### 🤖 AI-Generated Insights")
                    
                    if st.button("Generate Insights", key="generate_insights"):
                        with st.spinner("Analyzing customer behavior..."):
                            try:
                                # TODO: Update to use Agent Framework instead of Semantic Kernel
                                st.info("AI insights feature is being updated to use the new Agent Framework. Coming soon!")
                                
                                # Old Semantic Kernel code (deprecated):
                                # insights_function = kernel.get_function(
                                #     plugin_name="CustomerInsights",
                                #     function_name="analyze_customer_behavior"
                                # )
                                # result = asyncio.run(insights_function.invoke(
                                #     kernel=kernel,
                                #     customer_id=customer_id
                                # ))
                                # st.success("Analysis complete!")
                                # st.markdown(result.value)
                            
                            except Exception as e:
                                st.error(f"Error generating insights: {e}")
            
            except Exception as e:
                st.error(f"Error loading customer details: {e}")
    
    # Tab 3: Churn Risk
    with tab3:
        st.subheader("⚠️ Churn Risk Analysis")
        
        col1, col2 = st.columns([1, 3])
        
        with col1:
            risk_threshold = st.slider(
                "Risk Threshold",
                min_value=0.0,
                max_value=100.0,
                value=70.0,
                step=5.0
            )
        
        try:
            churn_df = sql_conn.get_churn_risk_customers(risk_threshold)
            
            if not churn_df.empty:
                st.warning(f"Found {len(churn_df)} customers at risk of churning")
                
                # Display at-risk customers
                st.dataframe(
                    churn_df[[
                        'CustomerID', 'FirstName', 'LastName', 'Email',
                        'CustomerSegment', 'TotalLifetimeValue', 'ChurnRiskScore'
                    ]],
                    use_container_width=True
                )
                
                # Visualization
                fig = px.scatter(
                    churn_df,
                    x='TotalLifetimeValue',
                    y='ChurnRiskScore',
                    size='TotalLifetimeValue',
                    color='CustomerSegment',
                    hover_data=['FirstName', 'LastName', 'Email'],
                    title='Churn Risk vs Lifetime Value',
                    labels={
                        'TotalLifetimeValue': 'Lifetime Value ($)',
                        'ChurnRiskScore': 'Churn Risk Score'
                    }
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.success(f"No customers found with churn risk >= {risk_threshold}%")
        
        except Exception as e:
            st.error(f"Error loading churn risk data: {e}")
    
    # Tab 4: Segmentation
    with tab4:
        st.subheader("📈 Customer Segmentation")
        
        try:
            segments_df = sql_conn.get_customer_segments_distribution()
            
            if not segments_df.empty:
                # Summary table
                st.dataframe(segments_df, use_container_width=True)
                
                # Update segmentation
                st.markdown("---")
                if st.button("🔄 Update Customer Segmentation"):
                    with st.spinner("Updating customer segments..."):
                        try:
                            sql_conn.update_customer_segmentation()
                            st.success("Customer segmentation updated successfully!")
                            st.rerun()
                        except Exception as e:
                            st.error(f"Error updating segmentation: {e}")
        
        except Exception as e:
            st.error(f"Error loading segmentation data: {e}")


if __name__ == "__main__":
    # Streamlit doesn't support top-level async, so we handle it with asyncio.run() where needed
    main()
