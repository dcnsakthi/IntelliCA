"""Customer Insights Plugin for Agent Framework."""
from agent_framework import ai_function
from typing import Annotated
import logging

logger = logging.getLogger(__name__)


class CustomerInsightsPlugin:
    """Plugin for generating customer insights and analytics."""
    
    def __init__(self, sql_connector, cosmos_connector):
        """
        Initialize the plugin with database connectors.
        
        Args:
            sql_connector: Fabric SQL connector instance
            cosmos_connector: Fabric CosmosDB connector instance
        """
        self.sql = sql_connector
        self.cosmos = cosmos_connector
    
    @ai_function(
        name="get_customer_profile",
        description="Get comprehensive customer profile with purchase history and behavior"
    )
    def get_customer_profile(
        self,
        customer_id: Annotated[int, "The customer ID to retrieve"]
    ) -> str:
        """
        Get detailed customer profile information.
        
        Args:
            customer_id: Customer ID
            
        Returns:
            JSON string with customer profile
        """
        try:
            # Get customer 360 view from SQL
            customer_df = self.sql.get_customer_360_view(customer_id)
            
            if customer_df.empty:
                return f"Customer {customer_id} not found"
            
            customer = customer_df.iloc[0].to_dict()
            
            # Get recent sessions from Cosmos DB
            sessions_df = self.cosmos.get_customer_sessions(
                str(customer_id),
                days=30
            )
            
            result = {
                "customerId": customer_id,
                "name": f"{customer.get('FirstName', '')} {customer.get('LastName', '')}",
                "email": customer.get('Email', ''),
                "segment": customer.get('CustomerSegment', ''),
                "lifetimeValue": float(customer.get('TotalLifetimeValue', 0)),
                "totalOrders": int(customer.get('TotalOrders', 0)),
                "avgOrderValue": float(customer.get('AvgOrderValue', 0)),
                "daysSinceLastOrder": int(customer.get('DaysSinceLastOrder', 0)),
                "churnRisk": float(customer.get('ChurnRiskScore', 0)),
                "recentSessions": len(sessions_df) if not sessions_df.empty else 0
            }
            
            import json
            return json.dumps(result, indent=2)
            
        except Exception as e:
            logger.error(f"Error getting customer profile: {e}")
            return f"Error: {str(e)}"
    
    @ai_function(
        name="get_top_customers",
        description="Get list of top customers by lifetime value"
    )
    def get_top_customers(
        self,
        limit: Annotated[int, "Number of top customers to return"] = 10
    ) -> str:
        """
        Get top customers by lifetime value.
        
        Args:
            limit: Number of customers to return
            
        Returns:
            JSON string with top customers
        """
        try:
            customers_df = self.sql.get_top_customers(limit)
            
            if customers_df.empty:
                return "No customers found"
            
            customers_list = customers_df.to_dict(orient='records')
            
            # Convert to serializable format
            for customer in customers_list:
                for key, value in customer.items():
                    if hasattr(value, 'isoformat'):
                        customer[key] = value.isoformat()
                    elif not isinstance(value, (str, int, float, bool, type(None))):
                        customer[key] = str(value)
            
            import json
            return json.dumps(customers_list, indent=2)
            
        except Exception as e:
            logger.error(f"Error getting top customers: {e}")
            return f"Error: {str(e)}"
    
    @ai_function(
        name="analyze_customer_behavior",
        description="Analyze customer behavior patterns and generate insights"
    )
    def analyze_customer_behavior(
        self,
        customer_id: Annotated[int, "The customer ID to analyze"]
    ) -> str:
        """
        Analyze customer behavior and provide insights.
        
        Args:
            customer_id: Customer ID
            
        Returns:
            Analysis summary as string
        """
        try:
            # Get customer data
            customer_df = self.sql.get_customer_360_view(customer_id)
            
            if customer_df.empty:
                return f"Customer {customer_id} not found"
            
            customer = customer_df.iloc[0]
            
            # Get order history
            orders_df = self.sql.get_customer_orders(customer_id)
            
            # Generate insights
            insights = []
            
            # Purchase frequency insight
            total_orders = int(customer.get('TotalOrders', 0))
            if total_orders > 10:
                insights.append(f"Frequent buyer with {total_orders} orders")
            elif total_orders > 5:
                insights.append(f"Regular customer with {total_orders} orders")
            else:
                insights.append(f"Occasional buyer with {total_orders} orders")
            
            # Value insight
            ltv = float(customer.get('TotalLifetimeValue', 0))
            avg_order = float(customer.get('AvgOrderValue', 0))
            insights.append(f"Lifetime value: ${ltv:,.2f}")
            insights.append(f"Average order value: ${avg_order:,.2f}")
            
            # Recency insight
            days_since_last = int(customer.get('DaysSinceLastOrder', 999))
            if days_since_last < 30:
                insights.append("Recently active customer")
            elif days_since_last < 90:
                insights.append("Customer may need re-engagement")
            else:
                insights.append("At-risk customer - high churn probability")
            
            # Segment insight
            segment = customer.get('CustomerSegment', 'Unknown')
            insights.append(f"Customer segment: {segment}")
            
            # Churn risk
            churn_risk = float(customer.get('ChurnRiskScore', 0))
            if churn_risk > 70:
                insights.append(f"⚠️ HIGH CHURN RISK ({churn_risk:.1f}%)")
            elif churn_risk > 40:
                insights.append(f"⚡ Moderate churn risk ({churn_risk:.1f}%)")
            
            return "\n".join(insights)
            
        except Exception as e:
            logger.error(f"Error analyzing customer behavior: {e}")
            return f"Error: {str(e)}"
    
    @ai_function(
        name="identify_churn_risks",
        description="Identify customers at risk of churning"
    )
    def identify_churn_risks(
        self,
        threshold: Annotated[float, "Churn risk score threshold (0-100)"] = 70.0
    ) -> str:
        """
        Identify customers with high churn risk.
        
        Args:
            threshold: Minimum churn risk score
            
        Returns:
            JSON string with at-risk customers
        """
        try:
            customers_df = self.sql.get_churn_risk_customers(threshold)
            
            if customers_df.empty:
                return f"No customers found with churn risk >= {threshold}"
            
            customers_list = customers_df.to_dict(orient='records')
            
            # Convert to serializable format
            for customer in customers_list:
                for key, value in customer.items():
                    if hasattr(value, 'isoformat'):
                        customer[key] = value.isoformat()
                    elif not isinstance(value, (str, int, float, bool, type(None))):
                        customer[key] = str(value)
            
            import json
            return json.dumps(customers_list, indent=2)
            
        except Exception as e:
            logger.error(f"Error identifying churn risks: {e}")
            return f"Error: {str(e)}"
    
    @ai_function(
        name="get_customer_orders",
        description="Get order history for a customer"
    )
    def get_customer_orders(
        self,
        customer_id: Annotated[int, "The customer ID"]
    ) -> str:
        """
        Get order history for a customer.
        
        Args:
            customer_id: Customer ID
            
        Returns:
            JSON string with order history
        """
        try:
            orders_df = self.sql.get_customer_orders(customer_id)
            
            if orders_df.empty:
                return f"No orders found for customer {customer_id}"
            
            orders_list = orders_df.to_dict(orient='records')
            
            # Convert to serializable format
            for order in orders_list:
                for key, value in order.items():
                    if hasattr(value, 'isoformat'):
                        order[key] = value.isoformat()
                    elif not isinstance(value, (str, int, float, bool, type(None))):
                        order[key] = str(value)
            
            import json
            return json.dumps(orders_list, indent=2)
            
        except Exception as e:
            logger.error(f"Error getting customer orders: {e}")
            return f"Error: {str(e)}"
    
    @ai_function(
        name="get_segment_distribution",
        description="Get distribution of customers across segments"
    )
    def get_segment_distribution(self) -> str:
        """
        Get customer segment distribution.
        
        Returns:
            JSON string with segment statistics
        """
        try:
            segments_df = self.sql.get_customer_segments_distribution()
            
            if segments_df.empty:
                return "No segment data available"
            
            segments_list = segments_df.to_dict(orient='records')
            
            # Convert to serializable format
            for segment in segments_list:
                for key, value in segment.items():
                    if not isinstance(value, (str, int, float, bool, type(None))):
                        segment[key] = float(value) if hasattr(value, '__float__') else str(value)
            
            import json
            return json.dumps(segments_list, indent=2)
            
        except Exception as e:
            logger.error(f"Error getting segment distribution: {e}")
            return f"Error: {str(e)}"
