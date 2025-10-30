"""Recommendation Engine Plugin for Agent Framework."""
from agent_framework import ai_function
from typing import Annotated
import logging
import asyncio

logger = logging.getLogger(__name__)


class RecommendationEnginePlugin:
    """Plugin for product recommendations using semantic search."""
    
    def __init__(self, cosmos_connector, embedding_service):
        """
        Initialize the plugin.
        
        Args:
            cosmos_connector: CosmosDB NoSQL connector with vector search
            embedding_service: Embedding service for generating vectors
        """
        self.cosmos = cosmos_connector
        self.embedding_service = embedding_service
    
    @ai_function(
        name="search_products_semantic",
        description="Search for products using natural language semantic search"
    )
    async def search_products_semantic(
        self,
        query: Annotated[str, "Natural language search query for products"]
    ) -> str:
        """
        Semantic search for products.
        
        Args:
            query: Natural language search query
            
        Returns:
            JSON string with matching products
        """
        try:
            # Generate embedding for query
            embedding_result = await self.embedding_service.generate_embeddings([query])
            query_embedding = embedding_result[0]
            
            # Search using vector similarity in CosmosDB
            results = self.cosmos.search_products_by_embedding(
                query_embedding=query_embedding,
                limit=10,
                similarity_threshold=0.6
            )
            
            if not results:
                return f"No products found matching: {query}"
            
            # Convert to serializable format
            for product in results:
                # Remove embedding vector from output (too large)
                if 'descriptionEmbedding' in product:
                    del product['descriptionEmbedding']
                for key, value in product.items():
                    if not isinstance(value, (str, int, float, bool, type(None), dict, list)):
                        product[key] = float(value) if hasattr(value, '__float__') else str(value)
            
            import json
            return json.dumps(results, indent=2)
            
        except Exception as e:
            logger.error(f"Error in semantic product search: {e}")
            return f"Error: {str(e)}"
    
    @ai_function(
        name="find_similar_products",
        description="Find products similar to a given product"
    )
    def find_similar_products(
        self,
        product_id: Annotated[int, "Product ID to find similar products for"]
    ) -> str:
        """
        Find similar products using vector similarity.
        
        Args:
            product_id: Source product ID
            
        Returns:
            JSON string with similar products
        """
        try:
            results = self.cosmos.find_similar_products(product_id, limit=5)
            
            if not results:
                return f"No similar products found for product {product_id}"
            
            # Convert to serializable format
            for product in results:
                # Remove embedding vector
                if 'descriptionEmbedding' in product:
                    del product['descriptionEmbedding']
                for key, value in product.items():
                    if not isinstance(value, (str, int, float, bool, type(None), dict, list)):
                        product[key] = float(value) if hasattr(value, '__float__') else str(value)
            
            import json
            return json.dumps(results, indent=2)
            
        except Exception as e:
            logger.error(f"Error finding similar products: {e}")
            return f"Error: {str(e)}"
    
    @ai_function(
        name="get_product_recommendations",
        description="Get personalized product recommendations for a customer based on their history"
    )
    def get_product_recommendations(
        self,
        customer_id: Annotated[int, "Customer ID to generate recommendations for"],
        limit: Annotated[int, "Number of recommendations to return"] = 5
    ) -> str:
        """
        Generate personalized product recommendations.
        
        Args:
            customer_id: Customer ID
            limit: Number of recommendations
            
        Returns:
            JSON string with recommended products
        """
        try:
            # This is a simplified recommendation
            # In production, you'd use more sophisticated ML models
            
            # For now, get top-rated products from categories the customer bought from
            top_products = self.cosmos.get_top_rated_products(
                category=None,
                min_reviews=3,
                limit=limit
            )
            
            if not top_products:
                return f"No recommendations available for customer {customer_id}"
            
            # Convert to serializable format
            for rec in top_products:
                # Remove embedding vector
                if 'descriptionEmbedding' in rec:
                    del rec['descriptionEmbedding']
                for key, value in rec.items():
                    if not isinstance(value, (str, int, float, bool, type(None), dict, list)):
                        rec[key] = float(value) if hasattr(value, '__float__') else str(value)
            
            import json
            return json.dumps(top_products, indent=2)
            
        except Exception as e:
            logger.error(f"Error generating recommendations: {e}")
            return f"Error: {str(e)}"
    
    @ai_function(
        name="get_product_by_id",
        description="Get detailed information about a specific product"
    )
    def get_product_by_id(
        self,
        product_id: Annotated[int, "Product ID to retrieve"]
    ) -> str:
        """
        Get product details by ID.
        
        Args:
            product_id: Product ID
            
        Returns:
            JSON string with product details
        """
        try:
            product = self.cosmos.get_product(product_id)
            
            if not product:
                return f"Product {product_id} not found"
            
            # Remove embedding vector from output (too large)
            if 'descriptionEmbedding' in product:
                del product['descriptionEmbedding']
            
            # Convert to serializable format
            for key, value in product.items():
                if hasattr(value, 'isoformat'):
                    product[key] = value.isoformat()
                elif not isinstance(value, (str, int, float, bool, type(None), dict, list)):
                    product[key] = str(value)
            
            import json
            return json.dumps(product, indent=2)
            
        except Exception as e:
            logger.error(f"Error getting product details: {e}")
            return f"Error: {str(e)}"
    
    @ai_function(
        name="get_top_rated_products",
        description="Get top-rated products by customer reviews"
    )
    def get_top_rated_products(
        self,
        category: Annotated[str, "Product category (optional)"] = None,
        limit: Annotated[int, "Number of products to return"] = 10
    ) -> str:
        """
        Get top-rated products.
        
        Args:
            category: Optional category filter
            limit: Number of products to return
            
        Returns:
            JSON string with top-rated products
        """
        try:
            products = self.cosmos.get_top_rated_products(
                category=category if category and category != "None" else None,
                min_reviews=3,
                limit=limit
            )
            
            if not products:
                return "No top-rated products found"
            
            # Remove embedding vectors
            for product in products:
                if 'descriptionEmbedding' in product:
                    del product['descriptionEmbedding']
                
                # Convert to serializable format
                for key, value in product.items():
                    if not isinstance(value, (str, int, float, bool, type(None), dict, list)):
                        product[key] = float(value) if hasattr(value, '__float__') else str(value)
            
            import json
            return json.dumps(products, indent=2)
            
        except Exception as e:
            logger.error(f"Error getting top-rated products: {e}")
            return f"Error: {str(e)}"
    
    @ai_function(
        name="get_product_categories",
        description="Get all available product categories"
    )
    def get_product_categories(self) -> str:
        """
        Get all product categories.
        
        Returns:
            JSON string with categories
        """
        try:
            # Query all products and extract unique categories
            products = self.cosmos.query_items(
                query="SELECT DISTINCT VALUE c.category FROM c WHERE c.category != null",
                container="products"
            )
            
            if not products:
                return "No categories found"
            
            categories_list = [{"category": cat} for cat in products]
            
            import json
            return json.dumps(categories_list, indent=2)
            
        except Exception as e:
            logger.error(f"Error getting categories: {e}")
            return f"Error: {str(e)}"
    
    @ai_function(
        name="search_products_by_text",
        description="Search for products using keyword/text search"
    )
    def search_products_by_text(
        self,
        search_term: Annotated[str, "Search term or keyword"]
    ) -> str:
        """
        Text-based product search.
        
        Args:
            search_term: Search keyword
            
        Returns:
            JSON string with matching products
        """
        try:
            products = self.cosmos.search_products_text(search_term)
            
            if not products:
                return f"No products found matching: {search_term}"
            
            # Convert to serializable format
            for product in products:
                # Remove embedding vector
                if 'descriptionEmbedding' in product:
                    del product['descriptionEmbedding']
                for key, value in product.items():
                    if not isinstance(value, (str, int, float, bool, type(None), dict, list)):
                        product[key] = float(value) if hasattr(value, '__float__') else str(value)
            
            import json
            return json.dumps(products, indent=2)
            
        except Exception as e:
            logger.error(f"Error in text search: {e}")
            return f"Error: {str(e)}"
