"""Microsoft Fabric CosmosDB NoSQL connector for real-time session data, product catalog, and vector search."""
from azure.cosmos import CosmosClient, PartitionKey, exceptions
from azure.cosmos.container import ContainerProxy
from azure.cosmos.database import DatabaseProxy
from azure.identity import DefaultAzureCredential
import pandas as pd
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta
import logging
import json

logger = logging.getLogger(__name__)


class FabricCosmosDBConnector:
    """Connector for Microsoft Fabric CosmosDB NoSQL operations with vector search support using Entra ID authentication."""
    
    def __init__(
        self,
        endpoint: str,
        database_name: str,
        container_name: str = "Sessions",
        products_container_name: str = "Products",
        reviews_container_name: str = "Reviews"
    ):
        """
        Initialize Microsoft Fabric CosmosDB NoSQL connector with Entra ID authentication.
        
        Args:
            endpoint: CosmosDB account endpoint URL
            database_name: Database name
            container_name: Container name for sessions (default: Sessions)
            products_container_name: Container name for products (default: Products)
            reviews_container_name: Container name for reviews (default: Reviews)
            
        Note:
            Authentication uses Azure Entra ID (DefaultAzureCredential).
            No account key is required.
            Supports native vector search for product and review embeddings.
        """
        self.endpoint = endpoint
        self.database_name = database_name
        self.container_name = container_name
        self.products_container_name = products_container_name
        self.reviews_container_name = reviews_container_name
        
        # Initialize client with DefaultAzureCredential
        self.credential = DefaultAzureCredential()
        self.client = CosmosClient(endpoint, credential=self.credential)
        self.database: Optional[DatabaseProxy] = None
        self.container: Optional[ContainerProxy] = None
        self.products_container: Optional[ContainerProxy] = None
        self.reviews_container: Optional[ContainerProxy] = None
        
    def initialize(self) -> None:
        """Initialize database and containers (create if not exists) with vector indexing policies."""
        try:
            # Create database if not exists
            self.database = self.client.create_database_if_not_exists(
                id=self.database_name
            )
            logger.info(f"Using Microsoft Fabric CosmosDB database: {self.database_name}")
            
            # Create sessions container if not exists
            self.container = self.database.create_container_if_not_exists(
                id=self.container_name,
                partition_key=PartitionKey(path="/customerId"),
                offer_throughput=400
            )
            logger.info(f"Using Microsoft Fabric CosmosDB container: {self.container_name}")
            
            # Create products container with vector indexing policy
            products_indexing_policy = {
                "indexingMode": "consistent",
                "automatic": True,
                "includedPaths": [{"path": "/*"}],
                "excludedPaths": [{"path": "/\"_etag\"/?"}],
                "vectorIndexes": [
                    {
                        "path": "/descriptionEmbedding",
                        "type": "quantizedFlat"  # Optimized for similarity search
                    }
                ]
            }
            
            # Vector embedding policy for products
            vector_embedding_policy = {
                "vectorEmbeddings": [
                    {
                        "path": "/descriptionEmbedding",
                        "dataType": "float32",
                        "dimensions": 1536,  # OpenAI ada-002 embedding size
                        "distanceFunction": "cosine"
                    }
                ]
            }
            
            self.products_container = self.database.create_container_if_not_exists(
                id=self.products_container_name,
                partition_key=PartitionKey(path="/category"),
                indexing_policy=products_indexing_policy,
                vector_embedding_policy=vector_embedding_policy,
                offer_throughput=400
            )
            logger.info(f"Using Microsoft Fabric CosmosDB products container: {self.products_container_name}")
            
            # Create reviews container with vector indexing policy
            reviews_indexing_policy = {
                "indexingMode": "consistent",
                "automatic": True,
                "includedPaths": [{"path": "/*"}],
                "excludedPaths": [{"path": "/\"_etag\"/?"}],
                "vectorIndexes": [
                    {
                        "path": "/reviewEmbedding",
                        "type": "quantizedFlat"
                    }
                ]
            }
            
            # Vector embedding policy for reviews
            reviews_vector_policy = {
                "vectorEmbeddings": [
                    {
                        "path": "/reviewEmbedding",
                        "dataType": "float32",
                        "dimensions": 1536,
                        "distanceFunction": "cosine"
                    }
                ]
            }
            
            self.reviews_container = self.database.create_container_if_not_exists(
                id=self.reviews_container_name,
                partition_key=PartitionKey(path="/productId"),
                indexing_policy=reviews_indexing_policy,
                vector_embedding_policy=reviews_vector_policy,
                offer_throughput=400
            )
            logger.info(f"Using Microsoft Fabric CosmosDB reviews container: {self.reviews_container_name}")
            
        except exceptions.CosmosHttpResponseError as e:
            logger.error(f"Failed to initialize Microsoft Fabric CosmosDB: {e}")
            raise
    
    def _ensure_initialized(self) -> None:
        """Ensure database and container are initialized."""
        if not self.database or not self.container:
            self.initialize()
    
    # Session tracking methods
    
    def create_session(self, session_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create a new user session.
        
        Args:
            session_data: Dictionary containing session information
            
        Returns:
            Created session document
        """
        self._ensure_initialized()
        
        # Add metadata
        session_data['id'] = session_data.get('id', session_data['sessionId'])
        session_data['type'] = 'session'
        session_data['createdAt'] = session_data.get(
            'createdAt',
            datetime.utcnow().isoformat()
        )
        session_data['updatedAt'] = datetime.utcnow().isoformat()
        
        try:
            created_item = self.container.create_item(body=session_data)
            logger.info(f"Created session: {created_item['id']}")
            return created_item
        except exceptions.CosmosHttpResponseError as e:
            logger.error(f"Failed to create session: {e}")
            raise
    
    def get_session(self, session_id: str, customer_id: str) -> Optional[Dict[str, Any]]:
        """
        Get a session by ID.
        
        Args:
            session_id: Session ID
            customer_id: Customer ID (partition key)
            
        Returns:
            Session document or None
        """
        self._ensure_initialized()
        
        try:
            item = self.container.read_item(
                item=session_id,
                partition_key=customer_id
            )
            return item
        except exceptions.CosmosResourceNotFoundError:
            logger.warning(f"Session not found: {session_id}")
            return None
        except exceptions.CosmosHttpResponseError as e:
            logger.error(f"Failed to get session: {e}")
            raise
    
    def update_session(
        self,
        session_id: str,
        customer_id: str,
        updates: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Update a session document.
        
        Args:
            session_id: Session ID
            customer_id: Customer ID (partition key)
            updates: Dictionary of fields to update
            
        Returns:
            Updated session document
        """
        self._ensure_initialized()
        
        # Get existing session
        session = self.get_session(session_id, customer_id)
        if not session:
            raise ValueError(f"Session not found: {session_id}")
        
        # Apply updates
        session.update(updates)
        session['updatedAt'] = datetime.utcnow().isoformat()
        
        try:
            updated_item = self.container.replace_item(
                item=session_id,
                body=session
            )
            logger.info(f"Updated session: {session_id}")
            return updated_item
        except exceptions.CosmosHttpResponseError as e:
            logger.error(f"Failed to update session: {e}")
            raise
    
    def add_session_event(
        self,
        session_id: str,
        customer_id: str,
        event: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Add an event to a session.
        
        Args:
            session_id: Session ID
            customer_id: Customer ID (partition key)
            event: Event data to add
            
        Returns:
            Updated session document
        """
        event['timestamp'] = event.get('timestamp', datetime.utcnow().isoformat())
        
        session = self.get_session(session_id, customer_id)
        if not session:
            raise ValueError(f"Session not found: {session_id}")
        
        # Initialize events array if not exists
        if 'events' not in session:
            session['events'] = []
        
        session['events'].append(event)
        session['eventCount'] = len(session['events'])
        
        return self.update_session(session_id, customer_id, session)
    
    # Query methods (moved to helper methods section below for unified access across containers)
    
    def get_customer_sessions(
        self,
        customer_id: str,
        days: int = 30,
        limit: int = 100
    ) -> pd.DataFrame:
        """
        Get all sessions for a customer within specified days.
        
        Args:
            customer_id: Customer ID
            days: Number of days to look back
            limit: Maximum number of sessions
            
        Returns:
            DataFrame with session data
        """
        cutoff_date = (datetime.utcnow() - timedelta(days=days)).isoformat()
        
        query = """
        SELECT * FROM c 
        WHERE c.customerId = @customerId 
            AND c.type = 'session'
            AND c.createdAt >= @cutoffDate
        ORDER BY c.createdAt DESC
        OFFSET 0 LIMIT @limit
        """
        
        parameters = [
            {"name": "@customerId", "value": customer_id},
            {"name": "@cutoffDate", "value": cutoff_date},
            {"name": "@limit", "value": limit}
        ]
        
        items = self.query_items(query, parameters, enable_cross_partition=False)
        return pd.DataFrame(items) if items else pd.DataFrame()
    
    def get_active_sessions(self, minutes: int = 30) -> pd.DataFrame:
        """
        Get currently active sessions (last activity within specified minutes).
        
        Args:
            minutes: Minutes of inactivity threshold
            
        Returns:
            DataFrame with active sessions
        """
        cutoff_time = (datetime.utcnow() - timedelta(minutes=minutes)).isoformat()
        
        query = """
        SELECT * FROM c 
        WHERE c.type = 'session' 
            AND c.updatedAt >= @cutoffTime
            AND c.status = 'active'
        ORDER BY c.updatedAt DESC
        """
        
        parameters = [{"name": "@cutoffTime", "value": cutoff_time}]
        
        items = self.query_items(query, parameters)
        return pd.DataFrame(items) if items else pd.DataFrame()
    
    def get_sessions_by_date_range(
        self,
        start_date: datetime,
        end_date: datetime
    ) -> pd.DataFrame:
        """
        Get sessions within a date range.
        
        Args:
            start_date: Start datetime
            end_date: End datetime
            
        Returns:
            DataFrame with sessions
        """
        query = """
        SELECT * FROM c 
        WHERE c.type = 'session'
            AND c.createdAt >= @startDate 
            AND c.createdAt <= @endDate
        ORDER BY c.createdAt DESC
        """
        
        parameters = [
            {"name": "@startDate", "value": start_date.isoformat()},
            {"name": "@endDate", "value": end_date.isoformat()}
        ]
        
        items = self.query_items(query, parameters)
        return pd.DataFrame(items) if items else pd.DataFrame()
    
    # Click stream and event tracking
    
    def track_page_view(
        self,
        session_id: str,
        customer_id: str,
        page_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Track a page view event."""
        event = {
            'eventType': 'pageView',
            'page': page_data.get('page'),
            'url': page_data.get('url'),
            'referrer': page_data.get('referrer'),
            'duration': page_data.get('duration')
        }
        return self.add_session_event(session_id, customer_id, event)
    
    def track_product_view(
        self,
        session_id: str,
        customer_id: str,
        product_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Track a product view event."""
        event = {
            'eventType': 'productView',
            'productId': product_data.get('productId'),
            'productName': product_data.get('productName'),
            'category': product_data.get('category'),
            'price': product_data.get('price')
        }
        return self.add_session_event(session_id, customer_id, event)
    
    def track_add_to_cart(
        self,
        session_id: str,
        customer_id: str,
        cart_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Track an add-to-cart event."""
        event = {
            'eventType': 'addToCart',
            'productId': cart_data.get('productId'),
            'quantity': cart_data.get('quantity', 1),
            'price': cart_data.get('price')
        }
        return self.add_session_event(session_id, customer_id, event)
    
    def track_search(
        self,
        session_id: str,
        customer_id: str,
        search_query: str,
        result_count: int
    ) -> Dict[str, Any]:
        """Track a search event."""
        event = {
            'eventType': 'search',
            'query': search_query,
            'resultCount': result_count
        }
        return self.add_session_event(session_id, customer_id, event)
    
    # Analytics queries
    
    def get_session_analytics(self, days: int = 7) -> Dict[str, Any]:
        """
        Get aggregated session analytics.
        
        Args:
            days: Number of days to analyze
            
        Returns:
            Dictionary with analytics data
        """
        cutoff_date = (datetime.utcnow() - timedelta(days=days)).isoformat()
        
        # Total sessions
        query = """
        SELECT VALUE COUNT(1) FROM c 
        WHERE c.type = 'session' AND c.createdAt >= @cutoffDate
        """
        total_sessions = self.query_items(
            query,
            [{"name": "@cutoffDate", "value": cutoff_date}]
        )
        
        # Average session duration
        query = """
        SELECT AVG(c.duration) as avgDuration FROM c 
        WHERE c.type = 'session' AND c.createdAt >= @cutoffDate
        """
        avg_duration = self.query_items(
            query,
            [{"name": "@cutoffDate", "value": cutoff_date}]
        )
        
        # Top pages
        query = """
        SELECT TOP 10 c.landingPage, COUNT(1) as count FROM c 
        WHERE c.type = 'session' AND c.createdAt >= @cutoffDate
        GROUP BY c.landingPage
        ORDER BY count DESC
        """
        top_pages = self.query_items(
            query,
            [{"name": "@cutoffDate", "value": cutoff_date}]
        )
        
        return {
            'totalSessions': total_sessions[0] if total_sessions else 0,
            'avgDuration': avg_duration[0].get('avgDuration') if avg_duration else 0,
            'topPages': top_pages
        }
    
    def get_popular_products(self, days: int = 7, limit: int = 10) -> pd.DataFrame:
        """
        Get most viewed products from session events.
        
        Args:
            days: Number of days to analyze
            limit: Maximum number of products
            
        Returns:
            DataFrame with popular products
        """
        cutoff_date = (datetime.utcnow() - timedelta(days=days)).isoformat()
        
        query = f"""
        SELECT TOP {limit}
            e.productId,
            e.productName,
            COUNT(1) as viewCount
        FROM c
        JOIN e IN c.events
        WHERE c.type = 'session' 
            AND c.createdAt >= @cutoffDate
            AND e.eventType = 'productView'
        GROUP BY e.productId, e.productName
        ORDER BY viewCount DESC
        """
        
        parameters = [{"name": "@cutoffDate", "value": cutoff_date}]
        
        items = self.query_items(query, parameters)
        return pd.DataFrame(items) if items else pd.DataFrame()
    
    def get_conversion_funnel(self, days: int = 7) -> Dict[str, int]:
        """
        Get conversion funnel metrics.
        
        Args:
            days: Number of days to analyze
            
        Returns:
            Dictionary with funnel metrics
        """
        cutoff_date = (datetime.utcnow() - timedelta(days=days)).isoformat()
        
        # Sessions with product views
        query = """
        SELECT VALUE COUNT(1) FROM c 
        WHERE c.type = 'session' 
            AND c.createdAt >= @cutoffDate
            AND ARRAY_LENGTH(c.events) > 0
        """
        with_events = self.query_items(
            query,
            [{"name": "@cutoffDate", "value": cutoff_date}]
        )
        
        # Could add more funnel steps here
        
        return {
            'totalSessions': with_events[0] if with_events else 0,
        }
    
    def delete_old_sessions(self, days: int = 90) -> int:
        """
        Delete sessions older than specified days.
        
        Args:
            days: Age threshold in days
            
        Returns:
            Number of deleted sessions
        """
        cutoff_date = (datetime.utcnow() - timedelta(days=days)).isoformat()
        
        query = """
        SELECT c.id, c.customerId FROM c 
        WHERE c.type = 'session' AND c.createdAt < @cutoffDate
        """
        
        parameters = [{"name": "@cutoffDate", "value": cutoff_date}]
        old_sessions = self.query_items(query, parameters)
        
        deleted_count = 0
        for session in old_sessions:
            try:
                self.container.delete_item(
                    item=session['id'],
                    partition_key=session['customerId']
                )
                deleted_count += 1
            except exceptions.CosmosHttpResponseError as e:
                logger.error(f"Failed to delete session {session['id']}: {e}")
        
        logger.info(f"Deleted {deleted_count} old sessions")
        return deleted_count
    
    # ============================================================================
    # PRODUCT CATALOG METHODS WITH VECTOR SEARCH
    # ============================================================================
    
    def create_product(self, product_data: Dict[str, Any], embedding: List[float]) -> Dict[str, Any]:
        """
        Create a new product with vector embedding.
        
        Args:
            product_data: Dictionary containing product information
            embedding: Product description embedding vector (1536 dimensions)
            
        Returns:
            Created product document
        """
        self._ensure_initialized()
        
        # Add required fields
        product_data['id'] = product_data.get('id', str(product_data.get('productId', product_data['sku'])))
        product_data['type'] = 'product'
        product_data['descriptionEmbedding'] = embedding
        product_data['createdAt'] = product_data.get('createdAt', datetime.utcnow().isoformat())
        product_data['updatedAt'] = datetime.utcnow().isoformat()
        product_data['isActive'] = product_data.get('isActive', True)
        
        try:
            created_item = self.products_container.create_item(body=product_data)
            logger.info(f"Created product: {created_item['id']}")
            return created_item
        except exceptions.CosmosHttpResponseError as e:
            logger.error(f"Failed to create product: {e}")
            raise
    
    def get_product(self, product_id: str, category: str) -> Optional[Dict[str, Any]]:
        """
        Get a product by ID.
        
        Args:
            product_id: Product ID
            category: Product category (partition key)
            
        Returns:
            Product document or None
        """
        self._ensure_initialized()
        
        try:
            item = self.products_container.read_item(
                item=product_id,
                partition_key=category
            )
            return item
        except exceptions.CosmosResourceNotFoundError:
            logger.warning(f"Product not found: {product_id}")
            return None
        except exceptions.CosmosHttpResponseError as e:
            logger.error(f"Failed to get product: {e}")
            raise
    
    def search_products_by_embedding(
        self,
        query_embedding: List[float],
        limit: int = 10,
        similarity_threshold: float = 0.7
    ) -> pd.DataFrame:
        """
        Semantic search for products using vector similarity (native CosmosDB vector search).
        
        Args:
            query_embedding: Query vector embedding (1536 dimensions)
            limit: Maximum number of results
            similarity_threshold: Minimum similarity score (0-1)
            
        Returns:
            DataFrame with similar products and similarity scores
        """
        self._ensure_initialized()
        
        # WORKAROUND: Microsoft Fabric CosmosDB strips fields even with SELECT *
        # Strategy: Get product IDs from vector search, then fetch full documents
        
        # First, get just IDs and similarity scores
        query = f"""
        SELECT TOP {limit} c.id, c.productId, VectorDistance(c.descriptionEmbedding, @embedding) AS similarity
        FROM c
        WHERE c.type = 'product' AND c.isActive = true
        ORDER BY VectorDistance(c.descriptionEmbedding, @embedding)
        """
        
        parameters = [
            {"name": "@embedding", "value": query_embedding}
        ]
        
        try:
            # Get similarity scores and IDs
            similarity_items = list(self.query_items(query, parameters, container=self.products_container))
            
            # Now fetch full documents for each ID
            items = []
            for sim_item in similarity_items:
                try:
                    # Query for full document by productId
                    doc_query = "SELECT * FROM c WHERE c.productId = @productId"
                    doc_params = [{"name": "@productId", "value": sim_item['productId']}]
                    full_docs = list(self.query_items(doc_query, doc_params, container=self.products_container))
                    
                    if full_docs:
                        full_item = full_docs[0]
                        # Add similarity score to the full document
                        full_item['similarity'] = sim_item['similarity']
                        items.append(full_item)
                except Exception as read_error:
                    print(f"Could not read product {sim_item.get('productId', 'unknown')}: {read_error}")
                    continue
            
            # VectorDistance returns distance (lower = more similar)
            # Convert distance to similarity percentage: similarity = 1 - distance
            # Then filter by similarity threshold
            for item in items:
                distance = item.get('similarity', 1.0)
                item['similarity'] = 1 - distance  # Convert to similarity score
            
            filtered_items = [item for item in items if item.get('similarity', 0) >= similarity_threshold]
            
            # Debug logging
            if filtered_items:
                logger.info(f"Vector search returned {len(filtered_items)} items")
                logger.info(f"First item keys: {list(filtered_items[0].keys())}")
                logger.info(f"First item productName: {filtered_items[0].get('productName', 'NOT FOUND')}")
            
            return pd.DataFrame(filtered_items) if filtered_items else pd.DataFrame()
        except exceptions.CosmosHttpResponseError as e:
            logger.error(f"Vector search failed with HTTP error: {e.status_code} - {e.message}")
            logger.error(f"Error details: {e}")
            raise
        except Exception as e:
            logger.error(f"Vector search failed: {type(e).__name__} - {e}")
            raise
    
    def find_similar_products(
        self,
        product_id: str,
        category: str,
        limit: int = 5
    ) -> pd.DataFrame:
        """
        Find products similar to a given product using vector similarity.
        
        Args:
            product_id: Source product ID
            category: Product category (partition key)
            limit: Maximum number of similar products
            
        Returns:
            DataFrame with similar products and similarity scores
        """
        # Get the source product
        product = self.get_product(product_id, category)
        if not product or 'descriptionEmbedding' not in product:
            logger.warning(f"Product not found or missing embedding: {product_id}")
            return pd.DataFrame()
        
        # Search using the product's embedding
        embedding = product['descriptionEmbedding']
        results = self.search_products_by_embedding(embedding, limit + 1, 0.5)
        
        # Remove the source product from results
        if not results.empty:
            results = results[results['id'] != product_id].head(limit)
        
        return results
    
    def search_products_text(self, search_term: str, limit: int = 50) -> pd.DataFrame:
        """
        Search products by text in name, description, or brand.
        
        Args:
            search_term: Search term
            limit: Maximum number of results
            
        Returns:
            DataFrame with matching products
        """
        self._ensure_initialized()
        
        query = f"""
        SELECT TOP {limit}
            c.id, c.productId, c.sku, c.productName, c.brand, c.category,
            c.subcategory, c.description, c.price, c.stockQuantity
        FROM c
        WHERE c.type = 'product' AND c.isActive = true
            AND (CONTAINS(LOWER(c.productName), @searchTerm)
                 OR CONTAINS(LOWER(c.description), @searchTerm)
                 OR CONTAINS(LOWER(c.brand), @searchTerm))
        ORDER BY c.productName
        """
        
        parameters = [{"name": "@searchTerm", "value": search_term.lower()}]
        items = self.query_items(query, parameters, container=self.products_container)
        return pd.DataFrame(items) if items else pd.DataFrame()
    
    def get_products_by_category(
        self,
        category: str,
        subcategory: Optional[str] = None,
        limit: int = 100
    ) -> pd.DataFrame:
        """
        Get products by category and optional subcategory.
        
        Args:
            category: Product category
            subcategory: Optional subcategory
            limit: Maximum number of products
            
        Returns:
            DataFrame with products
        """
        self._ensure_initialized()
        
        if subcategory:
            query = f"""
            SELECT TOP {limit} * FROM c
            WHERE c.type = 'product' AND c.category = @category
                AND c.subcategory = @subcategory AND c.isActive = true
            ORDER BY c.productName
            """
            parameters = [
                {"name": "@category", "value": category},
                {"name": "@subcategory", "value": subcategory}
            ]
        else:
            query = f"""
            SELECT TOP {limit} * FROM c
            WHERE c.type = 'product' AND c.category = @category AND c.isActive = true
            ORDER BY c.productName
            """
            parameters = [{"name": "@category", "value": category}]
        
        items = self.query_items(query, parameters, container=self.products_container, enable_cross_partition=False)
        return pd.DataFrame(items) if items else pd.DataFrame()
    
    def get_all_products(self, limit: int = 1000) -> List[Dict[str, Any]]:
        """
        Get all products from the container.
        
        Args:
            limit: Maximum number of products to return
            
        Returns:
            List of product dictionaries
        """
        self._ensure_initialized()
        
        query = f"""
        SELECT TOP {limit} * FROM c
        WHERE c.type = 'product' AND c.isActive = true
        ORDER BY c.productId
        """
        
        items = self.query_items(query, parameters=[], container=self.products_container, enable_cross_partition=True)
        return items if items else []
    
    def update_product_embedding(
        self,
        product_id: str,
        category: str,
        embedding: List[float]
    ) -> Dict[str, Any]:
        """
        Update a product's embedding vector.
        
        Args:
            product_id: Product ID
            category: Product category (partition key)
            embedding: New embedding vector
            
        Returns:
            Updated product document
        """
        product = self.get_product(product_id, category)
        if not product:
            raise ValueError(f"Product not found: {product_id}")
        
        product['descriptionEmbedding'] = embedding
        product['updatedAt'] = datetime.utcnow().isoformat()
        
        try:
            updated_item = self.products_container.replace_item(
                item=product_id,
                body=product
            )
            logger.info(f"Updated product embedding: {product_id}")
            return updated_item
        except exceptions.CosmosHttpResponseError as e:
            logger.error(f"Failed to update product embedding: {e}")
            raise
    
    # ============================================================================
    # PRODUCT REVIEWS METHODS WITH VECTOR SEARCH
    # ============================================================================
    
    def create_review(self, review_data: Dict[str, Any], embedding: List[float]) -> Dict[str, Any]:
        """
        Create a new product review with vector embedding.
        
        Args:
            review_data: Dictionary containing review information
            embedding: Review text embedding vector (1536 dimensions)
            
        Returns:
            Created review document
        """
        self._ensure_initialized()
        
        # Add required fields
        review_data['id'] = review_data.get('id', f"review-{review_data['productId']}-{review_data.get('customerId', 'anon')}-{datetime.utcnow().timestamp()}")
        review_data['type'] = 'review'
        review_data['reviewEmbedding'] = embedding
        review_data['createdAt'] = review_data.get('createdAt', datetime.utcnow().isoformat())
        review_data['updatedAt'] = datetime.utcnow().isoformat()
        
        try:
            created_item = self.reviews_container.create_item(body=review_data)
            logger.info(f"Created review: {created_item['id']}")
            return created_item
        except exceptions.CosmosHttpResponseError as e:
            logger.error(f"Failed to create review: {e}")
            raise
    
    def get_product_reviews(
        self,
        product_id: str,
        limit: int = 10
    ) -> pd.DataFrame:
        """
        Get reviews for a specific product.
        
        Args:
            product_id: Product ID
            limit: Maximum number of reviews
            
        Returns:
            DataFrame with reviews
        """
        self._ensure_initialized()
        
        query = f"""
        SELECT TOP {limit}
            c.id, c.reviewId, c.productId, c.customerId, c.customerName,
            c.rating, c.title, c.reviewText, c.verifiedPurchase,
            c.helpfulCount, c.createdAt
        FROM c
        WHERE c.type = 'review' AND c.productId = @productId
        ORDER BY c.createdAt DESC
        """
        
        parameters = [{"name": "@productId", "value": str(product_id)}]
        items = self.query_items(query, parameters, container=self.reviews_container, enable_cross_partition=False)
        return pd.DataFrame(items) if items else pd.DataFrame()
    
    def get_product_review_summary(self, product_id: str) -> pd.DataFrame:
        """
        Get aggregated review statistics for a product.
        
        Args:
            product_id: Product ID
            
        Returns:
            DataFrame with review summary statistics
        """
        self._ensure_initialized()
        
        query = """
        SELECT
            @productId AS productId,
            COUNT(1) AS totalReviews,
            AVG(c.rating) AS avgRating,
            SUM(CASE WHEN c.rating = 5 THEN 1 ELSE 0 END) AS fiveStarCount,
            SUM(CASE WHEN c.rating = 4 THEN 1 ELSE 0 END) AS fourStarCount,
            SUM(CASE WHEN c.rating = 3 THEN 1 ELSE 0 END) AS threeStarCount,
            SUM(CASE WHEN c.rating = 2 THEN 1 ELSE 0 END) AS twoStarCount,
            SUM(CASE WHEN c.rating = 1 THEN 1 ELSE 0 END) AS oneStarCount,
            SUM(CASE WHEN c.verifiedPurchase = true THEN 1 ELSE 0 END) AS verifiedPurchaseCount
        FROM c
        WHERE c.type = 'review' AND c.productId = @productId
        """
        
        parameters = [{"name": "@productId", "value": str(product_id)}]
        items = self.query_items(query, parameters, container=self.reviews_container, enable_cross_partition=False)
        return pd.DataFrame(items) if items else pd.DataFrame()
    
    def search_reviews_by_embedding(
        self,
        query_embedding: List[float],
        product_id: Optional[str] = None,
        limit: int = 10
    ) -> pd.DataFrame:
        """
        Semantic search for reviews using vector similarity.
        
        Args:
            query_embedding: Query vector embedding
            product_id: Optional product ID to filter reviews
            limit: Maximum number of results
            
        Returns:
            DataFrame with similar reviews and similarity scores
        """
        self._ensure_initialized()
        
        if product_id:
            query = f"""
            SELECT TOP {limit}
                c.id, c.reviewId, c.productId, c.customerId, c.customerName,
                c.rating, c.title, c.reviewText, c.verifiedPurchase,
                c.helpfulCount, c.createdAt,
                VectorDistance(c.reviewEmbedding, @embedding) AS similarity
            FROM c
            WHERE c.type = 'review' AND c.productId = @productId
            ORDER BY VectorDistance(c.reviewEmbedding, @embedding)
            """
            parameters = [
                {"name": "@embedding", "value": query_embedding},
                {"name": "@productId", "value": product_id}
            ]
            enable_cross_partition = False
        else:
            query = f"""
            SELECT TOP {limit}
                c.id, c.reviewId, c.productId, c.customerId, c.customerName,
                c.rating, c.title, c.reviewText, c.verifiedPurchase,
                c.helpfulCount, c.createdAt,
                VectorDistance(c.reviewEmbedding, @embedding) AS similarity
            FROM c
            WHERE c.type = 'review'
            ORDER BY VectorDistance(c.reviewEmbedding, @embedding)
            """
            parameters = [{"name": "@embedding", "value": query_embedding}]
            enable_cross_partition = True
        
        try:
            items = self.query_items(query, parameters, container=self.reviews_container, enable_cross_partition=enable_cross_partition)
            return pd.DataFrame(items) if items else pd.DataFrame()
        except exceptions.CosmosHttpResponseError as e:
            logger.error(f"Review vector search failed: {e}")
            raise
    
    def get_top_rated_products(
        self,
        category: Optional[str] = None,
        min_reviews: int = 5,
        limit: int = 20
    ) -> pd.DataFrame:
        """
        Get top-rated products with minimum review threshold.
        
        Args:
            category: Optional category filter
            min_reviews: Minimum number of reviews required
            limit: Maximum number of products
            
        Returns:
            DataFrame with top-rated products
        """
        self._ensure_initialized()
        
        # First, get review aggregates from reviews container
        if category:
            products_query = """
            SELECT c.productId FROM c
            WHERE c.type = 'product' AND c.category = @category AND c.isActive = true
            """
            parameters = [{"name": "@category", "value": category}]
            products = self.query_items(products_query, parameters, container=self.products_container, enable_cross_partition=False)
        else:
            products_query = """
            SELECT c.productId FROM c
            WHERE c.type = 'product' AND c.isActive = true
            """
            products = self.query_items(products_query, container=self.products_container)
        
        if not products:
            return pd.DataFrame()
        
        # Get review summaries for these products
        product_ids = [p['productId'] for p in products]
        results = []
        
        for pid in product_ids[:limit * 2]:  # Query more than needed for filtering
            summary = self.get_product_review_summary(str(pid))
            if not summary.empty and summary.iloc[0]['totalReviews'] >= min_reviews:
                product = next((p for p in products if p['productId'] == pid), None)
                if product:
                    result = {**product, **summary.iloc[0].to_dict()}
                    results.append(result)
        
        # Sort by rating and return top results
        df = pd.DataFrame(results)
        if not df.empty:
            df = df.sort_values(by=['avgRating', 'totalReviews'], ascending=[False, False]).head(limit)
        
        return df
    
    # ============================================================================
    # HELPER METHODS
    # ============================================================================
    
    def query_items(
        self,
        query: str,
        parameters: Optional[List[Dict[str, Any]]] = None,
        enable_cross_partition: bool = True,
        container: Optional[ContainerProxy] = None
    ) -> List[Dict[str, Any]]:
        """
        Execute a SQL query on the specified container.
        
        Args:
            query: SQL query string
            parameters: Optional query parameters
            enable_cross_partition: Enable cross-partition query
            container: Optional container to query (defaults to sessions container)
            
        Returns:
            List of matching documents
        """
        self._ensure_initialized()
        target_container = container if container is not None else self.container
        
        try:
            items = list(target_container.query_items(
                query=query,
                parameters=parameters,
                enable_cross_partition_query=enable_cross_partition
            ))
            logger.info(f"Query returned {len(items)} items")
            return items
        except exceptions.CosmosHttpResponseError as e:
            logger.error(f"Query failed: {e}")
            raise
    
    def test_connection(self) -> bool:
        """Test Microsoft Fabric CosmosDB connection."""
        try:
            self._ensure_initialized()
            # Simple query to test connection
            query = "SELECT VALUE COUNT(1) FROM c"
            self.query_items(query)
            logger.info("Microsoft Fabric CosmosDB connection test successful")
            return True
        except Exception as e:
            logger.error(f"Microsoft Fabric CosmosDB connection test failed: {e}")
            return False
