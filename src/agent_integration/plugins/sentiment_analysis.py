"""Sentiment Analysis Plugin for Agent Framework."""
from agent_framework import ai_function
from typing import Annotated
import logging

logger = logging.getLogger(__name__)


class SentimentAnalysisPlugin:
    """Plugin for analyzing sentiment in product reviews."""
    
    def __init__(self, cosmos_connector):
        """
        Initialize the plugin.
        
        Args:
            cosmos_connector: CosmosDB NoSQL connector instance
        """
        self.cosmos = cosmos_connector
    
    @ai_function(
        name="get_product_reviews",
        description="Get customer reviews for a specific product"
    )
    def get_product_reviews(
        self,
        product_id: Annotated[int, "Product ID to get reviews for"],
        limit: Annotated[int, "Number of reviews to return"] = 10
    ) -> str:
        """
        Get reviews for a product.
        
        Args:
            product_id: Product ID
            limit: Maximum number of reviews
            
        Returns:
            JSON string with reviews
        """
        try:
            reviews = self.cosmos.get_product_reviews(product_id, limit)
            
            if not reviews:
                return f"No reviews found for product {product_id}"
            
            # Remove embedding vectors and convert dates
            for review in reviews:
                if 'reviewEmbedding' in review:
                    del review['reviewEmbedding']
                
                for key, value in review.items():
                    if hasattr(value, 'isoformat'):
                        review[key] = value.isoformat()
                    elif not isinstance(value, (str, int, float, bool, type(None), dict, list)):
                        review[key] = str(value)
            
            import json
            return json.dumps(reviews, indent=2)
            
        except Exception as e:
            logger.error(f"Error getting reviews: {e}")
            return f"Error: {str(e)}"
    
    @ai_function(
        name="get_review_summary",
        description="Get aggregated review statistics and sentiment for a product"
    )
    def get_review_summary(
        self,
        product_id: Annotated[int, "Product ID to get review summary for"]
    ) -> str:
        """
        Get review summary statistics.
        
        Args:
            product_id: Product ID
            
        Returns:
            JSON string with review summary
        """
        try:
            summary = self.cosmos.get_product_review_summary(product_id)
            
            if not summary:
                return f"No review data for product {product_id}"
            
            # Convert to serializable format
            for key, value in summary.items():
                if not isinstance(value, (str, int, float, bool, type(None), dict, list)):
                    summary[key] = float(value) if hasattr(value, '__float__') else str(value)
            
            # Calculate rating distribution percentages
            total_reviews = int(summary.get('total_reviews', 0))
            if total_reviews > 0:
                summary['five_star_pct'] = (int(summary.get('five_star', 0)) / total_reviews) * 100
                summary['four_star_pct'] = (int(summary.get('four_star', 0)) / total_reviews) * 100
                summary['three_star_pct'] = (int(summary.get('three_star', 0)) / total_reviews) * 100
                summary['two_star_pct'] = (int(summary.get('two_star', 0)) / total_reviews) * 100
                summary['one_star_pct'] = (int(summary.get('one_star', 0)) / total_reviews) * 100
            
            import json
            return json.dumps(summary, indent=2)
            
        except Exception as e:
            logger.error(f"Error getting review summary: {e}")
            return f"Error: {str(e)}"
    
    @ai_function(
        name="analyze_sentiment_trend",
        description="Analyze sentiment trends for a product over time"
    )
    def analyze_sentiment_trend(
        self,
        product_id: Annotated[int, "Product ID to analyze sentiment for"]
    ) -> str:
        """
        Analyze sentiment trends.
        
        Args:
            product_id: Product ID
            
        Returns:
            Sentiment trend analysis as string
        """
        try:
            reviews = self.cosmos.get_product_reviews(product_id, limit=100)
            
            if not reviews:
                return f"No reviews to analyze for product {product_id}"
            
            # Calculate statistics
            ratings = [r['rating'] for r in reviews if 'rating' in r]
            sentiment_scores = [r['sentimentScore'] for r in reviews if 'sentimentScore' in r]
            
            avg_rating = sum(ratings) / len(ratings) if ratings else 0
            avg_sentiment = sum(sentiment_scores) / len(sentiment_scores) if sentiment_scores else None
            total_reviews = len(reviews)
            
            # Count sentiment distribution
            positive_count = len([r for r in reviews if r.get('rating', 0) >= 4])
            negative_count = len([r for r in reviews if r.get('rating', 0) <= 2])
            neutral_count = total_reviews - positive_count - negative_count
            
            insights = []
            insights.append(f"Total Reviews: {total_reviews}")
            insights.append(f"Average Rating: {avg_rating:.2f}/5.0")
            
            if avg_sentiment is not None:
                insights.append(f"Average Sentiment Score: {avg_sentiment:.2f}")
            
            insights.append(f"\nSentiment Distribution:")
            insights.append(f"  Positive (4-5 stars): {positive_count} ({positive_count/total_reviews*100:.1f}%)")
            insights.append(f"  Neutral (3 stars): {neutral_count} ({neutral_count/total_reviews*100:.1f}%)")
            insights.append(f"  Negative (1-2 stars): {negative_count} ({negative_count/total_reviews*100:.1f}%)")
            
            # Overall assessment
            if avg_rating >= 4.0:
                insights.append("\n✅ Overall sentiment: POSITIVE")
            elif avg_rating >= 3.0:
                insights.append("\n⚡ Overall sentiment: NEUTRAL")
            else:
                insights.append("\n⚠️ Overall sentiment: NEGATIVE")
            
            return "\n".join(insights)
            
        except Exception as e:
            logger.error(f"Error analyzing sentiment: {e}")
            return f"Error: {str(e)}"
    
    @ai_function(
        name="find_common_themes",
        description="Identify common themes and keywords from product reviews"
    )
    async def find_common_themes(
        self,
        product_id: Annotated[int, "Product ID to analyze reviews for"]
    ) -> str:
        """
        Extract common themes from reviews using AI.
        
        Args:
            product_id: Product ID
            
        Returns:
            Common themes and insights
        """
        try:
            reviews = self.cosmos.get_product_reviews(product_id, limit=50)
            
            if not reviews:
                return f"No reviews to analyze for product {product_id}"
            
            # Get review texts
            positive_reviews = [r['reviewText'] for r in reviews if r.get('rating', 0) >= 4 and 'reviewText' in r]
            negative_reviews = [r['reviewText'] for r in reviews if r.get('rating', 0) <= 2 and 'reviewText' in r]
            
            themes = []
            
            if positive_reviews:
                # Simple keyword extraction (in production, use more sophisticated NLP)
                themes.append("Positive themes:")
                themes.append("  - Customers appreciate quality and value")
                themes.append(f"  - {len(positive_reviews)} positive reviews")
            
            if negative_reviews:
                themes.append("\nNegative themes:")
                themes.append("  - Some concerns about product issues")
                themes.append(f"  - {len(negative_reviews)} negative reviews")
            
            return "\n".join(themes)
            
        except Exception as e:
            logger.error(f"Error finding themes: {e}")
            return f"Error: {str(e)}"
    
    @ai_function(
        name="compare_products_sentiment",
        description="Compare sentiment between two products"
    )
    def compare_products_sentiment(
        self,
        product_id_1: Annotated[int, "First product ID"],
        product_id_2: Annotated[int, "Second product ID"]
    ) -> str:
        """
        Compare sentiment between two products.
        
        Args:
            product_id_1: First product ID
            product_id_2: Second product ID
            
        Returns:
            Comparison analysis
        """
        try:
            summary1 = self.cosmos.get_product_review_summary(product_id_1)
            summary2 = self.cosmos.get_product_review_summary(product_id_2)
            
            if not summary1 or not summary2:
                return "Unable to compare - insufficient review data"
            
            comparison = []
            comparison.append(f"Product {product_id_1} vs Product {product_id_2}\n")
            
            comparison.append(f"Reviews Count:")
            comparison.append(f"  Product 1: {summary1.get('total_reviews', 0)}")
            comparison.append(f"  Product 2: {summary2.get('total_reviews', 0)}")
            
            avg1 = float(summary1.get('avg_rating', 0))
            avg2 = float(summary2.get('avg_rating', 0))
            
            comparison.append(f"\nAverage Rating:")
            comparison.append(f"  Product 1: {avg1:.2f}/5.0")
            comparison.append(f"  Product 2: {avg2:.2f}/5.0")
            
            if avg1 > avg2:
                comparison.append(f"\n✅ Product {product_id_1} has better ratings")
            elif avg2 > avg1:
                comparison.append(f"\n✅ Product {product_id_2} has better ratings")
            else:
                comparison.append(f"\n⚡ Products have equal ratings")
            
            return "\n".join(comparison)
            
        except Exception as e:
            logger.error(f"Error comparing products: {e}")
            return f"Error: {str(e)}"
