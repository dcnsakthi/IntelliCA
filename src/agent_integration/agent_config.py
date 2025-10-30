"""Agent Framework configuration and initialization."""
import os
import asyncio
from typing import Optional, List
import logging
from azure.identity import DefaultAzureCredential, AzureCliCredential
from agent_framework.azure import AzureOpenAIResponsesClient
from agent_framework.openai import OpenAIResponsesClient
from openai import AzureOpenAI, OpenAI
import numpy as np

logger = logging.getLogger(__name__)


def create_agent(use_azure: bool = True):
    """
    Create and configure an AI Agent using Microsoft Agent Framework.
    
    Args:
        use_azure: If True, use Azure OpenAI; otherwise use OpenAI directly
        
    Returns:
        Configured AI Agent instance
    """
    
    if use_azure:
        # Azure OpenAI configuration
        endpoint = os.getenv("AZURE_OPENAI_ENDPOINT")
        api_key = os.getenv("AZURE_OPENAI_API_KEY")
        deployment_name = os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME", "gpt-4")
        api_version = os.getenv("AZURE_OPENAI_API_VERSION", "2024-02-15-preview")
        
        if not endpoint or not api_key:
            raise ValueError(
                "Azure OpenAI credentials not found. "
                "Please set AZURE_OPENAI_ENDPOINT and AZURE_OPENAI_API_KEY"
            )
        
        # Create Azure OpenAI agent with Entra ID credential support
        try:
            # Try to use Azure CLI credential first
            credential = AzureCliCredential()
            agent = AzureOpenAIResponsesClient(
                endpoint=endpoint,
                deployment_name=deployment_name,
                api_version=api_version,
                credential=credential
            ).create_agent(
                name="CustomerAnalyticsAgent",
                instructions="""You are an intelligent customer analytics assistant. 
                You help analyze customer data, generate insights, recommend products, 
                and analyze sentiment. You have access to customer profiles, product catalogs, 
                and review data."""
            )
            logger.info(f"Initialized Azure OpenAI Agent with deployment: {deployment_name} (Entra ID)")
        except Exception:
            # Fallback to API key authentication
            agent = AzureOpenAIResponsesClient(
                endpoint=endpoint,
                deployment_name=deployment_name,
                api_version=api_version,
                api_key=api_key
            ).create_agent(
                name="CustomerAnalyticsAgent",
                instructions="""You are an intelligent customer analytics assistant. 
                You help analyze customer data, generate insights, recommend products, 
                and analyze sentiment. You have access to customer profiles, product catalogs, 
                and review data."""
            )
            logger.info(f"Initialized Azure OpenAI Agent with deployment: {deployment_name} (API Key)")
        
    else:
        # OpenAI direct configuration
        api_key = os.getenv("OPENAI_API_KEY")
        
        if not api_key:
            raise ValueError(
                "OpenAI API key not found. Please set OPENAI_API_KEY"
            )
        
        # Create OpenAI agent
        agent = OpenAIResponsesClient(
            api_key=api_key
        ).create_agent(
            name="CustomerAnalyticsAgent",
            instructions="""You are an intelligent customer analytics assistant. 
            You help analyze customer data, generate insights, recommend products, 
            and analyze sentiment. You have access to customer profiles, product catalogs, 
            and review data.""",
            model="gpt-4"
        )
        
        logger.info("Initialized OpenAI Agent")
    
    logger.info("Agent Framework initialized successfully")
    return agent


def get_embedding_service(use_azure: bool = True):
    """
    Create an embedding service for vector generation using OpenAI SDK.
    
    Args:
        use_azure: If True, use Azure OpenAI; otherwise use OpenAI directly
        
    Returns:
        OpenAI client instance for embeddings
    """
    if use_azure:
        endpoint = os.getenv("AZURE_OPENAI_ENDPOINT")
        api_key = os.getenv("AZURE_OPENAI_API_KEY")
        api_version = os.getenv("AZURE_OPENAI_API_VERSION", "2024-02-15-preview")
        
        if not endpoint or not api_key:
            raise ValueError(
                "Azure OpenAI credentials not found. "
                "Please set AZURE_OPENAI_ENDPOINT and AZURE_OPENAI_API_KEY"
            )
        
        # Create Azure OpenAI client for embeddings
        client = AzureOpenAI(
            api_key=api_key,
            api_version=api_version,
            azure_endpoint=endpoint
        )
        logger.info(f"Initialized Azure OpenAI embeddings client")
        return client
        
    else:
        api_key = os.getenv("OPENAI_API_KEY")
        
        if not api_key:
            raise ValueError(
                "OpenAI API key not found. Please set OPENAI_API_KEY"
            )
        
        client = OpenAI(api_key=api_key)
        logger.info("Initialized OpenAI embeddings client")
        return client


def generate_embeddings(
    texts: List[str],
    embedding_service=None,
    use_azure: bool = True
) -> List[List[float]]:
    """
    Generate embeddings for a list of texts using OpenAI SDK.
    
    Args:
        texts: List of text strings to embed
        embedding_service: Optional OpenAI client instance
        use_azure: If True, use Azure OpenAI deployment name
        
    Returns:
        List of embedding vectors
    """
    if embedding_service is None:
        embedding_service = get_embedding_service(use_azure=use_azure)
    
    # Get the deployment/model name
    if use_azure:
        model = os.getenv("AZURE_OPENAI_EMBEDDING_DEPLOYMENT", "text-embedding-ada-002")
    else:
        model = "text-embedding-ada-002"
    
    # Generate embeddings using OpenAI SDK
    embeddings = []
    for text in texts:
        response = embedding_service.embeddings.create(
            input=text,
            model=model
        )
        embedding = response.data[0].embedding
        embeddings.append(embedding)
    
    logger.info(f"Generated embeddings for {len(texts)} texts using {model}")
    return embeddings


def configure_logging(log_level: str = "INFO"):
    """Configure logging for the application."""
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('app.log'),
            logging.StreamHandler()
        ]
    )
