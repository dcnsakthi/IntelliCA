"""
Check what deployments are available in Azure OpenAI
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from dotenv import load_dotenv
load_dotenv()

from openai import AzureOpenAI

def main():
    print("=" * 60)
    print("Checking Azure OpenAI Deployments")
    print("=" * 60)
    
    endpoint = os.getenv("AZURE_OPENAI_ENDPOINT")
    api_key = os.getenv("AZURE_OPENAI_API_KEY")
    api_version = os.getenv("AZURE_OPENAI_API_VERSION")
    
    print(f"\nEndpoint: {endpoint}")
    print(f"API Version: {api_version}")
    
    client = AzureOpenAI(
        api_key=api_key,
        api_version=api_version,
        azure_endpoint=endpoint
    )
    
    print("\nTesting configured deployments:\n")
    
    # Test GPT-4 deployment
    gpt4_deployment = os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME", "gpt-4")
    print(f"1. Testing GPT-4 deployment: '{gpt4_deployment}'")
    try:
        response = client.chat.completions.create(
            model=gpt4_deployment,
            messages=[{"role": "user", "content": "Say hello"}],
            max_tokens=10
        )
        print(f"   ✅ GPT-4 deployment '{gpt4_deployment}' is working!")
    except Exception as e:
        if "DeploymentNotFound" in str(e) or "404" in str(e):
            print(f"   ❌ Deployment '{gpt4_deployment}' NOT FOUND")
        else:
            print(f"   ❌ Error: {e}")
    
    # Test Embedding deployment
    embedding_deployment = os.getenv("AZURE_OPENAI_EMBEDDING_DEPLOYMENT", "text-embedding-ada-002")
    print(f"\n2. Testing Embedding deployment: '{embedding_deployment}'")
    try:
        response = client.embeddings.create(
            model=embedding_deployment,
            input="test"
        )
        print(f"   ✅ Embedding deployment '{embedding_deployment}' is working!")
        print(f"   📊 Generated {len(response.data[0].embedding)}-dimensional vector")
    except Exception as e:
        if "DeploymentNotFound" in str(e) or "404" in str(e):
            print(f"   ❌ Deployment '{embedding_deployment}' NOT FOUND")
            print(f"\n   💡 To fix this:")
            print(f"   1. Go to Azure OpenAI Studio: https://oai.azure.com/")
            print(f"   2. Select your resource: {endpoint}")
            print(f"   3. Go to 'Deployments' → 'Create new deployment'")
            print(f"   4. Select model: 'text-embedding-ada-002' (or 'text-embedding-3-small')")
            print(f"   5. Deployment name: '{embedding_deployment}'")
            print(f"   6. Deploy and wait for completion")
        else:
            print(f"   ❌ Error: {e}")
    
    print("\n" + "=" * 60)
    print("Available alternatives if embedding deployment is missing:")
    print("  - Deploy the embedding model in Azure OpenAI Studio")
    print("  - Use a different existing embedding deployment name in .env")
    print("=" * 60)


if __name__ == "__main__":
    main()
