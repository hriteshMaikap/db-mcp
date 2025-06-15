from langchain_mcp_adapters.client import MultiServerMCPClient
from langgraph.prebuilt import create_react_agent
from langchain_groq import ChatGroq

from dotenv import load_dotenv
load_dotenv()

import asyncio

async def main():
    client=MultiServerMCPClient(
        {
            "mongodb": {
                "url": "http://localhost:8002/mcp/",  # MongoDB analysis server
                "transport": "streamable_http",
            }

        }
    )

    import os
    os.environ["GROQ_API_KEY"]=os.getenv("GROQ_API_KEY")
    os.environ["MISTRAL_API_KEY"]=os.getenv("MISTRAL_API_KEY")

    tools=await client.get_tools()
    model=ChatGroq(model="qwen-qwq-32b")
    
    # Create structured model for MongoDB analysis
    from pydantic import BaseModel, Field
    from typing import List, Dict, Any
    
    class MongoDataDisplay(BaseModel):
        sample_documents: List[Dict[str, Any]] = Field(description="Sample documents from the collection")
        total_count: int = Field(description="Total number of documents")
        collection_name: str = Field(description="Collection name")
        database_name: str = Field(description="Database name")
    
    class MongoNumericalInsights(BaseModel):
        document_count: int = Field(description="Total document count")
        field_analysis: Dict[str, List[str]] = Field(description="Field types analysis")
        query_results: List[Dict[str, Any]] = Field(description="Query results")
        execution_time_ms: float = Field(description="Execution time")
    
    class MongoAnalysisResult(BaseModel):
        data_display: MongoDataDisplay = Field(description="Data display section")
        numerical_insights: MongoNumericalInsights = Field(description="Numerical insights section")
        textual_summary: str = Field(description="Textual summary")
    
    structured_model = model.with_structured_output(MongoAnalysisResult)
    
    agent=create_react_agent(
        model,tools
    )
    
    # MongoDB examples
    print("\n" + "="*50)
    print("MONGODB ANALYSIS")
    print("="*50)
    
    # List available databases
    db_response = await agent.ainvoke(
        {"messages": [{"role": "user", "content": "List all available MongoDB databases"}]}
    )
    print("Available databases:", db_response['messages'][-1].content)
    
    # Get user input for database
    print("\nChoose what to analyze:")
    db_name = input("Enter database name: ").strip()
    
    if db_name:
        # List collections
        collections_response = await agent.ainvoke(
            {"messages": [{"role": "user", "content": f"List all collections in database '{db_name}'"}]}
        )
        print(f"\nCollections in {db_name}:", collections_response['messages'][-1].content)
        
        collection_name = input("Enter collection name to analyze: ").strip()
        
        if collection_name:
            # Get sample data
            sample_response = await agent.ainvoke(
                {"messages": [{"role": "user", "content": f"Get sample data from collection '{collection_name}' in database '{db_name}'"}]}
            )
            print(f"\nSample data:", sample_response['messages'][-1].content)
            
            # Get collection stats
            stats_response = await agent.ainvoke(
                {"messages": [{"role": "user", "content": f"Get collection stats for '{collection_name}' in database '{db_name}'"}]}
            )
            print(f"\nCollection stats:", stats_response['messages'][-1].content)
            
            # Run a simple query
            print("\nAvailable query types: 'recent', 'count_by_field', 'date_range'")
            query_type = input("Enter query type: ").strip() or "recent"
            
            query_response = await agent.ainvoke(
                {"messages": [{"role": "user", "content": f"Run simple query of type '{query_type}' on collection '{collection_name}' in database '{db_name}'"}]}
            )
            print(f"\nQuery results:", query_response['messages'][-1].content)
            
            # Demonstrate structured output parsing
            print("\n" + "-"*50)
            print("STRUCTURED OUTPUT DEMONSTRATION")
            print("-"*50)
            
            # Try to parse the response as structured output
            try:
                import json
                # Parse the last response which should be structured
                response_content = query_response['messages'][-1].content
                
                # Check if it's already JSON structured
                if response_content.startswith('{'):
                    parsed_data = json.loads(response_content)
                    
                    if 'data_display' in parsed_data:
                        print("\n📊 DATA DISPLAY:")
                        data_display = parsed_data['data_display']
                        print(f"  Database: {data_display.get('database_name', 'N/A')}")
                        print(f"  Collection: {data_display.get('collection_name', 'N/A')}")
                        print(f"  Total Documents: {data_display.get('total_count', 'N/A')}")
                        print(f"  Sample Size: {len(data_display.get('sample_documents', []))}")
                    
                    if 'numerical_insights' in parsed_data:
                        print("\n🔢 NUMERICAL INSIGHTS:")
                        insights = parsed_data['numerical_insights']
                        print(f"  Document Count: {insights.get('document_count', 'N/A')}")
                        print(f"  Execution Time: {insights.get('execution_time_ms', 'N/A')}ms")
                        print(f"  Query Results Count: {len(insights.get('query_results', []))}")
                        
                        field_analysis = insights.get('field_analysis', {})
                        if field_analysis:
                            print(f"  Field Types: {', '.join(field_analysis.keys())}")
                    
                    if 'textual_summary' in parsed_data:
                        print("\n📝 TEXTUAL SUMMARY:")
                        print(f"  {parsed_data['textual_summary']}")
                        
                else:
                    print("Response is not in structured JSON format")
                    
            except Exception as e:
                print(f"Could not parse structured output: {e}")
                print("Raw response:")
                print(response_content[:500] + "..." if len(response_content) > 500 else response_content)

asyncio.run(main())