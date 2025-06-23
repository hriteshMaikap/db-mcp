import os
import asyncio
import json
from langchain_mcp_adapters.client import MultiServerMCPClient
from langchain_groq import ChatGroq
from langgraph.prebuilt import create_react_agent
from langchain_core.prompts import ChatPromptTemplate
from dotenv import load_dotenv

load_dotenv()
try:
    groq_api_key = os.getenv("GROQ_API_KEY")
    print("Using Groq API Key")
except Exception as e:
    print("GROQ_API_KEY not set in environment variables. Please set it to use Groq models.")
    print(e)
    raise e

async def build_database_context(agent) -> str:
    """Build comprehensive database context with all table schemas and sample data"""
    print("Building database context...")
    
    # Get all tables
    tables_response = await agent.ainvoke({
        "messages": [{"role": "user", "content": "Get all table names"}]
    })
    
    try:
        tables_content = tables_response['messages'][-1].content
        tables = json.loads(tables_content)
        if isinstance(tables, dict) and "error" in tables:
            return f"Error getting tables: {tables['error']}"
    except:
        return "Error parsing tables response"
    
    # Build rich context for each table
    context = "# DATABASE SCHEMA CONTEXT\n\n"
    
    for table_name in tables:
        print(f"  Fetching schema for: {table_name}")
        schema_response = await agent.ainvoke({
            "messages": [{"role": "user", "content": f"Get schema for table {table_name}"}]
        })
        
        try:
            schema_content = schema_response['messages'][-1].content
            schema_data = json.loads(schema_content)
            
            if "error" in schema_data:
                context += f"## Table: {table_name}\nError: {schema_data['error']}\n\n"
                continue
            
            # Add table information
            context += f"## Table: {schema_data['table']}\n"
            context += "| Column | Type | Sample Value |\n"
            context += "|--------|------|-------------|\n"
            
            sample_row = schema_data.get('sample_row', {})
            
            for col_name, col_type in schema_data['schema']:
                sample_value = sample_row.get(col_name, 'NULL') if sample_row else 'NULL'
                context += f"| {col_name} | {col_type} | {sample_value} |\n"
            
            context += "\n"
            
        except Exception as e:
            context += f"## Table: {table_name}\nError parsing schema: {str(e)}\n\n"
    
    return context

async def analyze_query(analyzer_llm, database_context: str, user_query: str) -> dict:
    """
    Analyze user query against database schema to identify relevant tables and attributes
    """
    
    analysis_prompt = ChatPromptTemplate.from_template("""
You are a database query analyzer expert. Your job is to analyze user queries against database schemas to identify exactly which tables and columns are needed.

CURRENT DATE: 2025-06-23 16:28:06 UTC
USER: hriteshMaikap

DATABASE SCHEMA:
{database_context}

USER QUERY: {user_query}

ANALYSIS INSTRUCTIONS:
1. Carefully read the user query and understand what data they want
2. Examine the database schema to find relevant tables and columns
3. Consider relationships between tables (foreign keys, common fields)
4. Think about aggregations, filters, joins that might be needed
5. Be very precise about column names and table names (use exact names from schema)

Return your analysis as a JSON object with this exact structure:
{{
    "relevant_tables": ["table1", "table2"],
    "required_columns": {{
        "table1": ["col1", "col2"],
        "table2": ["col3", "col4"]
    }},
    "potential_joins": [
        {{
            "table1": "orders",
            "table2": "customers", 
            "join_condition": "orders.customer_id = customers.customer_id"
        }}
    ],
    "aggregations_needed": ["SUM(column)", "COUNT(*)", "GROUP BY column"],
    "filters_needed": ["WHERE condition1", "WHERE condition2"],
    "reasoning": "Detailed explanation of why these tables/columns were selected",
    "query_intent": "Brief description of what user wants to achieve"
}}

Be extremely accurate with table and column names. Only use names that exist in the provided schema.
""")
    
    response = await analyzer_llm.ainvoke(
        analysis_prompt.format(
            database_context=database_context,
            user_query=user_query
        )
    )
    
    # Extract JSON from response
    content = response.content.strip()
    try:
        # Find JSON in the response
        start = content.find('{')
        end = content.rfind('}') + 1
        if start != -1 and end > start:
            json_str = content[start:end]
            return json.loads(json_str)
    except Exception as e:
        print(f"Error parsing analysis response: {e}")
    
    # Fallback response
    return {
        "relevant_tables": [],
        "required_columns": {},
        "potential_joins": [],
        "aggregations_needed": [],
        "filters_needed": [],
        "reasoning": f"Could not parse analysis response: {content[:200]}...",
        "query_intent": "Unknown"
    }

async def main():
    # Set up MCP client for your SQL database server
    client = MultiServerMCPClient({
        "sql_db": {
            "url": "http://localhost:8000/mcp/", 
            "transport": "streamable_http",
        }
    })

    # Get tools from server, create LLM and agent
    tools = await client.get_tools()
    model = ChatGroq(model="qwen-qwq-32b")
    agent = create_react_agent(model, tools)
    
    # Create dedicated analyzer LLM
    analyzer_llm = ChatGroq(model="qwen-qwq-32b", temperature=0)
    
    # Build database context once at startup
    database_context = await build_database_context(agent)
    print("✓ Database context built successfully\n")

    while(1):
        question = input("Enter your question: ")
        
        if "exit" in question.lower() or "quit" in question.lower():
            print("Exiting...")
            break
        
        print("\n" + "="*50)
        print("ANALYZING QUERY...")
        print("="*50)
        
        # Analyze the query first
        analysis = await analyze_query(analyzer_llm, database_context, question)
        
        print(f"\n📊 QUERY ANALYSIS:")
        print(f"Intent: {analysis['query_intent']}")
        print(f"Relevant Tables: {analysis['relevant_tables']}")
        print(f"Required Columns: {json.dumps(analysis['required_columns'], indent=2)}")
        if analysis['potential_joins']:
            print(f"Potential Joins: {json.dumps(analysis['potential_joins'], indent=2)}")
        if analysis['aggregations_needed']:
            print(f"Aggregations: {analysis['aggregations_needed']}")
        if analysis['filters_needed']:
            print(f"Filters: {analysis['filters_needed']}")
        print(f"Reasoning: {analysis['reasoning']}")
        
        print("\n" + "="*50)
        print("AGENT RESPONSE...")
        print("="*50)
        
        # Then get the regular agent response
        response = await agent.ainvoke(
            {"messages": [{"role": "user", "content": question}]}
        )
        
        print("A:", response['messages'][-1].content)

if __name__ == "__main__":
    asyncio.run(main())