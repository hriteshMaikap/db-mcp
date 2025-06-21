import asyncio
import os
import json
from langchain_mcp_adapters.client import MultiServerMCPClient
from langchain_groq import ChatGroq
from langchain_core.prompts import ChatPromptTemplate
from langgraph.prebuilt import create_react_agent
from dotenv import load_dotenv

load_dotenv()
groq_api_key = os.getenv("GROQ_API_KEY")

# Initialize MCP client
client = MultiServerMCPClient({
    "sql_db": {
        "url": "http://localhost:8000/mcp/",
        "transport": "streamable_http",
    }
})

async def build_context_prompt(agent, tables) -> str:
    """Builds rich context prompt using database tools via the agent"""

    schemas, samples = [], []
    for table in tables:
        # Ask agent to get schema for the table
        schema_query = f"Provide the schema (columns, primary keys, foreign keys) for the table '{table}' as JSON."
        schema_response = await agent.ainvoke(
            {"messages": [{"role": "user", "content": schema_query}]}
        )
        schema_data = json.loads(schema_response['messages'][-1].content)
        schemas.append(schema_data)

        # Ask agent to get sample row for the table
        sample_query = f"Provide a sample row from the table '{table}' as JSON."
        sample_response = await agent.ainvoke(
            {"messages": [{"role": "user", "content": sample_query}]}
        )
        sample_data = json.loads(sample_response['messages'][-1].content)
        samples.append(sample_data)

    # 3. Create context prompt
    context = "# Database Schema Overview\n"
    context += "## Tables and Relationships\n"
    for schema_data in schemas:
        context += f"- **{schema_data['table']}**\n"
        context += f"  - Columns: {', '.join(c['name'] for c in schema_data['columns'])}\n"
        if schema_data.get('foreign_keys'):
            context += "  - Relationships:\n"
            for fk in schema_data['foreign_keys']:
                context += f"    - {fk['columns']} → {fk['references']}.{fk['remote_columns']}\n"
    context += "\n## Sample Data\n"
    for i, sample_data in enumerate(samples):
        table = tables[i]
        context += f"- **{table}**: {json.dumps(sample_data, indent=2)}\n"
    return context

async def main():
    # Get tools and create agent
    tools = await client.get_tools()
    analyzer_llm = ChatGroq(model="qwen-qwq-32b", temperature=0, api_key=groq_api_key)
    agent = create_react_agent(analyzer_llm, tools)

    # 1. Get all tables (via agent)
    tables_query = "List all table names in the database as a JSON array."
    tables_response = await agent.ainvoke(
        {"messages": [{"role": "user", "content": tables_query}]}
    )
    tables = json.loads(tables_response['messages'][-1].content)

    # 2. Build context prompt using the agent for schemas/samples
    question = "How many orders did each customer place last month?"
    context = await build_context_prompt(agent, tables)

    # 3. Analyzer agent (LLM only, no tools)
    analyzer_prompt = ChatPromptTemplate.from_template("""
    You are a database relationship expert. Analyze the database structure and user question.
    Identify which tables are needed to answer the question and how they should be joined.

    Database Context:
    {context}

    Your Task:
    1. Identify required tables
    2. Determine join relationships
    3. Output JSON format:
        {{
            "tables": ["table1", "table2"],
            "join_conditions": [
                "table1.column = table2.column"
            ],
            "key_columns": {{
                "table1": ["id", "name"],
                "table2": ["id", "foreign_id"]
            }}
        }}
    """)
    analyzer_agent = create_react_agent(analyzer_llm, tools=[], prompt=analyzer_prompt)

    # 4. SQL generator agent (needs execute_sql tool)
    sql_tool = [t for t in tools if t.name == "execute_sql"][0]
    generator_prompt = ChatPromptTemplate.from_template("""
    You are a SQL expert. Generate a SQL query to answer the question using:
    - Required tables: {tables}
    - Join conditions: {join_conditions}
    - Key columns: {key_columns}
    - Sample data context: {sample_context}

    Important Rules:
    - Use only SELECT statements
    - Always qualify column names (table.column)
    - Use explicit JOIN syntax
    - Include only necessary columns
    - Add LIMIT 10 unless user specifies otherwise

    Question: {question}
    """)
    generator_agent = create_react_agent(analyzer_llm, tools=[sql_tool], prompt=generator_prompt)

    # 5. Run analyzer agent
    analysis = await analyzer_agent.ainvoke({
        "messages": [{"role": "user", "content": context}]
    })
    analysis_data = json.loads(analysis['messages'][-1].content)

    # 6. Prepare SQL generation context
    sql_context = {
        "tables": analysis_data["tables"],
        "join_conditions": analysis_data["join_conditions"],
        "key_columns": json.dumps(analysis_data["key_columns"]),
        "sample_context": context,
        "question": question
    }
    sql_query_prompt = generator_prompt.format(**sql_context)
    result = await generator_agent.ainvoke({
        "messages": [{"role": "user", "content": sql_query_prompt}]
    })

    # Extract and execute SQL
    output_content = result['messages'][-1].content
    if "```sql" in output_content:
        sql = output_content.split("```sql")[1].split("```")[0].strip()
    else:
        sql = output_content.strip()
    print(f"Generated SQL:\n{sql}")

    # 7. Execute SQL (via agent)
    sql_exec_query = f"Execute the following SQL and provide the result as JSON:\n{sql}"
    sql_exec_response = await agent.ainvoke(
        {"messages": [{"role": "user", "content": sql_exec_query}]}
    )
    print(f"Query Result:\n{sql_exec_response['messages'][-1].content}")

if __name__ == "__main__":
    asyncio.run(main())