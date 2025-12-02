import os
import asyncio
import json
import re
from typing import List, Optional, Dict, Any
from dotenv import load_dotenv
from google import genai
from google.genai import types
from mcp import ClientSession
from mcp.client.sse import sse_client
from contextlib import AsyncExitStack

load_dotenv()

# Configuration
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
SQL_SERVER_URL = "http://localhost:8000/sse"
MONGO_SERVER_URL = "http://localhost:8001/sse"
VIZ_SERVER_URL = "http://localhost:8002/sse"

if not GEMINI_API_KEY:
    raise ValueError("GEMINI_API_KEY not set in .env")

client = genai.Client(api_key=GEMINI_API_KEY)

# --- Helper Functions ---

def fix_mongo_pipeline(pipeline: List[Dict]) -> List[Dict]:
    """Attempts to fix common malformed aggregation stages from LLM."""
    fixed_pipeline = []
    acc_pattern = re.compile(r"^(\$(?:sum|avg|min|max|first|last|push|addToSet))[:\s]*[\"']?(\$?[a-zA-Z0-9_.]+)?[\"']?$")

    for stage in pipeline:
        new_stage = stage.copy()
        if "$group" in new_stage:
            group = new_stage["$group"]
            new_group = {}
            for k, v in group.items():
                if k == "_id":
                    new_group[k] = v
                    continue
                if isinstance(v, str) and v.strip().startswith("$"):
                    match = acc_pattern.match(v.strip())
                    if match:
                        op = match.group(1)
                        val = match.group(2) if match.group(2) else "$value"
                        try:
                            val = int(val)
                        except (ValueError, TypeError):
                            try:
                                val = float(val)
                            except (ValueError, TypeError):
                                pass 
                        new_group[k] = {op: val}
                    else:
                        if ":" in v:
                            parts = v.split(":", 1)
                            op = parts[0].strip()
                            val = parts[1].strip().strip('"\'')
                            new_group[k] = {op: val}
                        else:
                            new_group[k] = v
                else:
                    new_group[k] = v
            new_stage["$group"] = new_group
        fixed_pipeline.append(new_stage)
    return fixed_pipeline

# --- Agent Class ---

class UnifiedAgent:
    def __init__(self):
        self.sql_session: Optional[ClientSession] = None
        self.mongo_session: Optional[ClientSession] = None
        self.viz_session: Optional[ClientSession] = None
        self.exit_stack = AsyncExitStack()
        self.tool_map = {} # Maps namespaced_name -> (session, original_name)
        self.gemini_tools = []

    async def start(self):
        """Initialize connections and discover tools."""
        try:
            # Connect to SQL Server
            sql_client = await self.exit_stack.enter_async_context(sse_client(SQL_SERVER_URL))
            self.sql_session = await self.exit_stack.enter_async_context(ClientSession(sql_client[0], sql_client[1]))
            await self.sql_session.initialize()
            print("✓ Connected to SQL Server")

            # Connect to Mongo Server
            mongo_client = await self.exit_stack.enter_async_context(sse_client(MONGO_SERVER_URL))
            self.mongo_session = await self.exit_stack.enter_async_context(ClientSession(mongo_client[0], mongo_client[1]))
            await self.mongo_session.initialize()
            print("✓ Connected to MongoDB Server")

            # Connect to Viz Server
            viz_client = await self.exit_stack.enter_async_context(sse_client(VIZ_SERVER_URL))
            self.viz_session = await self.exit_stack.enter_async_context(ClientSession(viz_client[0], viz_client[1]))
            await self.viz_session.initialize()
            print("✓ Connected to Visualization Server")
            
            # Discover and Bind Tools
            await self._discover_tools()
            
        except Exception as e:
            print(f"Error connecting to servers: {e}")
            raise

    async def _discover_tools(self):
        """Fetch tools from both servers and convert to Gemini format."""
        print("Discovering tools...")
        
        # SQL Tools
        sql_tools_res = await self.sql_session.list_tools()
        for tool in sql_tools_res.tools:
            name = f"sql_{tool.name}"
            self.tool_map[name] = (self.sql_session, tool.name)
            self.gemini_tools.append(self._convert_to_gemini_tool(tool, name))
            print(f"  - Registered: {name}")

        # Mongo Tools
        mongo_tools_res = await self.mongo_session.list_tools()
        for tool in mongo_tools_res.tools:
            name = f"mongo_{tool.name}"
            self.tool_map[name] = (self.mongo_session, tool.name)
            self.gemini_tools.append(self._convert_to_gemini_tool(tool, name))
            print(f"  - Registered: {name}")

        # Viz Tools
        viz_tools_res = await self.viz_session.list_tools()
        for tool in viz_tools_res.tools:
            name = f"viz_{tool.name}"
            self.tool_map[name] = (self.viz_session, tool.name)
            self.gemini_tools.append(self._convert_to_gemini_tool(tool, name))
            print(f"  - Registered: {name}")

    def _convert_to_gemini_tool(self, mcp_tool, new_name: str) -> types.Tool:
        """Convert MCP Tool to Gemini Tool definition."""
        # MCP schema is JSON Schema draft 2020-12, Gemini expects similar
        # We need to ensure types are compatible and remove unsupported fields
        
        sanitized_schema = self._sanitize_schema(mcp_tool.inputSchema)
        
        return types.Tool(
            function_declarations=[
                types.FunctionDeclaration(
                    name=new_name,
                    description=mcp_tool.description,
                    parameters=sanitized_schema
                )
            ]
        )

    def _sanitize_schema(self, schema: Dict[str, Any]) -> Dict[str, Any]:
        """Recursively remove fields that Gemini doesn't like (e.g. additionalProperties)."""
        if not isinstance(schema, dict):
            return schema
            
        new_schema = {}
        for k, v in schema.items():
            # Remove incompatible fields
            if k in ["additionalProperties", "anyOf"]:
                # Gemini often struggles with anyOf, prefer simple types or skip
                # For now, we'll just skip additionalProperties
                if k == "additionalProperties":
                    continue
                if k == "anyOf":
                    continue
            
            # Recursively sanitize
            if isinstance(v, dict):
                new_schema[k] = self._sanitize_schema(v)
            elif isinstance(v, list):
                new_schema[k] = [self._sanitize_schema(item) if isinstance(item, dict) else item for item in v]
            else:
                new_schema[k] = v
        
        return new_schema

    async def stop(self):
        await self.exit_stack.aclose()

    async def run(self, user_query: str):
        await self.start()
        try:
            print(f"\n--- Starting Autonomous Agent ---")
            print(f"User Query: {user_query}")
            
            chat = client.aio.chats.create(
                model="gemini-2.5-flash",
                config=types.GenerateContentConfig(
                    tools=self.gemini_tools,
                    temperature=0
                )
            )
            
            # System prompt to guide the agent
            system_prompt = """
You are an expert Database Agent with access to SQL, MongoDB, and Visualization tools.

Your goal is to answer the user's query by autonomously using the available tools.

STRATEGY:
1. EXPLORE: Always start by checking schemas (sql_get_schema, mongo_list_collections/mongo_get_schema).
2. QUERY: Run queries to get the data needed.
   - For MongoDB aggregations, ensure you use correct accumulator syntax (e.g. {"$sum": 1}, {"$avg": "$field"}).
3. VISUALIZE: If the user asks for a chart/graph/plot, use the viz_ tools.
   - You have: viz_create_bar_chart, viz_create_pie_chart, viz_create_line_chart, viz_create_scatter_chart
   - These tools take arrays of data (x_values, y_values) and return a filepath
4. SYNTHESIZE: Provide a clear, comprehensive final answer that addresses the user's question.

IMPORTANT:
- If you need to join data from SQL and Mongo, fetch one dataset first, then use those values to filter the other.
- Common join keys: customer_id, product_id, order_id
- For visualizations, you may need to transform/combine the data first before calling the viz tool
- Always provide a final answer explaining what you found

COMMON PATTERNS:
- Scatter plot: Compare two numeric metrics (e.g. sales vs ratings)
  * Call viz_create_scatter_chart with x_values=[sales], y_values=[ratings], etc.
"""
            
            # Initial message
            response = await chat.send_message(f"{system_prompt}\n\nQuery: {user_query}")
            
            max_iterations = 15  # Prevent infinite loops
            iteration = 0
            
            # ReAct Loop
            while iteration < max_iterations:
                iteration += 1
                
                # Check if the model wants to call a function
                if not response.function_calls:
                    # No function call, this should be the final answer
                    final_text = response.text if response.text else "No answer provided"
                    print(f"\n=== FINAL ANSWER ===\n{final_text}")
                    break
                
                # Execute all function calls requested
                parts = []
                for fc in response.function_calls:
                    tool_name = fc.name
                    tool_args = fc.args
                    
                    print(f"\n[Tool Call] {tool_name}")
                    print(f"  Args: {json.dumps(tool_args)}")
                    
                    if tool_name in self.tool_map:
                        session, original_name = self.tool_map[tool_name]
                        
                        # Special handling for mongo pipeline fix
                        if original_name == "run_aggregate_query" and "pipeline" in tool_args:
                            tool_args["pipeline"] = fix_mongo_pipeline(tool_args["pipeline"])
                            print(f"  (Fixed Pipeline): {json.dumps(tool_args['pipeline'])}")
                        
                        try:
                            result = await session.call_tool(original_name, arguments=tool_args)
                            output = result.content[0].text
                            # Truncate long outputs to save context
                            if len(output) > 2000:
                                output = output[:2000] + "... (truncated)"
                            print(f"  ✓ Result: {output[:200]}...")
                        except Exception as e:
                            output = f"Error: {str(e)}"
                            print(f"  ✗ Error: {output}")
                    else:
                        output = f"Error: Tool {tool_name} not found"
                        print(f"  ✗ Error: {output}")
                    
                    parts.append(types.Part.from_function_response(
                        name=tool_name,
                        response={"result": output}
                    ))
                
                # Send tool outputs back to the model
                response = await chat.send_message(parts)

        finally:
            await self.stop()

if __name__ == "__main__":
    import sys
    query = "Show customers who have made purchases but haven't left any reviews"
    if len(sys.argv) > 1:
        query = " ".join(sys.argv[1:])
        
    agent = UnifiedAgent()
    asyncio.run(agent.run(query))
