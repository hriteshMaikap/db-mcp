import os
import asyncio
import json
import uuid
import re
from typing import List, Optional, Literal, Dict, Any, Union
from dotenv import load_dotenv
from google import genai
from pydantic import BaseModel, Field
from mcp import ClientSession
from mcp.client.sse import sse_client
from contextlib import AsyncExitStack

# Reuse viz tools if needed, or just import them
# from .viz import create_bar_chart, ... 

load_dotenv()

# Configuration
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
SQL_SERVER_URL = "http://localhost:8000/sse"
MONGO_SERVER_URL = "http://localhost:8001/sse"

if not GEMINI_API_KEY:
    raise ValueError("GEMINI_API_KEY not set in .env")

client = genai.Client(api_key=GEMINI_API_KEY)

# --- Pydantic Models ---

class Step(BaseModel):
    id: str = Field(description="Unique identifier for the step (e.g., 'step_1').")
    description: str = Field(description="Description of what this step does.")
    tool: Literal["sql_query", "mongo_query"] = Field(description="The tool/action to use.")
    dependency: Optional[str] = Field(default=None, description="ID of a previous step this step depends on (e.g., 'step_1').")
    instruction: str = Field(description="Specific instruction for the tool (e.g., 'Get all users', 'Filter reviews by customer_ids from step_1').")

class UnifiedPlan(BaseModel):
    thought_process: str = Field(description="Reasoning behind the plan, identifying relationships between schemas.")
    steps: List[Step] = Field(description="Ordered list of steps to execute.")

class StepResult(BaseModel):
    step_id: str
    tool: str
    query_executed: str
    data: Any
    summary: str

class FinalAnswer(BaseModel):
    answer: str = Field(description="The final answer to the user's question.")
    insights: List[str] = Field(description="Key insights derived from the data.")

# --- Helper Models for Query Generation ---

class SQLQueryGen(BaseModel):
    query: str = Field(description="Valid SQL SELECT query.")
    explanation: str

class MongoQueryGen(BaseModel):
    collection: str
    query_type: Literal["find", "aggregate", "count"]
    filter: Optional[Dict[str, Any]] = None
    pipeline: Optional[List[Dict[str, Any]]] = None
    explanation: str

# --- Helper Functions ---

def fix_pipeline(pipeline: List[Dict]) -> List[Dict]:
    """Attempts to fix common malformed aggregation stages from LLM."""
    fixed_pipeline = []
    
    # Regex to capture accumulator pattern: $op followed by optional separator and value
    # e.g. "$avg: $field", "$avg $field", "$avg$field", "$avg"
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
                
                # Fix: "field": "$op: $val" or "$op$val" or "$op" -> "field": {"$op": "$val"}
                if isinstance(v, str) and v.strip().startswith("$"):
                    match = acc_pattern.match(v.strip())
                    if match:
                        op = match.group(1)
                        val = match.group(2) if match.group(2) else "$value"
                        
                        # Handle numbers if possible, e.g. "$sum: 1"
                        try:
                            val = int(val)
                        except (ValueError, TypeError):
                            try:
                                val = float(val)
                            except (ValueError, TypeError):
                                pass # Keep as string
                        new_group[k] = {op: val}
                    else:
                        # Fallback for simple split if regex doesn't match but looks like accumulator
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
        self.exit_stack = AsyncExitStack()

    async def start(self):
        """Initialize connections to both MCP servers."""
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
        except Exception as e:
            print(f"Error connecting to servers: {e}")
            raise

    async def stop(self):
        await self.exit_stack.aclose()

    async def get_combined_schema(self) -> str:
        """Fetch and combine schemas from both databases."""
        print("Fetching schemas...")
        
        # SQL Schema
        sql_schema_res = await self.sql_session.call_tool("get_schema", arguments={})
        sql_schema = f"--- SQL DATABASE SCHEMA ---\n{sql_schema_res.content[0].text}\n"

        # Mongo Schema
        mongo_cols_res = await self.mongo_session.call_tool("list_collections", arguments={})
        mongo_cols = json.loads(mongo_cols_res.content[0].text)
        
        mongo_schema = "--- MONGODB SCHEMA ---\n"
        for col in mongo_cols:
            s_res = await self.mongo_session.call_tool("get_schema", arguments={"collection_name": col})
            mongo_schema += f"Collection: {col}\n{s_res.content[0].text}\n\n"

        return sql_schema + "\n" + mongo_schema

    async def generate_plan(self, user_query: str, schema_context: str) -> UnifiedPlan:
        """Generate a multi-step plan based on the query and schemas."""
        print(f"\nPlanning for query: {user_query}")
        
        prompt = f"""
You are a Senior Data Architect planning cross-database queries.

Schemas:
{schema_context}

User Query: "{user_query}"

Available Tools:
- sql_query: Query SQL database (customers, orders, products, etc.)
- mongo_query: Query MongoDB (reviews, logs, etc.)

RULES:
1. Keep plans simple - typically 2-3 steps maximum
2. Identify common fields (customer_id, product_id) for linking data
3. First step should get IDs or filtering criteria from one DB
4. Second step uses those IDs to query the other DB
5. Do NOT use python_processing - the synthesis step will combine results
6. Be specific about which fields to extract and pass between steps

Example for "Customers who bought but didn't review":
Step 1 (SQL): Get all customer_ids from orders table
Step 2 (Mongo): Get distinct customer_ids from product_reviews collection  
(Final synthesis will compare the two lists)

Example for "Product sales vs ratings":
Step 1 (SQL): Get product_id, sales data from products/order_items
Step 2 (Mongo): Get product_id, avg rating from product_reviews
(Final synthesis will join on product_id)
"""
        
        response = await client.aio.models.generate_content(
            model="gemini-2.5-flash",
            contents=prompt,
            config={
                "response_mime_type": "application/json",
                "response_json_schema": UnifiedPlan.model_json_schema(),
            },
        )
        return UnifiedPlan.model_validate_json(response.text)

    async def execute_sql_step(self, step: Step, context_data: Dict[str, Any], schema_context: str) -> StepResult:
        """Generate and execute a SQL query."""
        print(f"  [SQL] {step.instruction}")
        
        # Prepare context from previous steps
        prev_data_str = ""
        if step.dependency and step.dependency in context_data:
            # Summarize or pass relevant data
            prev_data = context_data[step.dependency]
            # Limit data size for prompt
            prev_data_str = f"Data from {step.dependency}: {str(prev_data)[:2000]}"

        prompt = f"""
        Generate a SQL query (MySQL dialect).
        Schema: {schema_context}
        Task: {step.instruction}
        Context: {prev_data_str}
        
        Return valid SQL.
        """
        
        gen_resp = await client.aio.models.generate_content(
            model="gemini-2.5-flash",
            contents=prompt,
            config={"response_mime_type": "application/json", "response_json_schema": SQLQueryGen.model_json_schema()}
        )
        sql_gen = SQLQueryGen.model_validate_json(gen_resp.text)
        
        print(f"    Query: {sql_gen.query}")
        
        try:
            res = await self.sql_session.call_tool("run_select_query", arguments={"query": sql_gen.query})
            data = json.loads(res.content[0].text)
            return StepResult(
                step_id=step.id,
                tool="sql",
                query_executed=sql_gen.query,
                data=data,
                summary=f"Retrieved {len(data.get('rows', []))} rows."
            )
        except Exception as e:
            return StepResult(step_id=step.id, tool="sql", query_executed=sql_gen.query, data={"error": str(e)}, summary=f"Error: {e}")

    async def execute_mongo_step(self, step: Step, context_data: Dict[str, Any], schema_context: str) -> StepResult:
        """Generate and execute a Mongo query."""
        print(f"  [Mongo] {step.instruction}")
        
        # Extract relevant context
        prev_data_str = ""
        extracted_ids = None
        if step.dependency and step.dependency in context_data:
            prev_data = context_data[step.dependency]
            
            # Try to extract IDs from SQL results
            if isinstance(prev_data, dict) and 'rows' in prev_data:
                rows = prev_data['rows']
                # Try to find ID column (customer_id, product_id, etc.)
                if rows and len(rows) > 0:
                    # Assume first column is the ID if instruction mentions filtering
                    if 'filter' in step.instruction.lower() or 'in' in step.instruction.lower():
                        extracted_ids = [row[0] for row in rows if row and len(row) > 0]
                        prev_data_str = f"IDs from {step.dependency}: {extracted_ids[:20]}... (total: {len(extracted_ids)})"
                    else:
                        prev_data_str = f"Data from {step.dependency}: {str(rows[:3])}... (total: {len(rows)} rows)"
            else:
                prev_data_str = f"Data from {step.dependency}: {str(prev_data)[:1000]}"

        prompt = f"""
Generate a MongoDB query.

CRITICAL RULES:
1. For aggregations, ALL fields in $group except _id MUST use accumulator operators
2. To get distinct values, use: {{"$group": {{"_id": "$field"}}}}
3. To count: {{"$group": {{"_id": "$field", "count": {{"$sum": 1}}}}}}
4. To get average: {{"$group": {{"_id": "$field", "avg": {{"$avg": "$value_field"}}}}}}
5. NEVER write: {{"field": null}} or {{"field": "$op: $val"}} in $group
6. ALWAYS write: {{"field": {{"$op": "$val"}}}}

Schema:
{schema_context}

Task: {step.instruction}
Context: {prev_data_str}

Examples:
- Get distinct customer_ids: {{"query_type": "aggregate", "pipeline": [{{"$group": {{"_id": "$customer_id"}}}}]}}
- Count by category: {{"query_type": "aggregate", "pipeline": [{{"$group": {{"_id": "$category", "count": {{"$sum": 1}}}}}}]}}
- Filter by IDs: {{"query_type": "find", "filter": {{"customer_id": {{"$in": [1, 2, 3]}}}}}}
"""
        
        gen_resp = await client.aio.models.generate_content(
            model="gemini-2.5-flash",
            contents=prompt,
            config={"response_mime_type": "application/json", "response_json_schema": MongoQueryGen.model_json_schema()}
        )
        mongo_gen = MongoQueryGen.model_validate_json(gen_resp.text)
        
        # Fix pipeline if it's an aggregation
        if mongo_gen.query_type == "aggregate" and mongo_gen.pipeline:
            print(f"    Original Pipeline: {json.dumps(mongo_gen.pipeline)}")
            mongo_gen.pipeline = fix_pipeline(mongo_gen.pipeline)
            print(f"    Fixed Pipeline: {json.dumps(mongo_gen.pipeline)}")
        
        print(f"    Query: {mongo_gen.query_type} on {mongo_gen.collection}")
        if mongo_gen.query_type == "aggregate":
            print(f"    Final Pipeline: {json.dumps(mongo_gen.pipeline)}")
        else:
            print(f"    Filter: {json.dumps(mongo_gen.filter)}")

        try:
            if mongo_gen.query_type == "find":
                res = await self.mongo_session.call_tool("run_find_query", arguments={
                    "collection_name": mongo_gen.collection,
                    "filter": mongo_gen.filter or {},
                    "limit": 100 # Reasonable limit
                })
                data = json.loads(res.content[0].text)
            elif mongo_gen.query_type == "count":
                res = await self.mongo_session.call_tool("count_documents", arguments={
                    "collection_name": mongo_gen.collection,
                    "filter": mongo_gen.filter or {}
                })
                data = {"count": int(res.content[0].text)}
            else: # aggregate
                res = await self.mongo_session.call_tool("run_aggregate_query", arguments={
                    "collection_name": mongo_gen.collection,
                    "pipeline": mongo_gen.pipeline
                })
                data = json.loads(res.content[0].text)
            
            return StepResult(
                step_id=step.id,
                tool="mongo",
                query_executed=str(mongo_gen.model_dump()),
                data=data,
                summary=f"Query successful."
            )
        except Exception as e:
             return StepResult(step_id=step.id, tool="mongo", query_executed=str(mongo_gen.model_dump()), data={"error": str(e)}, summary=f"Error: {e}")

    async def run(self, user_query: str):
        await self.start()
        try:
            # 1. Schema
            schema = await self.get_combined_schema()
            
            # 2. Plan
            plan = await self.generate_plan(user_query, schema)
            print(f"\nPlan Thought Process: {plan.thought_process}")
            
            context_data = {}
            results = []
            
            # 3. Execute
            for step in plan.steps:
                print(f"\n--- Executing Step: {step.id} ---")
                print(f"    Description: {step.description}")
                
                if step.tool == "sql_query":
                    res = await self.execute_sql_step(step, context_data, schema)
                elif step.tool == "mongo_query":
                    res = await self.execute_mongo_step(step, context_data, schema)
                else:
                    print(f"❌ Unknown tool: {step.tool}")
                    continue
                
                results.append(res)
                context_data[step.id] = res.data
                print(f"    ✓ {res.summary}")
            
            # 4. Synthesize
            print("\n--- Synthesizing Final Answer ---")
            final_prompt = f"""
            User Query: {user_query}
            
            Execution Results:
            {json.dumps([r.model_dump() for r in results], default=str)}
            
            Provide a comprehensive answer to the user query based on these results.
            If you found relationships between the data, highlight them.
            """
            
            final_resp = await client.aio.models.generate_content(
                model="gemini-2.5-flash",
                contents=final_prompt,
                config={"response_mime_type": "application/json", "response_json_schema": FinalAnswer.model_json_schema()}
            )
            final = FinalAnswer.model_validate_json(final_resp.text)
            
            print(f"\n\n=== FINAL ANSWER ===\n{final.answer}")
            print("\n=== INSIGHTS ===")
            for insight in final.insights:
                print(f"- {insight}")
                
        finally:
            await self.stop()

if __name__ == "__main__":
    import sys
    
    # Default query if none provided
    query = "Show customers who have made purchases but haven't left any reviews"
    if len(sys.argv) > 1:
        query = " ".join(sys.argv[1:])
        
    agent = UnifiedAgent()
    asyncio.run(agent.run(query))
