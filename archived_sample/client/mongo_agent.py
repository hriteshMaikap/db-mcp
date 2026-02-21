import os
import asyncio
import json
import uuid
import re
from typing import List, Optional, Literal, Dict, Any
from dotenv import load_dotenv
from google import genai
from pydantic import BaseModel, Field
from mcp import ClientSession
from mcp.client.sse import sse_client

from .viz import create_bar_chart, create_pie_chart, create_line_chart, create_scatter_chart

load_dotenv()

# Configuration
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
MCP_SERVER_URL = "http://localhost:8001/sse"

if not GEMINI_API_KEY:
    raise ValueError("GEMINI_API_KEY not set in .env")

client = genai.Client(api_key=GEMINI_API_KEY)

# --- Pydantic Models for Chain of LLMs ---

class SubTask(BaseModel):
    id: str = Field(description="Unique identifier for the sub-task.")
    question: str = Field(description="The specific question to answer in this sub-task.")
    visualization_needed: bool = Field(description="Whether a chart is needed for this sub-task.")
    chart_type: Optional[Literal["bar", "pie", "line", "scatter"]] = Field(default=None, description="Type of chart if needed.")
    chart_title: Optional[str] = Field(default=None, description="Title for the chart.")

class AnalysisPlan(BaseModel):
    sub_tasks: List[SubTask] = Field(description="List of sub-tasks to execute concurrently.")

class MongoQuery(BaseModel):
    collection: str = Field(description="Name of the collection to query.")
    query_type: Literal["find", "aggregate", "count"] = Field(description="Type of query: 'find', 'aggregate', or 'count'.")
    filter: Optional[Dict[str, Any]] = Field(default={}, description="Filter for 'find' or 'count' query.")
    projection: Optional[Dict[str, Any]] = Field(default=None, description="Projection for 'find' query.")
    sort: Optional[List[Any]] = Field(default=None, description="Sort for 'find' query (e.g. [['field', -1]]).")
    limit: int = Field(default=10, description="Limit for 'find' query.")
    pipeline: Optional[List[Dict[str, Any]]] = Field(default=None, description="Pipeline for 'aggregate' query.")
    explanation: str = Field(description="Explanation of the query.")

class SubTaskResult(BaseModel):
    task_id: str
    question: str
    answer: str
    mongo_query: str
    data: Optional[Dict] = None
    image_path: Optional[str] = None

# --- Agent Logic ---

def fix_pipeline(pipeline: List[Dict]) -> List[Dict]:
    """Attempts to fix common malformed aggregation stages from LLM."""
    fixed_pipeline = []
    
    # Regex to capture accumulator pattern: $op followed by optional separator and value
    # e.g. "$avg: $field", "$avg $field", "$avg$field"
    acc_pattern = re.compile(r"^(\$(?:sum|avg|min|max|first|last|push|addToSet))[:\s]*[\"']?(\$?[a-zA-Z0-9_.]+)[\"']?$")

    for stage in pipeline:
        new_stage = stage.copy()
        if "$group" in new_stage:
            group = new_stage["$group"]
            new_group = {}
            for k, v in group.items():
                if k == "_id":
                    new_group[k] = v
                    continue
                
                # Fix: "field": "$op: $val" or "$op$val" -> "field": {"$op": "$val"}
                if isinstance(v, str) and v.strip().startswith("$"):
                    match = acc_pattern.match(v.strip())
                    if match:
                        op = match.group(1)
                        val = match.group(2)
                        
                        # Handle numbers if possible, e.g. "$sum: 1"
                        try:
                            val = int(val)
                        except ValueError:
                            try:
                                val = float(val)
                            except ValueError:
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

def validate_pipeline(pipeline: List[Dict]) -> None:
    """Validate aggregation pipeline for common errors."""
    for i, stage in enumerate(pipeline):
        if "$group" in stage:
            group_stage = stage["$group"]
            for key, value in group_stage.items():
                if key == "_id":
                    continue
                # Check if value is a dict with accumulator operator
                if not isinstance(value, dict):
                    raise ValueError(
                        f"In $group stage {i}, field '{key}' must use an accumulator operator "
                        f"like $sum, $avg, $min, $max. Got: {value}"
                    )
                # Check if it starts with $ (accumulator)
                if isinstance(value, dict):
                    operators = [k for k in value.keys() if k.startswith("$")]
                    if not operators:
                        raise ValueError(
                            f"In $group stage {i}, field '{key}' must use an accumulator operator. "
                            f"Got: {value}"
                        )

def create_visualization(documents: List[Dict], task: SubTask) -> Optional[str]:
    """Create visualization from documents."""
    if not documents:
        return None
    
    first_doc = documents[0]
    keys = list(first_doc.keys())
    
    # Find appropriate keys for visualization
    label_key = None
    value_key = None
    
    # Look for _id as label (common in aggregations)
    if "_id" in first_doc:
        label_key = "_id"
    else:
        label_key = next((k for k, v in first_doc.items() if isinstance(v, str)), keys[0])
    
    # Find numeric value
    for k, v in first_doc.items():
        if k != label_key and isinstance(v, (int, float)):
            value_key = k
            break
    
    if not value_key:
        print(f"  [Task {task.id}] Warning: No numeric values found for visualization")
        return None
    
    # Extract data
    x_vals = []
    y_vals = []
    for d in documents:
        label = d.get(label_key, "")
        # Handle nested _id in aggregations
        if isinstance(label, dict):
            label = str(label)
        x_vals.append(str(label))
        y_vals.append(float(d.get(value_key, 0)))
    
    filename = f"{task.id}_{uuid.uuid4().hex[:8]}.png"
    
    # Create chart based on type
    if task.chart_type == "bar":
        return create_bar_chart(x_vals, y_vals, task.chart_title or task.question, 
                               label_key, value_key, filename)
    elif task.chart_type == "pie":
        return create_pie_chart(x_vals, y_vals, task.chart_title or task.question, filename)
    elif task.chart_type == "line":
        return create_line_chart(x_vals, y_vals, task.chart_title or task.question, 
                                label_key, value_key, filename)
    elif task.chart_type == "scatter":
        try:
            x_nums = [float(d.get(label_key, 0)) for d in documents]
            return create_scatter_chart(x_nums, y_vals, task.chart_title or task.question, 
                                       label_key, value_key, filename)
        except ValueError:
            print(f"  [Task {task.id}] Warning: Scatter chart requires numeric X axis")
            return None
    
    return None

async def execute_subtask(session: ClientSession, schema_context: str, task: SubTask) -> SubTaskResult:
    """Executes a single sub-task: Mongo Gen -> Execute -> Viz -> Summarize."""
    print(f"  [Task {task.id}] Processing: {task.question}")
    
    # 1. Generate MongoDB Query with enhanced prompt
    prompt = f"""
You are an expert MongoDB Data Analyst. Generate a valid MongoDB query.

CRITICAL RULES FOR AGGREGATION:
1. In $group stages, ALL fields except _id MUST use accumulator operators
2. NEVER write: {{"$group": {{"_id": "$field", "value": "$other_field"}}}}
3. ALWAYS write: {{"$group": {{"_id": "$field", "value": {{"$sum": "$other_field"}}}}}}
4. ACCUMULATORS MUST BE OBJECTS, NOT STRINGS.
   WRONG: "count": "$sum: 1"
   CORRECT: "count": {{"$sum": 1}}

Common accumulators: $sum, $avg, $min, $max, $first, $last, $push, $addToSet

Database Schema:
{schema_context}

Task: "{task.question}"

Examples of CORRECT aggregation:
- Average: {{"$group": {{"_id": "$category", "avg_price": {{"$avg": "$price"}}}}}}
- Count: {{"$group": {{"_id": "$status", "count": {{"$sum": 1}}}}}}
- Total: {{"$group": {{"_id": null, "total": {{"$sum": "$amount"}}}}}}

Generate the appropriate query (find, aggregate, or count) for this task.
"""
    
    try:
        # Use Gemini's structured output with standard Pydantic schema
        response = await client.aio.models.generate_content(
            model="gemini-2.5-flash",
            contents=prompt,
            config={
                "response_mime_type": "application/json",
                "response_json_schema": MongoQuery.model_json_schema(),
            },
        )
        print(f"  [Task {task.id}] Raw LLM Response: {response.text}")
        mongo_response = MongoQuery.model_validate_json(response.text)
        
        # Fix and validate pipeline
        if mongo_response.query_type == "aggregate" and mongo_response.pipeline:
            mongo_response.pipeline = fix_pipeline(mongo_response.pipeline)
            print(f"  [Task {task.id}] Fixed Pipeline: {json.dumps(mongo_response.pipeline)}")
            validate_pipeline(mongo_response.pipeline)
            
        print(f"  [Task {task.id}] Parsed Query: {mongo_response.model_dump_json(indent=2)}")
            
    except Exception as e:
        print(f"  [Task {task.id}] Error generating query: {e}")
        return SubTaskResult(
            task_id=task.id, 
            question=task.question, 
            answer=f"Error generating query: {e}", 
            mongo_query=""
        )

    # 2. Execute Query
    try:
        if mongo_response.query_type == "find":
            query_result = await session.call_tool("run_find_query", arguments={
                "collection_name": mongo_response.collection,
                "filter": mongo_response.filter or {},
                "projection": mongo_response.projection,
                "sort": mongo_response.sort,
                "limit": mongo_response.limit
            })
        elif mongo_response.query_type == "count":
            query_result = await session.call_tool("count_documents", arguments={
                "collection_name": mongo_response.collection,
                "filter": mongo_response.filter or {}
            })
        else:  # aggregate
            query_result = await session.call_tool("run_aggregate_query", arguments={
                "collection_name": mongo_response.collection,
                "pipeline": mongo_response.pipeline
            })
            
        content_text = query_result.content[0].text
        
        # Handle count query (returns integer)
        if mongo_response.query_type == "count":
            result_data = {"count": int(content_text), "documents": []}
            documents = []
        else:
            result_data = json.loads(content_text)
            documents = result_data.get("documents", [])
        
    except Exception as e:
        print(f"  [Task {task.id}] Error executing query: {e}")
        return SubTaskResult(
            task_id=task.id, 
            question=task.question, 
            answer=f"Error executing query: {e}\nQuery: {mongo_response.explanation}", 
            mongo_query=str(mongo_response.model_dump())
        )

    # 3. Visualization (if needed)
    image_path = None
    if task.visualization_needed and documents:
        try:
            image_path = create_visualization(documents, task)
        except Exception as e:
            print(f"  [Task {task.id}] Warning: Viz error: {e}")

    # 4. Summarize
    summary_prompt = f"""
Task: "{task.question}"
Query Explanation: {mongo_response.explanation}
Results: {json.dumps(documents[:5], indent=2)}
Total Results: {len(documents)}

Provide a clear, concise answer to the task question based on the data.
Include specific numbers and insights.
"""
    summary_resp = await client.aio.models.generate_content(
        model="gemini-2.5-flash", 
        contents=summary_prompt
    )
    
    return SubTaskResult(
        task_id=task.id,
        question=task.question,
        answer=summary_resp.text,
        mongo_query=f"{mongo_response.explanation}\n\n{json.dumps(mongo_response.model_dump(), indent=2)}",
        data=result_data,
        image_path=image_path
    )

async def run_mongo_agent(user_query: str):
    print(f"--- MongoDB Analyst Agent Started ---")
    print(f"User Query: {user_query}")
    
    async with sse_client(MCP_SERVER_URL) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            
            # 1. Get Schema
            print("\n[1] Fetching Schema...")
            collections_result = await session.call_tool("list_collections", arguments={})
            collections = json.loads(collections_result.content[0].text)
            
            schema_context = ""
            for col in collections:
                schema_res = await session.call_tool("get_schema", arguments={"collection_name": col})
                schema_context += f"Collection: {col}\n{schema_res.content[0].text}\n\n"

            # 2. Decompose Request
            print("\n[2] Decomposing Request...")
            plan_prompt = f"""
You are a Senior Data Analyst planning a MongoDB analysis.

Database Schema:
{schema_context}

User Request: "{user_query}"

Break this into 2-5 independent sub-tasks that can be executed concurrently.
For each task:
- Make it specific and answerable with one MongoDB query
- Decide if visualization would help
- Choose appropriate chart type if needed

Keep tasks focused and avoid overlap.
"""
            plan_resp = await client.aio.models.generate_content(
                model="gemini-2.5-flash",
                contents=plan_prompt,
                config={
                    "response_mime_type": "application/json",
                    "response_json_schema": AnalysisPlan.model_json_schema(),
                },
            )
            plan = AnalysisPlan.model_validate_json(plan_resp.text)
            print(f"Plan: {len(plan.sub_tasks)} tasks")
            for t in plan.sub_tasks:
                print(f"  - {t.question}")

            # 3. Concurrent Execution
            print("\n[3] Executing Sub-tasks Concurrently...")
            tasks = [execute_subtask(session, schema_context, t) for t in plan.sub_tasks]
            results = await asyncio.gather(*tasks)

            # 4. Generate Report
            print("\n[4] Generating Report...")
            generate_html_report(user_query, results)
            print(f"✓ Report generated: reports/mongo_report.html")

def generate_html_report(query: str, results: List[SubTaskResult]):
    html = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>MongoDB Analysis Report</title>
    <style>
        body {{ 
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
            max-width: 1000px; 
            margin: 0 auto; 
            padding: 20px;
            background: #f5f5f5;
        }}
        .header {{
            background: white;
            padding: 30px;
            border-radius: 8px;
            margin-bottom: 20px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        h1 {{ margin: 0 0 10px 0; color: #333; }}
        .query-text {{ color: #666; font-size: 16px; }}
        .task {{ 
            background: white;
            border-left: 4px solid #4CAF50;
            padding: 20px;
            margin-bottom: 20px;
            border-radius: 4px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        .task h3 {{ 
            margin-top: 0;
            color: #4CAF50;
        }}
        .answer {{
            line-height: 1.6;
            margin: 15px 0;
        }}
        .query {{ 
            background: #f8f9fa;
            padding: 15px;
            font-family: 'Monaco', 'Courier New', monospace;
            white-space: pre-wrap;
            border-radius: 4px;
            font-size: 13px;
            overflow-x: auto;
            margin: 15px 0;
        }}
        img {{ 
            max-width: 100%;
            height: auto;
            margin: 20px 0;
            border-radius: 4px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.1);
        }}
        details {{
            margin: 15px 0;
        }}
        summary {{
            cursor: pointer;
            color: #666;
            font-weight: 500;
            padding: 8px 0;
        }}
        summary:hover {{
            color: #4CAF50;
        }}
        .footer {{
            text-align: center;
            color: #999;
            margin-top: 40px;
            padding: 20px;
        }}
    </style>
</head>
<body>
    <div class="header">
        <h1>📊 MongoDB Analysis Report</h1>
        <p class="query-text"><strong>Query:</strong> {query}</p>
    </div>
"""
    
    for i, res in enumerate(results, 1):
        html += f"""
    <div class="task">
        <h3>Task {i}: {res.question}</h3>
        <div class="answer">{res.answer}</div>
        <details>
            <summary>View Query Details</summary>
            <div class="query">{res.mongo_query}</div>
        </details>
"""
        if res.image_path:
            html += f'        <img src="{os.path.basename(res.image_path)}" alt="{res.question}" />\n'
        
        html += "    </div>\n"
    
    html += """
    <div class="footer">
        Generated by MongoDB MCP Agent
    </div>
</body>
</html>
"""
    
    os.makedirs("reports", exist_ok=True)
    with open("reports/mongo_report.html", "w", encoding="utf-8") as f:
        f.write(html)

if __name__ == "__main__":
    import sys
    query = "What is the average session duration for each device type?"
    if len(sys.argv) > 1:
        query = " ".join(sys.argv[1:])
    
    asyncio.run(run_mongo_agent(query))