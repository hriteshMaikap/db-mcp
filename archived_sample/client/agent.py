import os
import asyncio
import json
import uuid
from typing import List, Optional, Literal
from dotenv import load_dotenv
from google import genai
from pydantic import BaseModel, Field
from mcp import ClientSession
from mcp.client.sse import sse_client

from .viz import create_bar_chart, create_pie_chart, create_line_chart, create_scatter_chart

load_dotenv()

# Configuration
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
MCP_SERVER_URL = "http://localhost:8000/sse"

if not GEMINI_API_KEY:
    raise ValueError("GEMINI_API_KEY not set in .env")

client = genai.Client(api_key=GEMINI_API_KEY)

# --- Pydantic Models for Chain of LLMs ---

class SubTask(BaseModel):
    id: str = Field(description="Unique identifier for the sub-task.")
    question: str = Field(description="The specific question to answer in this sub-task.")
    visualization_needed: bool = Field(description="Whether a chart is needed for this sub-task.")
    chart_type: Optional[Literal["bar", "pie", "line", "scatter"]] = Field(description="Type of chart if needed.")
    chart_title: Optional[str] = Field(description="Title for the chart.")

class AnalysisPlan(BaseModel):
    sub_tasks: List[SubTask] = Field(description="List of sub-tasks to execute concurrently.")

class SQLQuery(BaseModel):
    query: str = Field(description="The valid SQL SELECT query to execute. Must be read-only.")
    explanation: str = Field(description="A brief explanation of what the query does.")

class SubTaskResult(BaseModel):
    task_id: str
    question: str
    answer: str
    sql_query: str
    data: Optional[dict] = None
    image_path: Optional[str] = None

# --- Agent Logic ---

async def execute_subtask(session: ClientSession, schema_context: str, task: SubTask) -> SubTaskResult:
    """Executes a single sub-task: SQL Gen -> Execute -> Viz -> Summarize."""
    print(f"  [Task {task.id}] Processing: {task.question}")
    
    # 1. Generate SQL
    prompt = f"""
    You are an expert Data Analyst.
    Database Schema: {schema_context}
    Task: "{task.question}"
    Generate a valid READ-ONLY SQL query (MySQL dialect).
    """
    try:
        response = client.models.generate_content(
            model="gemini-2.5-flash",
            contents=prompt,
            config={
                "response_mime_type": "application/json",
                "response_json_schema": SQLQuery.model_json_schema(),
            },
        )
        sql_response = SQLQuery.model_validate_json(response.text)
        sql_query = sql_response.query
    except Exception as e:
        return SubTaskResult(task_id=task.id, question=task.question, answer=f"Error generating SQL: {e}", sql_query="")

    # 2. Execute SQL
    try:
        query_result = await session.call_tool("run_select_query", arguments={"query": sql_query})
        content_text = query_result.content[0].text
        result_data = json.loads(content_text)
    except Exception as e:
        return SubTaskResult(task_id=task.id, question=task.question, answer=f"Error executing SQL: {e}", sql_query=sql_query)

    # 3. Visualization (if needed)
    image_path = None
    if task.visualization_needed and result_data.get('rows'):
        try:
            # Simple heuristic: first column is label/x, second is value/y
            # In a real app, we'd ask LLM to map columns to axes
            cols = result_data['columns']
            rows = result_data['rows']
            
            # Try to convert 2nd column to float
            try:
                x_vals = [r[0] for r in rows]
                y_vals = [float(r[1]) for r in rows]
                
                filename = f"{task.id}_{uuid.uuid4().hex[:8]}.png"
                
                if task.chart_type == "bar":
                    image_path = create_bar_chart(x_vals, y_vals, task.chart_title or task.question, cols[0], cols[1], filename)
                elif task.chart_type == "pie":
                    image_path = create_pie_chart(x_vals, y_vals, task.chart_title or task.question, filename)
                elif task.chart_type == "line":
                    image_path = create_line_chart(x_vals, y_vals, task.chart_title or task.question, cols[0], cols[1], filename)
                elif task.chart_type == "scatter":
                    # For scatter, we need numeric X
                    x_nums = [float(r[0]) for r in rows]
                    image_path = create_scatter_chart(x_nums, y_vals, task.chart_title or task.question, cols[0], cols[1], filename)
                    
            except (ValueError, IndexError):
                print(f"  [Task {task.id}] Warning: Could not create visualization (data format issue).")
        except Exception as e:
             print(f"  [Task {task.id}] Warning: Viz error: {e}")

    # 4. Summarize
    summary_prompt = f"""
    Task: "{task.question}"
    SQL: {sql_query}
    Data: {result_data}
    Provide a concise answer.
    """
    summary_resp = client.models.generate_content(model="gemini-2.5-flash", contents=summary_prompt)
    
    return SubTaskResult(
        task_id=task.id,
        question=task.question,
        answer=summary_resp.text,
        sql_query=sql_query,
        data=result_data,
        image_path=image_path
    )

async def run_analyst_agent(user_query: str):
    print(f"--- Advanced Analyst Agent Started ---")
    print(f"User Query: {user_query}")
    
    async with sse_client(MCP_SERVER_URL) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            
            # 1. Get Schema
            print("\n[1] Fetching Schema...")
            schema_result = await session.call_tool("get_schema", arguments={})
            schema_context = schema_result.content[0].text

            # 2. Decompose Request
            print("\n[2] Decomposing Request...")
            plan_prompt = f"""
            You are a Senior Data Analyst.
            Schema: {schema_context}
            User Request: "{user_query}"
            Break this request into independent sub-tasks (max 5).
            For each task, decide if a visualization is needed.
            """
            plan_resp = client.models.generate_content(
                model="gemini-2.5-flash",
                contents=plan_prompt,
                config={
                    "response_mime_type": "application/json",
                    "response_json_schema": AnalysisPlan.model_json_schema(),
                },
            )
            plan = AnalysisPlan.model_validate_json(plan_resp.text)
            print(f"Plan: {[t.question for t in plan.sub_tasks]}")

            # 3. Concurrent Execution
            print("\n[3] Executing Sub-tasks Concurrently...")
            tasks = [execute_subtask(session, schema_context, t) for t in plan.sub_tasks]
            results = await asyncio.gather(*tasks)

            # 4. Generate Report
            print("\n[4] Generating Report...")
            generate_html_report(user_query, results)
            print(f"Report generated: reports/report.html")

def generate_html_report(query: str, results: List[SubTaskResult]):
    html = f"""
    <html>
    <head>
        <title>Analysis Report</title>
        <style>
            body {{ font-family: sans-serif; max-width: 800px; margin: 0 auto; padding: 20px; }}
            .task {{ border: 1px solid #ddd; padding: 15px; margin-bottom: 20px; border-radius: 5px; }}
            .sql {{ background: #f4f4f4; padding: 10px; font-family: monospace; }}
            img {{ max-width: 100%; height: auto; margin-top: 10px; }}
        </style>
    </head>
    <body>
        <h1>Analysis Report</h1>
        <p><strong>Query:</strong> {query}</p>
        <hr>
    """
    
    for res in results:
        html += f"""
        <div class="task">
            <h3>{res.question}</h3>
            <p>{res.answer}</p>
            <div class="sql">{res.sql_query}</div>
        """
        if res.image_path:
            # image_path is like reports/filename.png, but html is in reports/ usually or root?
            # Let's assume we run from root, so reports/filename.png is correct relative path
            # But if we save html in reports/, it should be just filename.
            # Let's save html in root for simplicity.
            html += f'<img src="{res.image_path}" />'
        
        html += "</div>"
    
    html += "</body></html>"
    
    with open("report.html", "w") as f:
        f.write(html)

if __name__ == "__main__":
    import sys
    query = "Compare the number of products in 'Electronics' vs 'Toys' and show me the top 3 most expensive products overall."
    if len(sys.argv) > 1:
        query = sys.argv[1]
    
    asyncio.run(run_analyst_agent(query))
