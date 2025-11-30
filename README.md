# SQL MCP Server & Analyst Agent

This project implements a Model Context Protocol (MCP) server for SQL databases and an AI "Analyst" agent that uses it to generate insights.

## Architecture

- **SQL MCP Server**: A `FastMCP` based server running on **SSE over HTTP** (Streamable). It provides tools to inspect schemas, sample data, and execute read-only queries.
- **Analyst Client**: A Python script that uses `google-genai` (Gemini) to reason about the data and generate SQL queries, which are then executed by the MCP server via HTTP.

## Setup

1.  **Install Dependencies**:
    ```bash
    uv pip install -r requirements.txt
    ```

2.  **Environment Variables**:
    Create a `.env` file in the root directory (copy from `.env.example`) and fill in your MySQL credentials and Gemini API Key.
    ```env
    MYSQL_USER=root
    MYSQL_PASS=your_password
    MYSQL_HOST=localhost
    MYSQL_PORT=3306
    MYSQL_DB=your_database
    GEMINI_API_KEY=your_gemini_api_key
    ```

## Running the System

### 1. Start the MCP Server
The server must be running for the client to connect.

```bash
python -m sql_server.main
```
*You should see output indicating it is running on `http://127.0.0.1:8000/sse`*

### 2. Run the Analyst Agent
In a separate terminal, run the client agent.

```bash
python -m client.agent "Show me the top 5 users by activity"
```

## Features

- **HTTP/SSE Transport**: The server runs as a web service, allowing multiple clients to connect.
- **Schema Caching**: The server caches database metadata to reduce latency.
- **Read-Only Safety**: The `run_select_query` tool enforces strict read-only checks.
- **Visualization**: The client includes a `viz.py` module to generate charts (bar, pie) from query results.
