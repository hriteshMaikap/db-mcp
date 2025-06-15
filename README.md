# Interactive MongoDB MCP Server

## Overview

A clean, user-friendly MongoDB Model Context Protocol (MCP) server with an interactive interface. Features dynamic database exploration, token management, and structured output for comprehensive data analysis.

## ✨ Key Features

### Interactive Client Experience
- **Guided Workflow**: Step-by-step database → collection → analysis selection
- **Beautiful Output**: Structured display with emojis and clear sections
- **Dynamic Options**: Analysis types adapt to your data structure
- **Error Handling**: Graceful handling of connection and data issues

### 5 Core Tools + 1 Advanced Analyzer
1. **`list_databases()`** - Show all available databases
2. **`list_collections(database_name)`** - List collections with document counts
3. **`get_sample_data(database_name, collection_name, limit)`** - Get sample documents
4. **`get_collection_stats(database_name, collection_name)`** - Basic field analysis
5. **`analyze_mongodb_collection(database_name, collection_name, analysis_type)`** - Advanced analysis with 5 modes:
   - `overview`: General collection overview with samples
   - `recent`: Most recent documents
   - `aggregation`: Smart aggregation based on data types  
   - `field_analysis`: Detailed field type and distribution analysis
   - `time_series`: Time-based analysis (if date fields exist)

### Smart Features
- **Token Management**: 6K token limits for requests/responses with intelligent truncation
- **Structured Output**: Pydantic models ensuring consistent response format
- **Adaptive Analysis**: Different analysis types based on your data structure
- **Dynamic Database Selection**: No hardcoded database names

## 🚀 Quick Start

### 1. Install Dependencies
```bash
pip install pymongo mcp langchain-groq langchain-mcp-adapters langgraph python-dotenv pydantic tiktoken
```

### 2. Setup Environment
Create a `.env` file with your API key:
```env
GROQ_API_KEY=your_groq_api_key_here
```

### 3. Test MongoDB Connection
```bash
python test_mongodb.py
```
This creates sample data if none exists.

### 4. Start MongoDB Server
```bash
python mongodb_server.py
```

### 5. Run Interactive Client
```bash
python client.py
```

## 🎯 User Experience

### Interactive Flow
```
1. Welcome screen shows available databases
2. Choose database → see collections with document counts  
3. Choose collection → see analysis options
4. Choose analysis type → get structured results
5. Continue exploring or exit gracefully
```

### Analysis Options
```
1. overview     - General collection overview with samples
2. recent       - Get most recent documents  
3. aggregation  - Smart aggregation based on data types
4. field_analysis - Detailed field type analysis
5. time_series  - Time-based analysis (if date fields exist)
```

### Beautiful Output Format
```
============================================================
 ANALYZING ecommerce_analytics.marketing_campaigns
============================================================

----------------------------------------
 📊 DATA OVERVIEW
----------------------------------------
Database: ecommerce_analytics
Collection: marketing_campaigns
Total Documents: 15,432
Sample Documents: 3

----------------------------------------
 🔢 NUMERICAL INSIGHTS  
----------------------------------------
Document Count: 15,432
Execution Time: 23.45ms
Fields Found: 8
  • campaign_name: str
  • start_date: datetime
  • budget: int, float
  • status: str

----------------------------------------
 📝 ANALYSIS SUMMARY
----------------------------------------
Analysis of 'marketing_campaigns' in database 'ecommerce_analytics': 
Smart aggregation by campaign_name field. Collection contains 15,432 
documents with 8 fields. Analysis completed in 23.45ms.
```

## 💡 Example Usage

### Natural Language Queries
```python
# The client translates user choices into these queries:
"List all available MongoDB databases"
"List all collections in database 'ecommerce_analytics'"  
"Analyze MongoDB collection 'user_sessions' in database 'analytics' with analysis type 'aggregation'"
```

### What Each Analysis Type Does
- **Overview**: Shows sample docs + basic field types
- **Recent**: Gets newest documents by `_id` or timestamp
- **Aggregation**: Finds string fields and groups by most common values
- **Field Analysis**: Deep dive into field types, null percentages, data quality
- **Time Series**: Groups by date fields to show trends over time

## 🔧 Technical Features

### Token Management
- Automatic token counting with tiktoken
- 6K limits for both requests and responses
- Intelligent truncation with clear indicators
- Sample limiting to prevent overwhelming responses

### Structured Output
```python
class AnalysisResult(BaseModel):
    data_display: DataDisplay          # Sample docs + basic info
    numerical_insights: NumericalInsights  # Metrics + query results  
    textual_summary: str              # Human-readable summary
```

### Error Handling
- Graceful MongoDB connection failures
- Tool parameter validation
- JSON parsing fallbacks
- User-friendly error messages

## 📁 Project Structure

```
client.py           - Interactive MongoDB analyzer (main interface)
mongodb_server.py   - MCP server with 6 tools
test_mongodb.py     - Connection test + sample data creation
test_server.py      - Server function testing
requirements.txt    - Essential dependencies
README.md          - This documentation
```

## 🛠 Configuration

- **MongoDB URI**: `mongodb://localhost:27017` (hardcoded for simplicity)
- **Server Port**: 8002
- **Token Limits**: 6K request/response
- **Sample Limits**: 5-10 documents per query

## 🐛 Troubleshooting

1. **MongoDB not running**: Start with `mongod` or check MongoDB service
2. **No databases found**: Run `python test_mongodb.py` to create sample data
3. **Tool call errors**: Ensure server is running on port 8002
4. **API key errors**: Check your `.env` file has valid `GROQ_API_KEY`
5. **Token limit exceeded**: The system auto-truncates, but you can reduce sample sizes

## 🎯 Perfect For
- Database exploration and discovery
- Data quality assessment  
- Quick analytics and insights
- Field type analysis
- Time series data exploration
- Learning MongoDB structure
