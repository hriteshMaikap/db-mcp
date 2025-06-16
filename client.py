import gradio as gr
import asyncio
import os
from langchain_mcp_adapters.client import MultiServerMCPClient
from langgraph.prebuilt import create_react_agent
from langchain_groq import ChatGroq
from dotenv import load_dotenv

load_dotenv()

class MongoDBClient:
    def __init__(self):
        self.client = None
        self.agent = None
        self.connected = False
        
    async def connect(self):
        """Initialize MCP client and agent"""
        try:
            self.client = MultiServerMCPClient({
                "mongodb": {
                    "url": "http://localhost:8002/mcp/",
                    "transport": "streamable_http",
                }
            })
            
            tools = await self.client.get_tools()
            model = ChatGroq(model="qwen-qwq-32b", temperature=0)
            self.agent = create_react_agent(model, tools)
            self.connected = True
            
            return "✅ Connected to MongoDB MCP Server"
        except Exception as e:
            self.connected = False
            return f"❌ Connection failed: {str(e)}"
    
    async def query(self, user_input, history):
        """Execute MongoDB query"""
        if not self.connected:
            return "❌ Please connect first", history
        
        try:
            enhanced_query = f"""
Database: ecommerce_analytics

Query: {user_input}

Please execute this MongoDB query and provide clear results.
"""
            
            response = await self.agent.ainvoke({
                "messages": [{"role": "user", "content": enhanced_query}]
            })
            
            result = response['messages'][-1].content
            history.append([user_input, result])
            return "", history
            
        except Exception as e:
            error_msg = f"❌ Query failed: {str(e)}"
            history.append([user_input, error_msg])
            return "", history

# Global client instance
mongodb_client = MongoDBClient()

# Test queries for e-commerce analytics
TEST_QUERIES = {
    "🔌 Connection": [
        "Connect to MongoDB",
        "List all databases",
        "Show collections in ecommerce_analytics database"
    ],
    
    "👥 User Sessions": [
        "Show 5 user sessions",
        "Count total user sessions",
        "Find sessions from mobile devices",
        "Show sessions with cart activity",
        "Find sessions that converted to orders"
    ],
    
    "📱 Device & Traffic Analysis": [
        "Group sessions by device type",
        "Count sessions by traffic source",
        "Find Chrome browser sessions",
        "Show sessions from Mumbai",
        "Count bounce rate sessions"
    ],
    
    "⭐ Product Reviews": [
        "Show 5 product reviews",
        "Count total reviews",
        "Find 5-star reviews",
        "Show verified purchase reviews",
        "Find reviews with helpful votes > 10"
    ],
    
    "📊 Review Analytics": [
        "Calculate average rating across all reviews",
        "Group reviews by rating",
        "Count reviews by sentiment",
        "Find reviews with images",
        "Show most helpful reviews"
    ],
    
    "📢 Marketing Campaigns": [
        "Show 5 marketing campaigns",
        "Find active campaigns",
        "Show email campaigns",
        "Find campaigns with budget > 100000",
        "Show campaigns targeting Mumbai"
    ],
    
    "💰 Campaign Performance": [
        "Calculate average CTR for campaigns",
        "Find campaigns with conversion rate > 5%",
        "Show campaigns with highest ROI",
        "Group campaigns by type",
        "Calculate total marketing spend"
    ],
    
    "🔍 Advanced Analytics": [
        "Find top performing products by review count",
        "Show user journey from session to conversion",
        "Calculate customer acquisition cost by campaign",
        "Find seasonal trends in user sessions",
        "Analyze cart abandonment patterns"
    ]
}

def create_query_buttons():
    """Create sample query buttons"""
    buttons = []
    for category, queries in TEST_QUERIES.items():
        with gr.Accordion(category, open=False):
            for query in queries:
                btn = gr.Button(query, size="sm", variant="secondary")
                buttons.append((btn, query))
    return buttons

async def handle_connect():
    return await mongodb_client.connect()

async def handle_query(query, history):
    return await mongodb_client.query(query, history)

def load_query(query_text):
    return query_text

# Gradio Interface
with gr.Blocks(theme=gr.themes.Soft(), title="MongoDB E-commerce Analytics") as app:
    
    gr.Markdown("""
    # 🛒 MongoDB E-commerce Analytics Client
    **Interactive MongoDB MCP Client for E-commerce Data Analysis**
    
    **User:** `hriteshMaikap` | **Database:** `ecommerce_analytics` | **Server:** `localhost:8002`
    """)
    
    with gr.Row():
        # Main interface
        with gr.Column(scale=3):
            # Connection section
            with gr.Group():
                gr.Markdown("### 🔌 Connection")
                connect_btn = gr.Button("Connect to MongoDB Server", variant="primary")
                status = gr.Textbox(label="Status", value="Not connected", interactive=False)
            
            # Query interface
            with gr.Group():
                gr.Markdown("### 💬 Query Interface")
                chatbot = gr.Chatbot(
                    label="Query Results", 
                    height=400,
                    show_copy_button=True
                )
                
                with gr.Row():
                    query_box = gr.Textbox(
                        label="MongoDB Query",
                        placeholder="e.g., 'Show user sessions from mobile devices'",
                        scale=4
                    )
                    execute_btn = gr.Button("Execute", variant="primary", scale=1)
        
        # Sample queries
        with gr.Column(scale=2):
            gr.Markdown("### 📝 Sample Queries")
            gr.Markdown("Click to load into query box:")
            query_buttons = create_query_buttons()
    
    # Event handlers
    connect_btn.click(handle_connect, outputs=[status])
    execute_btn.click(handle_query, inputs=[query_box, chatbot], outputs=[query_box, chatbot])
    query_box.submit(handle_query, inputs=[query_box, chatbot], outputs=[query_box, chatbot])
    
    # Sample query handlers
    for btn, query in query_buttons:
        btn.click(load_query, inputs=[gr.State(query)], outputs=[query_box])

if __name__ == "__main__":
    print("🚀 Starting MongoDB E-commerce Analytics Client")
    print("📡 Ensure MongoDB MCP server is running on localhost:8002")
    
    app.launch(
        server_name="0.0.0.0",
        server_port=7860,
        share=False
    )