# 🚀 Natural Language SQL MCP Server

**Chat with your database using plain English!**

This is a Model Context Protocol (MCP) server that transforms natural language into SQL queries using AI. Instead of writing complex SQL, just ask questions like "Show me all users from New York" and get instant results.

## 🎯 What Is This Project?

This MCP server acts as an intelligent bridge between you and your SQL database. It:

- **Understands natural language** - Ask database questions in plain English
- **Generates SQL automatically** - Uses OpenAI GPT models to create accurate SQL queries  
- **Manages database connections** - Handles PostgreSQL connections securely
- **Provides schema awareness** - Understands your database structure for better queries
- **Includes safety features** - Prevents dangerous operations like bulk deletes
- **Integrates with Cursor IDE** - Works seamlessly as an MCP server

Perfect for developers, data analysts, and anyone who wants to query databases without writing SQL!

## 🛠️ Available MCP Tools

Once connected, you can use these tools through any MCP client (like Cursor IDE):

### 🔌 Connection Tools
- **`connect_database`** - Connect to your PostgreSQL database
- **`disconnect_database`** - Safely close database connection
- **`get_connection_status`** - Check if you're connected and see connection details

### 📊 Schema Discovery Tools  
- **`list_tables`** - See all tables in your database
- **`describe_table`** - Get detailed information about a specific table (columns, types, constraints)
- **`get_database_summary`** - Get an overview of your entire database structure

### 💬 Natural Language Query Tools
- **`query_data`** - Ask questions and get data back
  - *Example: "Show me all orders from the last 30 days"*
  - *Example: "Find customers who spent more than $500"*
  - *Example: "How many products are in stock?"*

### ✏️ Data Modification Tools
- **`add_data`** - Insert new records using natural language
  - *Example: "Add a new user named John with email john@company.com"*
- **`update_data`** - Modify existing records conversationally  
  - *Example: "Update user ID 123 to change their status to active"*
- **`delete_data`** - Remove records safely (requires specific criteria)
  - *Example: "Delete the order with ID 456"*

### 🧪 Utility Tools
- **`hello`** - Test server connection with a friendly greeting
- **`server_info`** - Get server configuration and status information

## 🚀 Quick Setup Guide

### Prerequisites
- **Python 3.9+** ([Download here](https://www.python.org/downloads/))
- **PostgreSQL database** ([Download here](https://www.postgresql.org/download/))
- **OpenAI API key** ([Get one here](https://platform.openai.com/api-keys))

### Step 1: Installation
```bash
# Clone the repository
git clone <your-repo-url>
cd db-rag

# Install dependencies  
pip install -r requirements.txt
```

### Step 2: Configuration
Create a `.env` file in the project root:

```bash
# Database settings
DB_HOST=localhost
DB_PORT=5432  
DB_USERNAME=postgres
DB_PASSWORD=your_database_password
DB_NAME=your_database_name
DB_TYPE=postgresql

# OpenAI settings
LLM_API_KEY=your_openai_api_key_here
LLM_MODEL=gpt-4o-mini

# Server settings  
MCP_SERVER_NAME=Natural Language SQL Server
MCP_HOST=127.0.0.1
MCP_PORT=8000
MCP_TRANSPORT=http
```

### Step 3: Start the Server
```bash
python src/server.py
```

You should see: `Starting Natural Language SQL Server on http://127.0.0.1:8000`

## 🔧 Using with Cursor IDE

### Setup in Cursor
1. **Open Cursor IDE**
2. **Go to Settings** → **MCP Servers** (or search for "MCP" in settings)
3. **Add new server**:
```bash
  {
    "mcpServers": {
      "natural-language-sql": {
        "name": "Natural Language SQL Server",
        "url": "http://localhost:8000/mcp",
        "transport": "http",
        "description": "Convert natural language to SQL queries",
        "enabled": true
      }
    }
  }
```
4. **Save and restart** Cursor IDE

### Example Usage in Cursor
Once connected, you can chat with Cursor and use the tools:

```bash
You: Connect to my database using the connect_database tool

AI: I'll connect to your database using the credentials from your configuration.

You: What tables do I have?

AI: Let me list all tables in your database using the list_tables tool.

You: Show me all users created in the last week

AI: I'll query for recent users using natural language with the query_data tool.

You: Add a new product called "Wireless Mouse" with price $29.99

AI: I'll add that product using the add_data tool with natural language.
```

## 🔒 Security Features

- **Automatic WHERE clause enforcement** - UPDATE and DELETE operations must specify conditions
- **SQL injection prevention** - All queries are parameterized and validated
- **Connection management** - Secure database connection handling
- **Environment variables** - Sensitive data stored in `.env` file (never commit this!)

## 📚 Example Natural Language Queries

### Questions (query_data):
- "How many orders were placed today?"
- "Show me the top 5 customers by total purchases"
- "Find all products with less than 10 items in stock"
- "What's the average order value this month?"

### Adding Data (add_data):
- "Create a new customer named Alice Johnson with email alice@example.com"
- "Add a product called Gaming Keyboard with price 89.99 in Electronics category"

### Updating Data (update_data):  
- "Set the status to 'shipped' for order number 12345"
- "Update all products in Electronics category to have 10% discount"

### Deleting Data (delete_data):
- "Remove the user with email test@example.com"
- "Delete all orders older than 2 years"

## 🐛 Troubleshooting

**Server won't start?**
- Check Python version: `python --version` (needs 3.9+)
- Install dependencies: `pip install -r requirements.txt`

**Can't connect to database?**
- Verify PostgreSQL is running: `brew services start postgresql` (macOS) or `sudo systemctl start postgresql` (Linux)
- Test connection manually: `psql -h localhost -U postgres`

**OpenAI errors?**
- Verify API key is correct
- Check you have credits in your OpenAI account

**Cursor IDE not showing the server?**
- Make sure server is running on http://localhost:8000
- Restart Cursor IDE completely
- Check MCP server configuration in Cursor settings

## 🎉 What's Next?

This server is **fully functional** and ready to use! Future enhancements may include:
- MySQL and SQLite support
- Advanced query capabilities  
- Web interface
- Enhanced security features

---

**Ready to chat with your database? Start the server and connect through Cursor IDE!** 🚀