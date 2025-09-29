"""
Clean HTTP wrapper for the Natural Language SQL MCP Server.
Only includes the endpoints actually used by the frontend.
"""

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Dict, Any, Optional
from contextlib import asynccontextmanager
import asyncio
import sys
import os
import traceback
import urllib.parse

# Add the project root to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import only the tools we actually use
from src.tools.connection import connect_database, get_connection_status
from src.tools.query import query_data

# Request/Response models
class QueryRequest(BaseModel):
    natural_language_query: str

class DatabaseConnectionRequest(BaseModel):
    uri: str
    database_type: str = "auto"

class MockContext:
    def __init__(self, session_id: str = "http_session"):
        self.session_id = session_id

    async def info(self, message: str):
        print(f"INFO: {message}")

    async def warning(self, message: str):
        print(f"WARNING: {message}")

    async def error(self, message: str):
        print(f"ERROR: {message}")

# Lifespan event handler
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    print("=" * 60)
    print("🚀 NATURAL LANGUAGE SQL HTTP API v2.0.0")
    print("=" * 60)
    print("✅ FastAPI HTTP server initialized")
    print("✅ CORS middleware configured")
    print("✅ Essential endpoints only")
    print("📖 API Documentation available at: /docs")
    print("=" * 60)
    yield
    # Shutdown
    print("👋 Shutting down HTTP API server")

app = FastAPI(
    title="Natural Language SQL API",
    version="2.0.0",
    description="Minimal HTTP wrapper for Natural Language SQL",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://localhost:5174", "http://127.0.0.1:5173", "http://127.0.0.1:5174"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/health")
async def health_check():
    return {"status": "healthy", "version": "2.0.0", "service": "Natural Language SQL API"}

# Database endpoints
@app.get("/api/database/status")
async def get_connection_status_endpoint():
    try:
        ctx = MockContext()
        result = await get_connection_status(ctx)
        return {"success": True, "result": result}
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.post("/api/database/connect")
async def connect_database_endpoint(request: DatabaseConnectionRequest):
    try:
        ctx = MockContext()
        
        # Parse database URI to extract connection details
        parsed = urllib.parse.urlparse(request.uri)
        
        connection_params = {
            "host": parsed.hostname or "localhost",
            "port": parsed.port,
            "username": parsed.username,
            "password": parsed.password,
            "database_name": parsed.path.lstrip('/') if parsed.path else None,
            "db_type": request.database_type if request.database_type != "auto" else parsed.scheme
        }
        
        # Map common URI schemes to our database types
        scheme_mapping = {
            "postgresql": "postgresql",
            "postgres": "postgresql", 
            "mysql": "mysql",
            "sqlite": "sqlite"
        }
        
        if connection_params["db_type"] in scheme_mapping:
            connection_params["db_type"] = scheme_mapping[connection_params["db_type"]]
        
        result = await connect_database(ctx, **connection_params)
        return {"success": True, "result": result}
    except Exception as e:
        print(f"Connection error: {str(e)}")
        traceback.print_exc()
        return {"success": False, "error": str(e)}

# Query endpoints
@app.post("/api/query/execute")
async def execute_query_endpoint(request: QueryRequest):
    try:
        ctx = MockContext()
        result = await query_data(ctx, natural_language_query=request.natural_language_query)
        return {"success": True, "result": result}
    except Exception as e:
        print(f"Query error: {str(e)}")
        traceback.print_exc()
        return {"success": False, "error": str(e)}

if __name__ == "__main__":
    import uvicorn
    print("🚀 Starting Natural Language SQL HTTP API")
    uvicorn.run(app, host="0.0.0.0", port=8000)
