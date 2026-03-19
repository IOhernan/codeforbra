from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import os

app = FastAPI(
    title="Clone Full Backend",
    description="Backend compatible 926 - Motor de migración",
    version="1.0.0"
)

# Configurar CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Variables de entorno
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://admin:admin@clone_full_postgres:5432/migracion")

@app.get("/")
def read_root():
    return {
        "status": "Clone Full Backend running",
        "version": "1.0.0",
        "mode": "compatibility_926",
        "database": DATABASE_URL
    }

@app.get("/health")
def health_check():
    return {
        "status": "healthy",
        "service": "clone_full_backend",
        "version": "1.0.0",
        "mode": "compatibility_926"
    }

@app.get("/config")
def get_config():
    return {
        "database_url": DATABASE_URL,
        "mode": "compatibility_926",
    }

@app.post("/api/test")
def test_endpoint():
    return {"message": "Test endpoint working", "mode": "compatibility_926"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
