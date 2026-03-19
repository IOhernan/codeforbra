from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import os

app = FastAPI(
    title="AFI Backend",
    description="Backend principal del sistema AFI",
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
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://afi_user:afi_pass@afi_postgres:5432/afiliaciones")
QDRANT_URL = os.getenv("QDRANT_URL", "http://afi_qdrant:6333")
OLLAMA_URL = os.getenv("OLLAMA_URL", "http://afi_ollama:11434")

@app.get("/")
def read_root():
    return {
        "status": "Backend AFI running",
        "version": "1.0.0",
        "database": DATABASE_URL,
        "qdrant": QDRANT_URL,
        "ollama": OLLAMA_URL
    }

@app.get("/health")
def health_check():
    return {
        "status": "healthy",
        "service": "afi_backend",
        "version": "1.0.0"
    }

@app.get("/config")
def get_config():
    return {
        "database_url": DATABASE_URL,
        "qdrant_url": QDRANT_URL,
        "ollama_url": OLLAMA_URL,
        "model_name": os.getenv("MODEL_NAME", "llama3.2:1b"),
        "embedding_model": os.getenv("EMBEDDING_MODEL", "nomic-embed-text"),
    }

@app.post("/api/test")
def test_endpoint():
    return {"message": "Test endpoint working"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
