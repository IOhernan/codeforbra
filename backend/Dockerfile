FROM python:3.11-slim

WORKDIR /app

# Instalar dependencias del sistema
RUN apt-get update && apt-get install -y \
    gcc \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Crear archivo requirements.txt si no existe
RUN echo "fastapi==0.104.1\nuvicorn==0.24.0\npsycopg2-binary==2.9.9\npydantic==2.5.0" > requirements.txt

# Instalar dependencias Python
RUN pip install --no-cache-dir -r requirements.txt

# Crear archivo main.py dummy si no existe
RUN echo "from fastapi import FastAPI\n\napp = FastAPI()\n\n@app.get('/')\ndef read_root():\n    return {'status': 'Backend running'}" > main.py

EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]
