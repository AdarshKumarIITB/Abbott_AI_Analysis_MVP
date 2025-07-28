FROM python:3.10-slim

# 1. Set working dir
WORKDIR /app

# 2. Copy & install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 3. Copy your project
COPY src/ ./src/
COPY registry/semantic_layer/ ./registry/semantic_layer/

# 4. Default command (overrideable)
ENTRYPOINT ["python", "src/ingest.py"]
