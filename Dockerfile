# --- Stage 1: Build React Frontend ---
FROM node:20-slim AS frontend-builder
WORKDIR /build

COPY frontend/package*.json ./
RUN npm ci --no-audit --no-fund

COPY frontend/ ./
RUN npm run build

# --- Stage 2: Python Runtime ---
FROM python:3.11-slim
WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app/ ./app/
COPY --from=frontend-builder /build/dist ./frontend/dist

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=10s --start-period=15s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--log-level", "info"]
