# =============================================================================
# Unified Logistics Data Platform - Multi-stage Dockerfile
# =============================================================================
# Stage 1: Dashboard (lightweight, for deployment)
# Stage 2: Full platform (development/demo)
# =============================================================================

# --- Stage: Dashboard Only (default) ---
FROM python:3.11-slim AS dashboard

WORKDIR /app

# Install dashboard dependencies only
COPY requirements-streamlit.txt .
RUN pip install --no-cache-dir -r requirements-streamlit.txt

# Copy application code and sample data
COPY src/dashboard/ src/dashboard/
COPY data/sample/ data/sample/

# Streamlit config
COPY .streamlit/ .streamlit/

# PORT is injected by most cloud platforms (Render, Cloud Run, Railway, Fly).
# Default to 8501 so `docker run -p 8501:8501 ...` still works locally.
ENV PORT=8501
EXPOSE 8501

HEALTHCHECK CMD curl --fail http://localhost:${PORT:-8501}/_stcore/health || exit 1

# Use shell form so $PORT is expanded at container start (not build time).
ENTRYPOINT ["sh", "-c", "streamlit run src/dashboard/app.py --server.port=${PORT:-8501} --server.address=0.0.0.0 --server.headless=true"]


# --- Stage: Full Platform ---
FROM python:3.11-slim AS full

WORKDIR /app

# System dependencies for Spark/Kafka
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    curl \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy all source code
COPY . .

# Create data directories
RUN mkdir -p data/bronze data/silver data/gold data/checkpoints data/quality_reports data/warehouse

ENV PORT=8501
EXPOSE 8501 8080 8081

# Default: run the dashboard. Shell form so $PORT expands at runtime.
CMD ["sh", "-c", "streamlit run src/dashboard/app.py --server.port=${PORT:-8501} --server.address=0.0.0.0 --server.headless=true"]
