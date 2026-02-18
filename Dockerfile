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

EXPOSE 8501

HEALTHCHECK CMD curl --fail http://localhost:8501/_stcore/health || exit 1

ENTRYPOINT ["streamlit", "run", "src/dashboard/app.py", \
    "--server.port=8501", \
    "--server.address=0.0.0.0", \
    "--server.headless=true"]


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

EXPOSE 8501 8080 8081

# Default: run the dashboard
CMD ["streamlit", "run", "src/dashboard/app.py", \
    "--server.port=8501", \
    "--server.address=0.0.0.0", \
    "--server.headless=true"]
