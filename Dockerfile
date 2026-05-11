# =============================================================================
# Unified Logistics Data Platform - Multi-stage Dockerfile
# =============================================================================
# Stage 1: dashboard - lightweight image for cloud deploys (Render, Cloud Run,
#          Railway, Fly). Bundles the sample dataset so the app renders without
#          any external infra.
# Stage 2: full      - full platform image with Java/Spark/dbt deps for
#          development and the live-stack walkthrough.
# =============================================================================

# --- Stage: Dashboard Only (default) ---
FROM python:3.11-slim AS dashboard

WORKDIR /app

# curl is only used by the HEALTHCHECK below. Keeping the install minimal.
RUN apt-get update \
 && apt-get install -y --no-install-recommends curl \
 && rm -rf /var/lib/apt/lists/*

# Install Python dependencies first so the layer caches across code changes.
COPY requirements-streamlit.txt .
RUN pip install --no-cache-dir -r requirements-streamlit.txt

# Application code, sample dataset, Streamlit config.
COPY src/dashboard/ src/dashboard/
COPY data/sample/ data/sample/
COPY .streamlit/ .streamlit/
COPY streamlit_app.py .

# Run as an unprivileged user.
RUN useradd --create-home --uid 1001 streamlit \
 && chown -R streamlit:streamlit /app
USER streamlit

# PORT is injected by most cloud platforms. Default to 8501 so
# `docker run -p 8501:8501 ...` still works locally.
ENV PORT=8501 \
    PYTHONUNBUFFERED=1 \
    STREAMLIT_BROWSER_GATHER_USAGE_STATS=false \
    STREAMLIT_SERVER_HEADLESS=true
EXPOSE 8501

HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
    CMD curl --fail "http://localhost:${PORT:-8501}/_stcore/health" || exit 1

# Shell form so $PORT expands at container start, not build time.
ENTRYPOINT ["sh", "-c", "exec streamlit run src/dashboard/app.py --server.port=${PORT:-8501} --server.address=0.0.0.0"]


# --- Stage: Full Platform ---
FROM python:3.11-slim AS full

WORKDIR /app

# System dependencies for Spark/Kafka.
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    curl \
 && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Python dependencies.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# All source code.
COPY . .

RUN mkdir -p data/bronze data/silver data/gold data/checkpoints data/quality_reports data/warehouse

ENV PORT=8501 \
    PYTHONUNBUFFERED=1 \
    STREAMLIT_BROWSER_GATHER_USAGE_STATS=false \
    STREAMLIT_SERVER_HEADLESS=true
EXPOSE 8501 8080 8081

HEALTHCHECK --interval=30s --timeout=5s --start-period=45s --retries=3 \
    CMD curl --fail "http://localhost:${PORT:-8501}/_stcore/health" || exit 1

# Default: run the dashboard. Shell form so $PORT expands at runtime.
CMD ["sh", "-c", "exec streamlit run src/dashboard/app.py --server.port=${PORT:-8501} --server.address=0.0.0.0"]
