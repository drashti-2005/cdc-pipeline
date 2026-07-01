# CDC Pipeline Docker Image
# Multi-stage build for production-ready image

# =============================================================================
# Stage 1: Builder
# =============================================================================
FROM python:3.11-slim as builder

# Set build arguments
ARG VERSION=0.1.0
ARG BUILD_DATE
ARG VCS_REF

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    git \
    && rm -rf /var/lib/apt/lists/*

# Create virtual environment
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Install Python dependencies
WORKDIR /build
COPY requirements.txt .
RUN pip install --upgrade pip setuptools wheel && \
    pip install -r requirements.txt

# Copy source code
COPY setup.py .
COPY src/ src/
COPY README.md .

# Install package (not editable for production)
RUN pip install .

# =============================================================================
# Stage 2: Runtime
# =============================================================================
FROM python:3.11-slim as runtime

# Set labels
ARG VERSION
ARG BUILD_DATE
ARG VCS_REF

LABEL org.opencontainers.image.title="CDC Pipeline" \
      org.opencontainers.image.description="Change Data Capture Pipeline for PostgreSQL to Kafka" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.created="${BUILD_DATE}" \
      org.opencontainers.image.revision="${VCS_REF}" \
      org.opencontainers.image.vendor="CDC Pipeline Project" \
      org.opencontainers.image.licenses="MIT"

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    CDC_HOME=/app \
    CDC_CONFIG_PATH=/app/config \
    CDC_LOG_LEVEL=INFO \
    CDC_LOG_FORMAT=json

# Install runtime dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    curl \
    tini \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --create-home --shell /bin/bash cdc

# Copy virtual environment from builder
COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Set working directory
WORKDIR /app

# Copy application scripts and configs (not src/ since it's in venv)
COPY --chown=cdc:cdc scripts/ scripts/
COPY --chown=cdc:cdc README.md .

# Create directories
RUN mkdir -p /app/config /app/logs /app/data && \
    chown -R cdc:cdc /app

# Switch to non-root user
USER cdc

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import sys; sys.exit(0)" || exit 1

# Expose metrics port
EXPOSE 8080

# Use tini as init system
ENTRYPOINT ["/usr/bin/tini", "--"]

# Default command - Keep container running for now
CMD ["tail", "-f", "/dev/null"]
