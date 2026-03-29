# ──────────────────────────────────────────────────────────────────────────
# BookStore Microservice — Multi-stage Dockerfile
#
# Shared across all 6 services. Build with:
#   docker build -t bookstore/books-svc --build-arg SERVICE=books .
#   docker build -t bookstore/cart-svc --build-arg SERVICE=cart .
#   docker build -t bookstore/orders-svc --build-arg SERVICE=orders .
#   docker build -t bookstore/search-svc --build-arg SERVICE=search .
#   docker build -t bookstore/social-svc --build-arg SERVICE=social .
#   docker build -t bookstore/sync-svc --build-arg SERVICE=sync .
#
# KEY DIFFERENCE from Lambda:
#   Lambda: AWS manages the runtime, packaging (layers/zip), and lifecycle.
#   Container: You own the runtime, dependencies, and process lifecycle.
#   Trade-off: more control + portability, but you manage the image.
# ──────────────────────────────────────────────────────────────────────────

# ── Stage 1: Dependencies ────────────────────────────────────────────────
FROM python:3.12-slim AS builder

WORKDIR /app

# Install dependencies first (layer caching — rebuild only when requirements change)
COPY requirements.txt .
RUN pip install --no-cache-dir --target=/app/deps -r requirements.txt

# ── Stage 2: Runtime ─────────────────────────────────────────────────────
FROM python:3.12-slim

# Build argument: which service to run
ARG SERVICE=books
ENV SERVICE=${SERVICE}

# Non-root user for security
RUN groupadd -r bookstore && useradd -r -g bookstore bookstore

WORKDIR /app

# Copy dependencies from builder
COPY --from=builder /app/deps /app/deps
ENV PYTHONPATH=/app/deps

# Copy service code
COPY services/${SERVICE}/ /app/

# Health check for Kubernetes
HEALTHCHECK --interval=15s --timeout=5s --start-period=10s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/health')" || exit 1

# Run as non-root
USER bookstore

# Uvicorn with production settings
# KEY DIFFERENCE from Lambda:
#   Lambda: single-threaded, one request at a time per invocation
#   Container: multiple workers, concurrent request handling, persistent connections
EXPOSE 8000
CMD ["python", "-m", "uvicorn", "main:app", \
     "--host", "0.0.0.0", \
     "--port", "8000", \
     "--workers", "2", \
     "--loop", "uvloop", \
     "--http", "httptools", \
     "--access-log"]
