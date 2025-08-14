# ===== Builder Stage =====
FROM python:3.12-slim AS builder

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PATH="/root/.local/bin:$PATH"

WORKDIR /app

# Install system dependencies and poetry in a single layer
RUN apt-get update && apt-get install -y \
    curl \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/* \
    && curl -sSL https://install.python-poetry.org | python3 -

COPY pyproject.toml poetry.lock* ./
RUN poetry install --no-interaction --no-root


# ===== Runtime Stage =====
FROM python:3.12-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PATH="/root/.local/bin:$PATH"

WORKDIR /app

# Install only runtime dependencies in a single layer
RUN apt-get update && apt-get install -y \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy poetry and dependencies from builder
COPY --from=builder /root/.local /root/.local
COPY --from=builder /root/.cache /root/.cache
COPY --from=builder /app /app

# Copy application code
COPY . .

# Use an entrypoint script for better flexibility
ENTRYPOINT ["poetry", "run", "python3"]
CMD ["main.py"]