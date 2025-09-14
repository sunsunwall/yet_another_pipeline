# Use Python 3.12 slim image
FROM python:3.12-slim

# Install uv
RUN pip install uv

# Set workdir
WORKDIR /app

# Copy dependency files
COPY pyproject.toml uv.lock ./

# Sync dependencies
RUN uv sync --frozen --no-install-project

# Copy source code
COPY src/ ./src/

# Expose port for HTTP (if added later)
EXPOSE 8080

# Run the service with uvicorn
CMD uv run uvicorn src.api_client.app:app --host 0.0.0.0 --port $PORT