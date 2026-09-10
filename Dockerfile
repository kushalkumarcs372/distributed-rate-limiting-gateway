FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY gateway/ ./gateway/
COPY loadbalancer/ ./loadbalancer/
COPY loadtest/ ./loadtest/

# Run as an unprivileged user instead of root
RUN useradd --create-home --uid 10001 app
USER app

# Entrypoint is overridden per-service in docker-compose.yml / k8s manifests
CMD ["uvicorn", "gateway.app:app", "--host", "0.0.0.0", "--port", "8000"]
