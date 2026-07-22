FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY gateway/ ./gateway/
COPY loadbalancer/ ./loadbalancer/
COPY loadtest/ ./loadtest/

# Entrypoint is overridden per-service in docker-compose.yml
CMD ["uvicorn", "gateway.app:app", "--host", "0.0.0.0", "--port", "8000"]
