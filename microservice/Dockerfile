FROM python:3.12-slim


ENV PYTHONUNBUFFERED=1
WORKDIR /app

COPY . .

RUN pip install --no-cache-dir clickhouse-driver prometheus_client requests 

CMD ["python", "simulate_data.py"]
