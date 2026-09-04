FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

COPY data_generator/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY data_generator/ ./data_generator/

ENV PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

CMD ["python", "-m", "data_generator.main"]
