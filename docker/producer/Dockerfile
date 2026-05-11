FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

WORKDIR /app

COPY requirements/producer-requirements.txt /tmp/producer-requirements.txt
RUN pip install --no-cache-dir -r /tmp/producer-requirements.txt

COPY src /app/src

CMD ["python", "-m", "src.producer.kafka_producer"]
