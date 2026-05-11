FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /mlflow

RUN apt-get update && \
    apt-get install -y --no-install-recommends curl && \
    rm -rf /var/lib/apt/lists/*

COPY requirements/ml-requirements.txt /tmp/ml-requirements.txt
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r /tmp/ml-requirements.txt

EXPOSE 5000
