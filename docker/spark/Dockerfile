FROM python:3.11-slim-bookworm

RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jdk-headless \
    procps \
    curl \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY requirements/spark-requirements.txt /tmp/spark-requirements.txt
COPY requirements/ml-requirements.txt /tmp/ml-requirements.txt

RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r /tmp/spark-requirements.txt -r /tmp/ml-requirements.txt

ENV SPARK_HOME=/usr/local/lib/python3.11/site-packages/pyspark
ENV PATH=$SPARK_HOME/bin:$PATH

COPY src /app/src

USER root
