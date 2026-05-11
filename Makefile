SHELL := /bin/bash

COMPOSE := docker compose
SPARK_PACKAGES := io.delta:delta-spark_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1
SPARK_SUBMIT := $(COMPOSE) exec -T spark-master spark-submit --master spark://spark-master:7077 --packages $(SPARK_PACKAGES)
TOPIC ?= earthquake-events
PARTITIONS ?= 3
REPLICATION ?= 1
PRODUCER_MAX_ROWS ?= 1000
PRODUCER_LOOP ?= false
STREAM_TRIGGER_AVAILABLE_NOW ?= true

.PHONY: up down restart logs create-topic producer bronze silver gold train-regression train-classification inference quality-report evaluate all clean-data ps

up:
	$(COMPOSE) up -d --build kafka kafka-ui postgres minio minio-create-bucket mlflow spark-master spark-worker jupyter

ps:
	$(COMPOSE) ps

down:
	$(COMPOSE) down

restart: down up

logs:
	$(COMPOSE) logs -f --tail=200

create-topic:
	$(COMPOSE) exec -T kafka kafka-topics --bootstrap-server kafka:29092 --create --if-not-exists --topic $(TOPIC) --partitions $(PARTITIONS) --replication-factor $(REPLICATION)
	$(COMPOSE) exec -T kafka kafka-topics --bootstrap-server kafka:29092 --describe --topic $(TOPIC)

producer:
	$(COMPOSE) run --rm -e PRODUCER_MAX_ROWS=$(PRODUCER_MAX_ROWS) -e PRODUCER_LOOP=$(PRODUCER_LOOP) producer

bronze:
	$(COMPOSE) exec -T -e STREAM_TRIGGER_AVAILABLE_NOW=$(STREAM_TRIGGER_AVAILABLE_NOW) spark-master spark-submit --master spark://spark-master:7077 --packages $(SPARK_PACKAGES) /app/src/spark/jobs/01_stream_to_bronze.py

silver:
	$(COMPOSE) exec -T -e STREAM_TRIGGER_AVAILABLE_NOW=$(STREAM_TRIGGER_AVAILABLE_NOW) spark-master spark-submit --master spark://spark-master:7077 --packages $(SPARK_PACKAGES) /app/src/spark/jobs/02_bronze_to_silver.py

gold:
	$(SPARK_SUBMIT) /app/src/spark/jobs/03_silver_to_gold.py

quality-report:
	$(SPARK_SUBMIT) /app/src/spark/jobs/04_data_quality_report.py

train-regression:
	$(SPARK_SUBMIT) /app/src/ml/train_magnitude_regression.py

train-classification:
	$(SPARK_SUBMIT) /app/src/ml/train_tsunami_classification.py

inference:
	$(SPARK_SUBMIT) /app/src/ml/batch_inference.py

evaluate:
	$(SPARK_SUBMIT) /app/src/ml/evaluate_models.py

all: up create-topic producer bronze silver gold quality-report train-regression train-classification inference evaluate
	@echo "Pipeline tamamlandı. Arayüzler: Kafka UI http://localhost:8085 | Spark UI http://localhost:8080 | MLflow http://localhost:5000 | MinIO http://localhost:9001 | Jupyter http://localhost:8888"

clean-data:
	rm -rf data/delta/* data/checkpoints/* data/reports/*
	touch data/delta/.gitkeep data/checkpoints/.gitkeep data/reports/.gitkeep
