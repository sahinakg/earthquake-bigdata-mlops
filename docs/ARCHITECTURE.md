# Mimari

Bu proje lakehouse ve MLOps pratiklerini aynı repo içinde gösterir.

## Genel akış

```text
CSV Dataset
    |
    v
Kafka Producer
    |
    v
Kafka Topic: earthquake-events
    |
    v
Spark Structured Streaming
    |
    v
Delta Lake Bronze
    |
    v
Spark Cleaning & Validation
    |
    v
Delta Lake Silver
    |
    +----> Delta Lake Quarantine
    |
    v
Feature Engineering & Aggregation
    |
    v
Delta Lake Gold
    |
    v
ML Training + MLflow
    |
    v
Batch Inference + Prediction Delta Tables
```

## Servis mimarisi

```text
+---------------------+       +-------------------+
|  CSV file           |       |  Kafka UI         |
|  data/raw/*.csv     |       |  localhost:8085   |
+----------+----------+       +---------+---------+
           |                            |
           v                            v
+----------+----------+       +---------+---------+
| Python Producer     +------>+ Kafka Broker      |
| src/producer        |       | kafka:29092       |
+---------------------+       +---------+---------+
                                      |
                                      v
+---------------------+       +---------+---------+
| Spark Master        |<------+ Spark Worker      |
| localhost:8080      |       | executor          |
+----------+----------+       +-------------------+
           |
           v
+----------+----------+
| Delta Lake on volume|
| data/delta          |
+----------+----------+
           |
           v
+----------+----------+       +-------------------+
| Spark ML Training   +------>+ MLflow Server     |
| src/ml              |       | localhost:5000    |
+---------------------+       +---------+---------+
                                      |
                         +------------+------------+
                         |                         |
                         v                         v
                  +------+-------+         +-------+------+
                  | PostgreSQL   |         | MinIO        |
                  | metadata     |         | artifacts    |
                  +--------------+         +--------------+
```

## Tasarım kararları

| Karar | Gerekçe |
|---|---|
| Kafka KRaft mode | Lokal geliştirme için ZooKeeper'sız daha sade yapı |
| Delta local volume | Üniversite/demo ortamında S3 gerektirmeden lakehouse davranışı |
| MinIO | MLflow artifact store için S3 benzeri endüstriyel pratik |
| PostgreSQL | MLflow backend metadata için kalıcı ve sorgulanabilir store |
| Structured Streaming + checkpoint | Yeniden başlatılabilir streaming işleme |
| Bronze/Silver/Gold | Ham, temiz ve analitik veriyi net ayırma |
| Spark ML Pipeline | Feature dönüşümü ve modelin birlikte saklanması |

## Veri dayanıklılığı

- Kafka offsetleri checkpoint ile takip edilir.
- Bronze append-only mantıkta çalışır.
- Silver duplicate yönetimi için `record_natural_key` kullanır.
- Quarantine path geçersiz kayıtları silmeden saklar.
- MLflow run'ları PostgreSQL ve MinIO ile kalıcıdır.
