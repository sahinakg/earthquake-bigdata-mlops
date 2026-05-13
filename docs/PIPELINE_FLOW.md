# Pipeline Flow

## Çalışma sırası

| Sıra | Komut | Açıklama |
|---:|---|---|
| 1 | `make up` | Docker servislerini başlatır |
| 2 | `make create-topic` | Kafka topic oluşturur |
| 3 | `make producer` | CSV'den Kafka'ya event gönderir |
| 4 | `make bronze` | Kafka stream -> Bronze Delta |
| 5 | `make silver` | Bronze -> Silver + Quarantine |
| 6 | `make gold` | Silver -> Gold feature ve aggregate tablolar |
| 7 | `make quality-report` | Veri kalite raporu üretir |
| 8 | `make train-regression` | Magnitude regression modellerini eğitir |
| 9 | `make train-classification` | Tsunami classification modellerini eğitir |
| 10 | `make inference` | En iyi MLflow run'larından model yükleyip tahmin üretir |
| 11 | `make evaluate` | MLflow run özetini raporlar |

## Job detayları

| Job | Input | Output | Çalışma tipi |
|---|---|---|---|
| `01_stream_to_bronze.py` | Kafka `earthquake-events` | `data/delta/bronze/earthquakes` | Structured Streaming |
| `02_bronze_to_silver.py` | Bronze Delta | Silver Delta + Quarantine | Structured Streaming |
| `03_silver_to_gold.py` | Silver Delta | Gold Delta tablolar | Batch |
| `04_data_quality_report.py` | Silver veya Bronze Delta | JSON/CSV kalite raporu | Batch |
| `train_magnitude_regression.py` | Gold feature table | MLflow run + model | Batch ML |
| `train_tsunami_classification.py` | Gold tsunami risk table | MLflow run + model | Batch ML |
| `batch_inference.py` | Gold feature table + MLflow model | Prediction Delta tablolar | Batch inference |

## Streaming trigger davranışı

Varsayılan olarak `.env` içinde:

```text
STREAM_TRIGGER_AVAILABLE_NOW=true
```

Bu ayar Structured Streaming job'larının mevcut veriyi okuyup bitince kapanmasını sağlar. Sürekli çalışan streaming job isterseniz:

```text
STREAM_TRIGGER_AVAILABLE_NOW=false
STREAM_PROCESSING_TIME=10 seconds
```

sonra:

```bash
make bronze STREAM_TRIGGER_AVAILABLE_NOW=false
```

## Checkpoint mantığı

Checkpoint path'leri:

```text
data/checkpoints/bronze/earthquakes
data/checkpoints/silver/earthquakes_clean
```

Spark job yeniden başlatıldığında aynı checkpoint path kullanılırsa önceki state ve offset bilgisi korunur.
