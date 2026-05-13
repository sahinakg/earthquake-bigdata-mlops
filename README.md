# Earthquake Big Data MLOps

## 1. Proje adı

**Earthquake Big Data MLOps**: 1990-2023 deprem verileri üzerinde Kafka, Spark Structured Streaming, Delta Lake, MLflow ve Docker tabanlı uçtan uca büyük veri projesi.

## 2. Proje özeti

Bu proje, Kaggle üzerindeki **All the Earthquakes Dataset: from 1990-2023** veri setini gerçek zamanlı veri akışı gibi simüle eder. CSV dosyasından satır satır okunan deprem kayıtları Kafka topic'ine JSON event olarak gönderilir. Spark Structured Streaming bu eventleri Kafka'dan okur, Bronze Delta Lake katmanına yazar, ardından Silver katmanında veri temizleme ve kalite kontrolleri yapar. Gold katmanında hem analitik tablolar hem de makine öğrenmesi feature tabloları üretilir. Spark ML ile iki model eğitilir:

- **Regression**: `magnitudo` tahmini
- **Classification**: `tsunami` riski tahmini

Tüm deneyler, metrikler, model artifact'leri, örnek tahmin çıktıları ve model diagnostic dosyaları MLflow ile takip edilir.

## 3. Problem tanımı

Deprem verileri yüksek hacimli, zamana bağlı ve olay bazlıdır. Bu nedenle büyük veri mimarisinde şu problemler birlikte ele alınır:

1. Büyük CSV verisini belleğe komple almadan üretmek.
2. Olayları Kafka ile akış mantığında taşımak.
3. Spark Structured Streaming ile tekrar başlatılabilir ve checkpoint destekli tüketim yapmak.
4. Ham, temizlenmiş ve analitik veriyi Delta Lake üzerinde ayrı lakehouse katmanlarında tutmak.
5. Veri kalitesi problemlerini silmeden işaretlemek ve problemli kayıtları quarantine alanına almak.
6. ML modellerini izlenebilir, metriklenebilir ve tekrar üretilebilir şekilde eğitmek.
7. Eğitilmiş modellerle batch inference çıktıları üretmek.

## 4. Veri seti açıklaması

Veri seti 1990-2023 yılları arasındaki deprem olaylarını içerir. CSV dosyası kullanıcı tarafından indirilir ve aşağıdaki konuma koyulur:

```text
data/raw/Eartquakes-1990-2023.csv
```

Dosya adındaki `Eartquakes` yazımı veri setindeki dosya adıyla uyumlu olması için aynen korunur. Ham kolondaki `magnitudo` adı da değiştirilmez.

## 5. Kullanılan kolonlar

Ham CSV kolonları:

| Kolon | Açıklama |
|---|---|
| `time` | Deprem olayının epoch milisaniye karşılığı |
| `place` | Depremin metinsel lokasyon açıklaması |
| `status` | Kaydın inceleme durumu |
| `tsunami` | Tsunami ilişkili olay göstergesi, 0/1 |
| `significance` | Olayın önem/skor bilgisi |
| `data_type` | Olay tipi, genellikle `earthquake` |
| `magnitudo` | Deprem büyüklüğü; ham kolon adı özellikle böyle korunur |
| `state` | Bölge/eyalet/ülke benzeri lokasyon alanı |
| `longitude` | Boylam |
| `latitude` | Enlem |
| `depth` | Derinlik |
| `date` | Timestamp metni |

Producer tarafından eklenen kolonlar:

| Kolon | Açıklama |
|---|---|
| `event_id` | Satır kimliğinden deterministik üretilen UUID |
| `ingested_at` | Kafka'ya gönderim zamanı |
| `source_file` | Kaynak dosya adı |

Kafka metadata kolonları:

| Kolon | Açıklama |
|---|---|
| `kafka_topic` | Kafka topic adı |
| `kafka_partition` | Partition numarası |
| `kafka_offset` | Topic içindeki offset |
| `kafka_timestamp` | Kafka mesaj timestamp'i |

## 6. Hedefler

- Uçtan uca Docker Compose ortamı kurmak.
- Kafka ile streaming veri üretmek.
- Spark Structured Streaming ile Kafka eventlerini tüketmek.
- Delta Lake üzerinde Bronze/Silver/Gold lakehouse mimarisi oluşturmak.
- Veri temizleme, tip dönüşümü, duplicate yönetimi ve kalite bayrakları üretmek.
- Regression ve classification modellerini Spark ML Pipeline ile eğitmek.
- MLflow ile deney, metrik ve artifact takibi yapmak.
- Batch inference ile Delta tahmin tabloları üretmek.
- Üniversite sunumuna uygun teknik dokümantasyon sağlamak.

## 7. Mimari

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
Delta Lake Silver + Quarantine
    |
    v
Feature Engineering & Aggregation
    |
    v
Delta Lake Gold
    |
    v
Spark ML Training + MLflow
    |
    v
Batch Inference + Prediction Delta Tables
```

Detaylı mimari açıklaması için `docs/ARCHITECTURE.md` dosyasına bakın.

## 8. Teknolojiler

| Teknoloji | Projedeki rolü |
|---|---|
| Docker | Tüm servisleri izole container'larda çalıştırır |
| Docker Compose | Kafka, Spark, MLflow, PostgreSQL, MinIO ve Jupyter servislerini birlikte yönetir |
| Apache Kafka | Deprem eventlerini topic üzerinden taşır |
| Kafka UI | Topic, partition, offset ve mesaj kontrolü sağlar |
| Apache Spark | Büyük veri işleme ve ML eğitim altyapısıdır |
| Spark Structured Streaming | Kafka'dan stream okur, checkpoint ile devamlılık sağlar |
| Delta Lake | ACID destekli lakehouse tablo formatıdır |
| MLflow | Deney, metrik, model ve artifact takibi yapar |
| PostgreSQL | MLflow backend store olarak deney metadata'sını saklar |
| MinIO | MLflow artifact store olarak model dosyalarını saklar |
| PySpark ML | Regression ve classification modellerini eğitir |
| Jupyter | Opsiyonel veri keşfi ve model incelemesi sağlar |

## 9. Docker servisleri

| Servis | Açıklama | Port |
|---|---|---|
| `kafka` | Kafka broker, KRaft mode | `localhost:9092` |
| `kafka-ui` | Kafka topic izleme | `http://localhost:8085` |
| `spark-master` | Spark master | `spark://spark-master:7077`, UI: `http://localhost:8080` |
| `spark-worker` | Spark worker | İç ağ |
| `postgres` | MLflow backend store | `localhost:5432` |
| `minio` | MLflow artifact store | API: `http://localhost:9000`, UI: `http://localhost:9001` |
| `mlflow` | MLflow Tracking Server | `http://localhost:5000` |
| `producer` | CSV -> Kafka producer | Profil ile çalışır |
| `jupyter` | Notebook ortamı | `http://localhost:8888` |

MinIO varsayılan kullanıcı bilgileri:

```text
username: minioadmin
password: minioadmin
```

## 10. Veri akışı

1. CSV dosyası `data/raw/Eartquakes-1990-2023.csv` altına konur.
2. Producer CSV'yi satır satır okur.
3. Her satır JSON event formatına çevrilir.
4. Event içine `event_id`, `ingested_at`, `source_file` eklenir.
5. Eventler `earthquake-events` Kafka topic'ine gönderilir.
6. Spark Structured Streaming Kafka topic'ini okur.
7. Ham eventler Bronze Delta tablosuna yazılır.
8. Bronze verisi Silver katmanında temizlenir ve doğrulanır.
9. Geçersiz kayıtlar quarantine Delta path'ine yazılır.
10. Gold katmanında feature ve aggregate tabloları oluşturulur.
11. Spark ML modelleri Gold verisiyle eğitilir.
12. MLflow metrikleri ve modelleri saklar.
13. Batch inference tahmin Delta tablolarını üretir.

## 11. Bronze/Silver/Gold katmanları

### Bronze

Path:

```text
data/delta/bronze/earthquakes
```

Bronze katmanı ham veriyi mümkün olduğunca bozmadan saklar. Sadece JSON parse edilir ve Kafka metadata kolonları eklenir. Hatalı JSON kayıtları şu path'e yazılır:

```text
data/delta/bronze_bad_records/earthquakes
```

### Silver

Path:

```text
data/delta/silver/earthquakes_clean
```

Silver katmanında:

- Tip dönüşümleri yapılır.
- `status` ve `data_type` lowercase yapılır.
- `state` ve `place` trim edilir.
- `year`, `month`, `day`, `hour` üretilir.
- Doğal kayıt anahtarı üretilir.
- Latitude/longitude kontrolleri yapılır.
- Null magnitude/depth kayıtları işaretlenir.
- Negatif depth silinmez, kalite bayrağıyla işaretlenir.
- `quality_score` ve `is_valid_record` üretilir.

Geçersiz kayıtlar:

```text
data/delta/quarantine/earthquakes_invalid
```

### Gold

Path'ler:

```text
data/delta/gold/earthquake_features
data/delta/gold/earthquake_daily_aggregates
data/delta/gold/earthquake_state_aggregates
data/delta/gold/earthquake_tsunami_risk_dataset
```

Gold katmanı ML ve analitik kullanım için hazırlanmıştır.

## 12. Kafka streaming mantığı

Kafka topic adı:

```text
earthquake-events
```

Topic özellikleri:

- Partition: `3`
- Replication factor: local ortamda `1`
- Event value: JSON
- Event key: varsayılan olarak `state`, yoksa `event_id`

Producer environment variable'ları:

```text
KAFKA_BOOTSTRAP_SERVERS
KAFKA_TOPIC
CSV_PATH
PRODUCER_SLEEP_SECONDS
PRODUCER_MAX_ROWS
PRODUCER_LOOP
```

Producer büyük CSV'yi belleğe komple almaz. `csv.DictReader` ile satır satır okur.

## 13. Spark processing mantığı

Spark job'ları:

| Dosya | Görev |
|---|---|
| `01_stream_to_bronze.py` | Kafka'dan stream okur, Bronze Delta'ya yazar |
| `02_bronze_to_silver.py` | Bronze Delta'yı stream olarak okur, temizler, Silver ve quarantine yazar |
| `03_silver_to_gold.py` | Gold feature ve aggregate tablolarını üretir |
| `04_data_quality_report.py` | Veri kalite raporu üretir |

Örnek Spark submit:

```bash
spark-submit \
  --master spark://spark-master:7077 \
  --packages io.delta:delta-spark_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  /app/src/spark/jobs/01_stream_to_bronze.py
```

Pratikte bu komutlar Makefile ile çalıştırılır.

## 14. Delta Lake mantığı

Delta Lake bu projede dosya tabanlı lakehouse tablo formatı olarak kullanılır. Delta tabloları `data/delta` altında tutulur. Checkpoint dizinleri `data/checkpoints` altındadır. Streaming job yeniden başlatıldığında checkpoint sayesinde kaldığı offset ve batch durumundan devam edebilir.

## 15. MLflow mantığı

MLflow üç ana bilgi saklar:

1. **Experiment metadata**: PostgreSQL içinde tutulur.
2. **Metrics/params/tags**: PostgreSQL backend store'a yazılır.
3. **Artifacts/model dosyaları**: MinIO üzerinde `mlflow-artifacts` bucket'ına yazılır.

MLflow UI:

```text
http://localhost:5000
```

## 16. Makine öğrenmesi modelleri

İki model ailesi vardır:

1. Magnitude regression
2. Tsunami classification

Her iki eğitim script'i Spark ML `Pipeline` kullanır. Pipeline içinde `StringIndexer`, `VectorAssembler` ve model stage'i yer alır.

## 17. Regression modeli

Dosya:

```text
src/ml/train_magnitude_regression.py
```

Target:

```text
magnitudo
```

Feature'lar:

```text
latitude, longitude, depth, significance, month, hour, tsunami, state_index_model
```

Modeller:

- `LinearRegression` baseline
- `RandomForestRegressor` ana model

Metrikler:

- RMSE
- MAE
- R2

Experiment adı:

```text
earthquake_magnitude_regression
```

## 18. Classification modeli

Dosya:

```text
src/ml/train_tsunami_classification.py
```

Target:

```text
tsunami
```

Feature'lar:

```text
latitude, longitude, depth, magnitudo, significance, month, hour, state_index_model
```

Modeller:

- `LogisticRegression` baseline
- `RandomForestClassifier` ana model

Class imbalance için train setinden class weight hesaplanır ve `weightCol` olarak modele verilir.

Experiment adı:

```text
earthquake_tsunami_classification
```

## 19. Metrikler

Regression:

| Metrik | Anlam |
|---|---|
| RMSE | Büyük hataları daha çok cezalandırır |
| MAE | Ortalama mutlak hata |
| R2 | Açıklanan varyans oranı |

Classification:

| Metrik | Anlam |
|---|---|
| Accuracy | Genel doğru tahmin oranı |
| Precision | Pozitif tahminlerin doğruluğu |
| Recall | Gerçek pozitifleri yakalama oranı |
| F1 | Precision ve recall harmonik ortalaması |
| ROC-AUC | Eşik bağımsız sınıflandırma performansı |

Tsunami sınıfı dengesiz olabileceği için accuracy tek başına yeterli değildir. Bu yüzden F1, precision, recall ve ROC-AUC de loglanır.

## 20. Tahmin çıktıları

Batch inference dosyası:

```text
src/ml/batch_inference.py
```

Prediction Delta path'leri:

```text
data/delta/predictions/magnitude_predictions
data/delta/predictions/tsunami_predictions
```

Tahmin kolonları:

- `event_id`
- `prediction`
- `probability_tsunami` classification için
- `model_name`
- `run_id`
- `model_version`
- `predicted_at`

## 21. Klasör yapısı

```text
earthquake-bigdata-mlops/
├── docker-compose.yml
├── .env.example
├── .gitignore
├── Makefile
├── README.md
├── RUNNING.md
├── requirements/
├── docker/
├── data/
├── src/
├── notebooks/
├── docs/
└── tests/
```

Detaylı ağaç için bu dokümanın üretildiği repo ağacına bakın.

## 22. Kurulum

1. Docker Desktop kurulu olmalı.
2. En az 8 GB RAM önerilir.
3. Repo kökünde `.env` oluşturulur:

```bash
cp .env.example .env
```

4. CSV dosyası şu konuma koyulur:

```text
data/raw/Eartquakes-1990-2023.csv
```

5. Servisler başlatılır:

```bash
make up
```

## 23. Çalıştırma

Adım adım çalıştırma:

```bash
make up
make create-topic
make producer PRODUCER_MAX_ROWS=1000 PRODUCER_LOOP=false
make bronze
make silver
make gold
make quality-report
make train-regression
make train-classification
make inference
make evaluate
```

Tek komuta yakın tam akış:

```bash
make all
```

## 24. Beklenen çıktılar

- Kafka UI'da `earthquake-events` topic'i görünür.
- Spark UI'da completed/running Spark application'ları görünür.
- `data/delta/bronze/earthquakes` altında Delta log ve parquet dosyaları oluşur.
- `data/delta/silver/earthquakes_clean` altında temiz veri oluşur.
- `data/delta/gold/*` altında Gold tabloları oluşur.
- `data/reports/data_quality_report.json` ve `.csv` oluşur.
- MLflow UI'da iki experiment ve model run'ları görünür.
- `data/delta/predictions/*` altında prediction tabloları oluşur.

## 25. Hata giderme

### Docker port çakışması

Belirti: `port is already allocated`.

Çözüm: 8085, 8080, 5000, 9000, 9001, 8888 portlarını kullanan uygulamaları kapatın veya `docker-compose.yml` port mapping değerlerini değiştirin.

### Kafka bağlantı hatası

Belirti: Producer `Kafka not ready yet` uyarısı verir.

Çözüm:

```bash
docker compose ps
make logs
make create-topic
```

### Topic bulunamadı

Çözüm:

```bash
make create-topic
```

### Spark package uyumsuzluğu

Belirti: Kafka source veya Delta class bulunamıyor.

Çözüm: Makefile içindeki `SPARK_PACKAGES` değerinin Spark sürümüyle uyumlu olduğundan emin olun.

### Delta Lake class not found

Belirti: `io.delta.sql.DeltaSparkSessionExtension` veya `DeltaCatalog` hatası.

Çözüm:

```bash
docker compose down
make up
make bronze
```

İlk çalıştırmada Maven package indirme biraz zaman alabilir.

### CSV dosyası bulunamadı

Belirti: Producer `CSV file not found` hatası verir.

Çözüm: Dosya adını birebir kontrol edin:

```text
data/raw/Eartquakes-1990-2023.csv
```

### MLflow bağlantı hatası

Çözüm:

```bash
docker compose ps mlflow postgres minio
make logs
```

MLflow UI: `http://localhost:5000`

### Memory yetersizliği

Çözüm: Docker Desktop memory limitini artırın. `.env` içinde:

```text
SPARK_WORKER_MEMORY=3G
SPARK_WORKER_CORES=2
```

Düşük makinelerde `PRODUCER_MAX_ROWS=1000` ile başlayın.

### Windows path sorunları

Çözüm: Projeyi kısa ve boşluksuz bir dizinde tutun. WSL2 backend kullanın. CSV dosyasının gerçekten `data/raw` altında olduğundan emin olun.

### Docker Desktop çalışmıyor

Çözüm: Docker Desktop'ı açın ve şu komutu kontrol edin:

```bash
docker info
```

## 26. Geliştirme önerileri

- Model registry ve stage yönetimi eklenebilir.
- Streaming inference job eklenebilir.
- Great Expectations veya Deequ benzeri veri kalite aracı entegre edilebilir.
- Airflow ile orchestration yapılabilir.
- Grafana/Prometheus ile monitoring eklenebilir.
- Feature Store yaklaşımı eklenebilir.
- Geo-spatial analiz için H3 index veya geohash üretilebilir.

## 27. Sunumda nasıl anlatılır

1. Deprem verisinin olay bazlı ve büyük hacimli olduğunu anlatın.
2. CSV'nin Kafka ile streaming eventlere dönüştürüldüğünü gösterin.
3. Kafka UI'da topic ve mesajları gösterin.
4. Spark job'larının Bronze/Silver/Gold dönüşümünü nasıl yaptığını anlatın.
5. Delta Lake'in checkpoint ve ACID tablo mantığını açıklayın.
6. Data quality raporunu açın.
7. MLflow UI'da regression ve classification experimentlerini gösterin.
8. Batch inference çıktılarının Delta tablosu olarak yazıldığını gösterin.
9. Sonuç olarak projenin sadece demo değil, izlenebilir ve tekrar çalıştırılabilir bir lakehouse + MLOps pipeline olduğunu vurgulayın.
