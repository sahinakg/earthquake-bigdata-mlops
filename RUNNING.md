# RUNNING.md - Çalıştırma Kılavuzu

Bu dosya sadece projeyi ayağa kaldırma ve çalıştırma adımlarına odaklanır.

## 1. Gereksinimler

- Docker Desktop
- Docker Compose v2
- En az 8 GB RAM önerilir
- En az 10 GB boş disk alanı önerilir
- Kaggle veri seti: `All the Earthquakes Dataset : from 1990-2023`

## 2. Repo klonlama

```bash
git clone <repo-url>
cd earthquake-bigdata-mlops
```

Bu proje size ZIP olarak geldiyse klasörü açıp repo kök dizinine girmeniz yeterlidir.

## 3. CSV dosyasını data/raw içine koyma

Kaggle'dan indirilen CSV dosyasını şu path'e koyun:

```text
data/raw/Eartquakes-1990-2023.csv
```

Dosya adı özellikle `Eartquakes-1990-2023.csv` olmalıdır.

## 4. .env dosyasını oluşturma

```bash
cp .env.example .env
```

İlk deneme için `.env` dosyasını değiştirmeniz gerekmez.

## 5. Docker servislerini başlatma

```bash
make up
```

Alternatif:

```bash
docker compose up -d --build kafka kafka-ui postgres minio minio-create-bucket mlflow spark-master spark-worker jupyter
```

Servisleri kontrol edin:

```bash
docker compose ps
```

## 6. Kafka topic oluşturma

```bash
make create-topic
```

Topic adı:

```text
earthquake-events
```

## 7. Producer çalıştırma

Kısa demo için:

```bash
make producer PRODUCER_MAX_ROWS=1000 PRODUCER_LOOP=false
```

Daha yavaş streaming simülasyonu için `.env` içinde `PRODUCER_SLEEP_SECONDS` değerini artırabilirsiniz.

Sürekli loop için:

```bash
make producer PRODUCER_MAX_ROWS=0 PRODUCER_LOOP=true
```

`PRODUCER_MAX_ROWS=0` tüm CSV'yi okur.

## 8. Spark Bronze job çalıştırma

```bash
make bronze
```

Bu job Kafka'dan event okur ve Bronze Delta tablosuna yazar.

Bronze path:

```text
data/delta/bronze/earthquakes
```

Checkpoint path:

```text
data/checkpoints/bronze/earthquakes
```

## 9. Spark Silver job çalıştırma

```bash
make silver
```

Silver path:

```text
data/delta/silver/earthquakes_clean
```

Invalid/quarantine path:

```text
data/delta/quarantine/earthquakes_invalid
```

## 10. Spark Gold job çalıştırma

```bash
make gold
```

Gold path'leri:

```text
data/delta/gold/earthquake_features
data/delta/gold/earthquake_daily_aggregates
data/delta/gold/earthquake_state_aggregates
data/delta/gold/earthquake_tsunami_risk_dataset
```

## 11. MLflow arayüzünü açma

Tarayıcı:

```text
http://localhost:5000
```

İlk model eğitiminden sonra experimentler burada görünür.

## 12. Model eğitme

Regression modeli:

```bash
make train-regression
```

Classification modeli:

```bash
make train-classification
```

## 13. Batch inference çalıştırma

```bash
make inference
```

Çıktılar:

```text
data/delta/predictions/magnitude_predictions
data/delta/predictions/tsunami_predictions
```

## 14. Delta çıktılarını kontrol etme

Jupyter içinde veya Spark shell ile Delta path'lerini okuyabilirsiniz.

Örnek notebook:

```text
notebooks/01_eda_earthquakes.ipynb
```

## 15. Kafka UI kontrolü

Tarayıcı:

```text
http://localhost:8085
```

Kontrol edilecekler:

- `earthquake-events` topic'i var mı?
- Partition sayısı 3 mü?
- Mesaj sayısı artıyor mu?
- Consumer group offsetleri görünüyor mu?

## 16. Spark UI kontrolü

Tarayıcı:

```text
http://localhost:8080
```

Kontrol edilecekler:

- Spark worker bağlı mı?
- Running/completed application var mı?
- Executor ve stage bilgileri görünüyor mu?

## 17. MLflow UI kontrolü

Tarayıcı:

```text
http://localhost:5000
```

Kontrol edilecekler:

- `earthquake_magnitude_regression`
- `earthquake_tsunami_classification`
- Run metrikleri
- Model artifactleri
- Confusion matrix ve prediction sample artifactleri

## 18. Servisleri durdurma

```bash
make down
```

Volume'ları da silmek isterseniz:

```bash
docker compose down -v
```

## 19. Temizlik komutları

Sadece Delta/checkpoint/report çıktılarını silmek için:

```bash
make clean-data
```

Docker volume temizliği için:

```bash
docker compose down -v
```

Dikkat: `down -v` PostgreSQL ve MinIO verilerini de siler. MLflow run geçmişi kaybolur.

## 20. Sık hatalar ve çözümleri

### Port çakışması

Hata: `bind: address already in use`.

Çözüm: İlgili portu kullanan uygulamayı kapatın veya `docker-compose.yml` port mapping değerlerini değiştirin.

### Kafka bağlantı hatası

Çözüm:

```bash
docker compose ps kafka
make logs
```

Kafka healthy olmadan producer çalıştırmayın.

### Topic bulunamadı

Çözüm:

```bash
make create-topic
```

### Spark package uyumsuzluğu

Çözüm: İlk çalıştırmada Spark Maven paketlerini indirir. İnternet bağlantınız yoksa package indirme başarısız olabilir.

### Delta Lake class not found

Çözüm:

```bash
docker compose down
make up
make bronze
```

### CSV dosyası bulunamadı

Çözüm: Dosya adının birebir şu olduğundan emin olun:

```text
Eartquakes-1990-2023.csv
```

### MLflow bağlantı hatası

Çözüm:

```bash
docker compose ps mlflow postgres minio
make logs
```

### Memory yetersizliği

Çözüm: Docker Desktop memory ayarını artırın. İlk çalıştırmada `PRODUCER_MAX_ROWS=1000` kullanın.

### Windows path sorunları

Çözüm: WSL2 backend kullanın ve projeyi boşluk içermeyen kısa bir path altında tutun.

### Docker Desktop çalışmıyor

Çözüm:

```bash
docker info
```

Bu komut hata veriyorsa Docker Desktop'ı başlatın.
