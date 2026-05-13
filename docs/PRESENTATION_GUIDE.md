# Presentation Guide

Bu doküman projeyi hocaya veya sınıfa anlatmak için önerilen sunum akışını verir.

## 1. Problem

Deprem verileri yıllara yayılan, olay bazlı ve hacimli verilerdir. Amaç bu veriyi modern büyük veri mimarisiyle işlemek, temizlemek, analitik hale getirmek ve makine öğrenmesi modeli üretmektir.

## 2. Veri seti

Veri seti 1990-2023 yılları arasındaki deprem kayıtlarını içerir. Önemli kolonlar:

- `date`
- `place`
- `state`
- `latitude`
- `longitude`
- `depth`
- `magnitudo`
- `tsunami`
- `significance`

Ham veri adı ve `magnitudo` kolonu özellikle değiştirilmemiştir.

## 3. Mimari

ASCII mimariyi gösterin:

```text
CSV -> Producer -> Kafka -> Spark Streaming -> Bronze -> Silver -> Gold -> MLflow + Models -> Predictions
```

Bu noktada her bileşenin neden gerekli olduğunu açıklayın.

## 4. Kafka streaming

Producer CSV'yi satır satır okur ve Kafka topic'ine JSON event gönderir. Mesaj key'i varsayılan olarak `state` alanıdır. Kafka UI'da topic, partition ve mesajlar gösterilebilir.

Demo:

```bash
make create-topic
make producer PRODUCER_MAX_ROWS=1000
```

## 5. Spark processing

Spark Kafka'dan stream okur. Structured Streaming mikro-batch yaklaşımıyla çalışır. Checkpoint sayesinde job yeniden başlatıldığında kaldığı yerden devam edebilir.

Demo:

```bash
make bronze
make silver
make gold
```

## 6. Delta Lake katmanları

- Bronze: Ham event + Kafka metadata
- Silver: Temizlenmiş veri + kalite flagleri
- Gold: Feature ve aggregate tablolar

Quarantine alanını özellikle vurgulayın: Kötü kayıtlar silinmez, ayrı path'e yazılır.

## 7. ML modelleri

Regression modeli `magnitudo` tahmin eder. Classification modeli `tsunami` riskini tahmin eder. İki problem tipi gösterildiği için proje daha güçlü görünür.

## 8. MLflow deney takibi

MLflow UI'da şunları gösterin:

- Experiment isimleri
- Run parametreleri
- RMSE/MAE/R2
- Accuracy/Precision/Recall/F1/ROC-AUC
- Model artifactleri
- Confusion matrix
- Prediction sample CSV

## 9. Demo akışı

Önerilen canlı demo:

```bash
make up
make create-topic
make producer PRODUCER_MAX_ROWS=1000
make bronze
make silver
make gold
make quality-report
make train-regression
make train-classification
make inference
```

Ardından şu arayüzleri açın:

- Kafka UI: `http://localhost:8085`
- Spark UI: `http://localhost:8080`
- MLflow UI: `http://localhost:5000`
- MinIO UI: `http://localhost:9001`
- Jupyter: `http://localhost:8888`

## 10. Sonuçlar

Sunum sonunda şunları vurgulayın:

- Proje sadece CSV analizi değil, streaming + lakehouse + MLflow içeren uçtan uca sistemdir.
- Veri kalitesi ve invalid kayıt yönetimi vardır.
- ML tarafında sadece accuracy değil, problem tipine uygun metrikler loglanmıştır.
- Model inference çıktısı da Delta Lake'e yazılır.

## 11. Geliştirme önerileri

- Airflow ile orchestration
- Model registry stage yönetimi
- Streaming inference
- Geospatial feature engineering
- Dashboard ve monitoring
- Data quality framework entegrasyonu
