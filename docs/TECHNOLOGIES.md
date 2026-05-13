# Teknolojiler

## Docker nedir?

Docker, uygulamaları ve bağımlılıklarını container adı verilen izole ortamlarda çalıştırmayı sağlar. Bu projede Kafka, Spark, MLflow, PostgreSQL, MinIO ve Jupyter local makineye ayrı ayrı kurulmaz; hepsi container içinde çalışır.

## Docker Compose nedir?

Docker Compose birden fazla container'ı tek YAML dosyasıyla yönetir. `docker-compose.yml` dosyası bu projedeki servisleri, portları, environment variable'ları, network ve volume ayarlarını tanımlar.

## Kafka nedir?

Kafka olay akışı platformudur. Producer event yazar, consumer event okur. Bu projede CSV satırları Kafka mesajlarına dönüştürülür.

## Producer nedir?

Producer Kafka'ya mesaj gönderen uygulamadır. Bu projedeki producer `src/producer/kafka_producer.py` dosyasıdır. CSV'yi satır satır okur ve JSON event üretir.

## Topic nedir?

Topic Kafka'da mesajların yazıldığı mantıksal kanaldır. Bu projede topic adı `earthquake-events` olarak belirlenmiştir.

## Consumer nedir?

Consumer Kafka topic'inden mesaj okuyan uygulamadır. Bu projede Spark Structured Streaming job'ı consumer rolünü üstlenir.

## Spark nedir?

Apache Spark dağıtık veri işleme motorudur. Büyük veri setlerini paralel olarak işler. Bu projede Spark hem streaming ETL hem de ML eğitimi için kullanılır.

## Spark Structured Streaming nedir?

Structured Streaming Spark DataFrame API ile streaming veri işlemeyi sağlar. Kafka topic'inden stream okur, mikro-batch yaklaşımıyla Delta Lake'e yazar.

## Delta Lake nedir?

Delta Lake data lake üzerinde ACID transaction, schema management ve time travel gibi özellikler sağlayan tablo formatıdır. Bu projede Bronze, Silver, Gold ve prediction tabloları Delta formatındadır.

## Bronze/Silver/Gold mimarisi nedir?

- **Bronze**: Ham veri, minimum müdahale.
- **Silver**: Temizlenmiş, tipleri düzeltilmiş, kalite kontrolleri yapılmış veri.
- **Gold**: Analitik ve makine öğrenmesi için optimize edilmiş veri.

## MLflow nedir?

MLflow makine öğrenmesi deneylerini takip etmek için kullanılır. Parametre, metrik, model ve artifact loglar.

## Model tracking nedir?

Model tracking, hangi veriyle, hangi parametrelerle, hangi modelin eğitildiğini ve hangi metrikleri verdiğini kayıt altına alma sürecidir.

## Artifact nedir?

Artifact, bir run sırasında üretilen dosyadır. Örnek: model dosyası, confusion matrix CSV, feature importance CSV, prediction sample CSV.

## Experiment nedir?

Experiment, aynı problem için yapılan run'ların mantıksal grubudur. Bu projede iki experiment vardır:

- `earthquake_magnitude_regression`
- `earthquake_tsunami_classification`

## Run nedir?

Run, tek bir model eğitimi veya deney çalıştırmasıdır. Her run parametre, metrik ve artifact içerir.
