# Data Dictionary

## Ham CSV kolonları

| Kolon | Tip / örnek | Açıklama |
|---|---|---|
| `time` | long string | Epoch milisaniye değeri |
| `place` | string | Lokasyon açıklaması |
| `status` | string | USGS benzeri kaynaklarda review durumu |
| `tsunami` | int 0/1 | Tsunami ilişkisi göstergesi |
| `significance` | int | Olay önem skoru |
| `data_type` | string | Olay tipi, örnek: `earthquake` |
| `magnitudo` | double | Deprem büyüklüğü; kolon adı ham veriyle birebir korunur |
| `state` | string | Bölge/eyalet/ülke alanı |
| `longitude` | double | Boylam, -180 ile 180 arasında beklenir |
| `latitude` | double | Enlem, -90 ile 90 arasında beklenir |
| `depth` | double | Derinlik; negatif değer silinmez, kalite bayrağıyla işaretlenir |
| `date` | timestamp string | Olay zamanı |

## Producer metadata kolonları

| Kolon | Açıklama |
|---|---|
| `event_id` | Satır kimliğine göre deterministik UUID |
| `ingested_at` | Producer'ın event'i Kafka'ya gönderdiği UTC zaman |
| `source_file` | Kaynak CSV dosya adı |

## Kafka metadata kolonları

| Kolon | Açıklama |
|---|---|
| `kafka_topic` | Event'in geldiği Kafka topic |
| `kafka_partition` | Kafka partition |
| `kafka_offset` | Kafka offset |
| `kafka_timestamp` | Kafka message timestamp |

## Silver kalite ve türetilmiş alanlar

| Kolon | Açıklama |
|---|---|
| `event_timestamp` | `date` veya epoch `time` üzerinden üretilen timestamp |
| `year` | Olay yılı |
| `month` | Olay ayı |
| `day` | Olay günü |
| `hour` | Olay saati |
| `record_natural_key` | `time/place/date` üzerinden SHA-256 doğal anahtar |
| `invalid_latitude` | Latitude null veya [-90, 90] dışında mı? |
| `invalid_longitude` | Longitude null veya [-180, 180] dışında mı? |
| `missing_magnitudo` | `magnitudo` null mı? |
| `missing_depth` | `depth` null mı? |
| `negative_depth` | `depth` negatif mi? |
| `quality_score` | 0-100 arasında basit kalite skoru |
| `is_valid_record` | ML/analitik için kullanılabilir temel geçerlilik bayrağı |

## Gold feature alanları

| Kolon | Açıklama |
|---|---|
| `abs_latitude` | Mutlak enlem |
| `abs_longitude` | Mutlak boylam |
| `depth_bucket` | `negative`, `shallow`, `medium`, `deep` |
| `magnitude_bucket` | `minor`, `light`, `moderate`, `strong`, `major` |
| `is_shallow` | Derinlik 0-70 km aralığında mı? |
| `is_medium_depth` | Derinlik 70-300 km aralığında mı? |
| `is_deep` | Derinlik 300 km ve üstü mü? |
| `is_high_significance` | Significance >= 500 mü? |
| `is_tsunami` | Tsunami == 1 mi? |
| `state_index` | Spark ML StringIndexer ile üretilmiş state index |

## Prediction kolonları

| Kolon | Açıklama |
|---|---|
| `prediction` | Model tahmini |
| `probability_tsunami` | Classification için tsunami olasılığı |
| `model_name` | Tahmini üreten model ailesi |
| `run_id` | MLflow run ID |
| `model_version` | Bu projede run_id ile aynı tutulur |
| `predicted_at` | Tahmin üretim zamanı |
