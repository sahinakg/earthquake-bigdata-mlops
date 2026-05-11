from __future__ import annotations

import csv
import json
import logging
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

try:
    from confluent_kafka import KafkaException, Producer
except ImportError:
    KafkaException = Exception  # type: ignore
    Producer = None  # type: ignore

from src.common.config import get_config, require_file
from src.common.logging_utils import configure_logging
from src.common.schemas import RAW_CSV_COLUMNS

LOGGER = logging.getLogger(__name__)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def normalize_row(row: Dict[str, Any]) -> Dict[str, Optional[str]]:
    normalized: Dict[str, Optional[str]] = {}
    for column in RAW_CSV_COLUMNS:
        value = row.get(column)
        if value is None:
            normalized[column] = None
        else:
            text = str(value).strip()
            normalized[column] = text if text != "" else None
    return normalized


def build_event(row: Dict[str, Any], source_file: str, row_number: Optional[int] = None) -> Dict[str, Any]:
    event = normalize_row(row)
    stable_identity = "|".join(
        [
            source_file,
            str(row_number) if row_number is not None else "unknown-row",
            str(event.get("time") or ""),
            str(event.get("place") or ""),
            str(event.get("date") or ""),
        ]
    )
    event["event_id"] = str(uuid.uuid5(uuid.NAMESPACE_URL, stable_identity))
    event["ingested_at"] = utc_now_iso()
    event["source_file"] = source_file
    return event


def iter_csv_events(csv_path: str, max_rows: int = 0) -> Iterable[Dict[str, Any]]:
    source_file = Path(csv_path).name
    with open(csv_path, "r", encoding="utf-8", newline="") as file_obj:
        reader = csv.DictReader(file_obj)
        missing_columns = set(RAW_CSV_COLUMNS) - set(reader.fieldnames or [])
        if missing_columns:
            raise ValueError(f"CSV missing expected columns: {sorted(missing_columns)}")
        for row_number, row in enumerate(reader, start=1):
            if max_rows > 0 and row_number > max_rows:
                break
            yield build_event(row=row, source_file=source_file, row_number=row_number)


def delivery_callback(error: Optional[KafkaException], message: Any) -> None:
    if error is not None:
        LOGGER.error("Kafka delivery failed: %s", error)
    else:
        LOGGER.debug(
            "Delivered event to topic=%s partition=%s offset=%s",
            message.topic(),
            message.partition(),
            message.offset(),
        )


def create_producer(bootstrap_servers: str) -> Producer:
    if Producer is None:
        raise RuntimeError("confluent-kafka is required to run the producer. Install requirements/producer-requirements.txt")
    return Producer(
        {
            "bootstrap.servers": bootstrap_servers,
            "client.id": "earthquake-csv-producer",
            "acks": "all",
            "enable.idempotence": True,
            "linger.ms": 20,
            "retries": 10,
            "retry.backoff.ms": 500,
            "compression.type": "snappy",
        }
    )


def wait_for_kafka(producer: Producer, retries: int = 30, sleep_seconds: float = 2.0) -> None:
    for attempt in range(1, retries + 1):
        try:
            producer.list_topics(timeout=5)
            LOGGER.info("Kafka is reachable.")
            return
        except Exception as exc:
            LOGGER.warning("Kafka not ready yet. attempt=%s/%s error=%s", attempt, retries, exc)
            time.sleep(sleep_seconds)
    raise RuntimeError("Kafka was not reachable after retry attempts.")


def event_key(event: Dict[str, Any], key_field: str) -> str:
    value = event.get(key_field) or event.get("event_id")
    return str(value)


def send_events_once(
    producer: Producer,
    csv_path: str,
    topic: str,
    sleep_seconds: float,
    max_rows: int,
    key_field: str,
    log_every: int,
) -> int:
    sent = 0
    for event in iter_csv_events(csv_path=csv_path, max_rows=max_rows):
        payload = json.dumps(event, ensure_ascii=False, separators=(",", ":"))
        producer.produce(
            topic=topic,
            key=event_key(event, key_field).encode("utf-8"),
            value=payload.encode("utf-8"),
            callback=delivery_callback,
        )
        producer.poll(0)
        sent += 1
        if log_every > 0 and sent % log_every == 0:
            LOGGER.info("Produced %s events to topic=%s", sent, topic)
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)
    producer.flush(timeout=60)
    return sent


def main() -> None:
    configure_logging()
    config = get_config()
    require_file(
        config.csv_path,
        message=(
            f"CSV file not found at {config.csv_path}. "
            "Kaggle dosyasını data/raw/Eartquakes-1990-2023.csv konumuna koyun."
        ),
    )

    producer = create_producer(config.kafka_bootstrap_servers)
    wait_for_kafka(producer)

    total_sent = 0
    loop_index = 0
    while True:
        loop_index += 1
        LOGGER.info(
            "Starting producer loop=%s csv=%s topic=%s max_rows=%s sleep=%s",
            loop_index,
            config.csv_path,
            config.kafka_topic,
            config.producer_max_rows,
            config.producer_sleep_seconds,
        )
        sent = send_events_once(
            producer=producer,
            csv_path=config.csv_path,
            topic=config.kafka_topic,
            sleep_seconds=config.producer_sleep_seconds,
            max_rows=config.producer_max_rows,
            key_field=config.producer_key_field,
            log_every=config.producer_log_every,
        )
        total_sent += sent
        LOGGER.info("Producer loop completed. sent=%s total_sent=%s", sent, total_sent)
        if not config.producer_loop:
            break


if __name__ == "__main__":
    main()
