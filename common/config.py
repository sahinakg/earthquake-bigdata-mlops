from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


def _get_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _get_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        return default
    return int(value)


def _get_float(name: str, default: float) -> float:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        return default
    return float(value)


def _path(value: str) -> str:
    return str(Path(value).expanduser())


@dataclass(frozen=True)
class AppConfig:
    project_root: str = _path(os.getenv("PROJECT_ROOT", "/app"))

    kafka_bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    kafka_topic: str = os.getenv("KAFKA_TOPIC", "earthquake-events")
    kafka_starting_offsets: str = os.getenv("KAFKA_STARTING_OFFSETS", "earliest")

    csv_path: str = os.getenv("CSV_PATH", "/app/data/raw/Eartquakes-1990-2023.csv")
    producer_sleep_seconds: float = _get_float("PRODUCER_SLEEP_SECONDS", 0.05)
    producer_max_rows: int = _get_int("PRODUCER_MAX_ROWS", 1000)
    producer_loop: bool = _get_bool("PRODUCER_LOOP", False)
    producer_key_field: str = os.getenv("PRODUCER_KEY_FIELD", "state")
    producer_log_every: int = _get_int("PRODUCER_LOG_EVERY", 1000)

    stream_trigger_available_now: bool = _get_bool("STREAM_TRIGGER_AVAILABLE_NOW", True)
    stream_processing_time: str = os.getenv("STREAM_PROCESSING_TIME", "10 seconds")
    spark_sql_shuffle_partitions: int = _get_int("SPARK_SQL_SHUFFLE_PARTITIONS", 8)

    mlflow_tracking_uri: str = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
    train_test_split: float = _get_float("TRAIN_TEST_SPLIT", 0.8)
    random_seed: int = _get_int("RANDOM_SEED", 42)

    @property
    def data_dir(self) -> str:
        return f"{self.project_root}/data"

    @property
    def bronze_path(self) -> str:
        return f"{self.data_dir}/delta/bronze/earthquakes"

    @property
    def bronze_bad_records_path(self) -> str:
        return f"{self.data_dir}/delta/bronze_bad_records/earthquakes"

    @property
    def bronze_checkpoint_path(self) -> str:
        return f"{self.data_dir}/checkpoints/bronze/earthquakes"

    @property
    def silver_path(self) -> str:
        return f"{self.data_dir}/delta/silver/earthquakes_clean"

    @property
    def silver_checkpoint_path(self) -> str:
        return f"{self.data_dir}/checkpoints/silver/earthquakes_clean"

    @property
    def quarantine_path(self) -> str:
        return f"{self.data_dir}/delta/quarantine/earthquakes_invalid"

    @property
    def gold_features_path(self) -> str:
        return f"{self.data_dir}/delta/gold/earthquake_features"

    @property
    def gold_daily_aggregates_path(self) -> str:
        return f"{self.data_dir}/delta/gold/earthquake_daily_aggregates"

    @property
    def gold_state_aggregates_path(self) -> str:
        return f"{self.data_dir}/delta/gold/earthquake_state_aggregates"

    @property
    def gold_tsunami_risk_path(self) -> str:
        return f"{self.data_dir}/delta/gold/earthquake_tsunami_risk_dataset"

    @property
    def magnitude_predictions_path(self) -> str:
        return f"{self.data_dir}/delta/predictions/magnitude_predictions"

    @property
    def tsunami_predictions_path(self) -> str:
        return f"{self.data_dir}/delta/predictions/tsunami_predictions"

    @property
    def reports_dir(self) -> str:
        return f"{self.data_dir}/reports"


def get_config() -> AppConfig:
    return AppConfig()


def require_file(path: str, message: Optional[str] = None) -> None:
    if not Path(path).exists():
        raise FileNotFoundError(message or f"Required file not found: {path}")
