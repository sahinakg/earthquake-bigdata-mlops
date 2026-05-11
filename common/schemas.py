from __future__ import annotations

from typing import List

try:
    from pyspark.sql.types import (
        IntegerType,
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )
except ImportError:
    IntegerType = LongType = StringType = StructField = StructType = TimestampType = None  # type: ignore

RAW_CSV_COLUMNS: List[str] = [
    "time",
    "place",
    "status",
    "tsunami",
    "significance",
    "data_type",
    "magnitudo",
    "state",
    "longitude",
    "latitude",
    "depth",
    "date",
]

PRODUCER_COLUMNS: List[str] = ["event_id", "ingested_at", "source_file"]

KAFKA_METADATA_COLUMNS: List[str] = [
    "kafka_topic",
    "kafka_partition",
    "kafka_offset",
    "kafka_timestamp",
]

BRONZE_COLUMNS: List[str] = RAW_CSV_COLUMNS + PRODUCER_COLUMNS + KAFKA_METADATA_COLUMNS

SILVER_QUALITY_COLUMNS: List[str] = [
    "event_timestamp",
    "year",
    "month",
    "day",
    "hour",
    "record_natural_key",
    "invalid_latitude",
    "invalid_longitude",
    "missing_magnitudo",
    "missing_depth",
    "negative_depth",
    "quality_score",
    "is_valid_record",
]

GOLD_FEATURE_COLUMNS: List[str] = [
    "event_id",
    "event_timestamp",
    "year",
    "month",
    "day",
    "hour",
    "latitude",
    "longitude",
    "depth",
    "significance",
    "tsunami",
    "state",
    "status",
    "data_type",
    "magnitudo",
    "abs_latitude",
    "abs_longitude",
    "depth_bucket",
    "magnitude_bucket",
    "is_shallow",
    "is_medium_depth",
    "is_deep",
    "is_high_significance",
    "is_tsunami",
    "state_index",
]


def get_raw_event_fields() -> List[str]:
    return list(RAW_CSV_COLUMNS)


def get_bronze_columns() -> List[str]:
    return list(BRONZE_COLUMNS)


def get_bronze_event_schema():
    if StructType is None:
        raise RuntimeError("PySpark is required to build Spark schemas.")
    return StructType(
        [StructField(column, StringType(), True) for column in RAW_CSV_COLUMNS]
        + [
            StructField("event_id", StringType(), True),
            StructField("ingested_at", StringType(), True),
            StructField("source_file", StringType(), True),
        ]
    )


def get_expected_silver_columns() -> List[str]:
    return RAW_CSV_COLUMNS + PRODUCER_COLUMNS + KAFKA_METADATA_COLUMNS + SILVER_QUALITY_COLUMNS


def get_expected_gold_feature_columns() -> List[str]:
    return list(GOLD_FEATURE_COLUMNS)
