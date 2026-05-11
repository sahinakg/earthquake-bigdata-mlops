from src.common.schemas import BRONZE_COLUMNS, RAW_CSV_COLUMNS, get_bronze_columns, get_raw_event_fields


def test_raw_schema_contains_expected_columns() -> None:
    expected = [
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
    assert get_raw_event_fields() == expected
    assert RAW_CSV_COLUMNS == expected


def test_bronze_schema_contains_producer_and_kafka_columns() -> None:
    columns = get_bronze_columns()
    for required in ["event_id", "ingested_at", "source_file", "kafka_topic", "kafka_partition", "kafka_offset", "kafka_timestamp"]:
        assert required in columns
    assert columns == BRONZE_COLUMNS
