from src.producer.kafka_producer import build_event
from src.common.schemas import RAW_CSV_COLUMNS


def test_build_event_adds_required_metadata() -> None:
    row = {
        "time": "631153353990",
        "place": "12 km NNW of Meadow Lakes, Alaska",
        "status": "reviewed",
        "tsunami": "0",
        "significance": "96",
        "data_type": "earthquake",
        "magnitudo": "2.5",
        "state": "Alaska",
        "longitude": "-149.6692",
        "latitude": "61.7302",
        "depth": "30.1",
        "date": "1990-01-01 00:22:33.990000+00:00",
    }
    event = build_event(row, source_file="Eartquakes-1990-2023.csv", row_number=1)
    for column in RAW_CSV_COLUMNS:
        assert column in event
    assert event["event_id"]
    assert event["ingested_at"]
    assert event["source_file"] == "Eartquakes-1990-2023.csv"
    assert event["magnitudo"] == "2.5"


def test_build_event_is_deterministic_for_same_row_identity() -> None:
    row = {column: "x" for column in RAW_CSV_COLUMNS}
    first = build_event(row, source_file="file.csv", row_number=10)
    second = build_event(row, source_file="file.csv", row_number=10)
    assert first["event_id"] == second["event_id"]
