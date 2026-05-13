import pytest

pyspark = pytest.importorskip("pyspark")

from pyspark.sql import SparkSession
from src.spark.transformations import clean_silver_df


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    session = SparkSession.builder.master("local[1]").appName("test_transformations").getOrCreate()
    yield session
    session.stop()


def test_clean_silver_df_casts_and_flags_records(spark: SparkSession) -> None:
    rows = [
        {
            "time": "631153353990",
            "place": "  Test Place  ",
            "status": " REVIEWED ",
            "tsunami": "0",
            "significance": "96",
            "data_type": " Earthquake ",
            "magnitudo": "2.5",
            "state": " Alaska ",
            "longitude": "-149.6692",
            "latitude": "61.7302",
            "depth": "30.1",
            "date": "1990-01-01 00:22:33.990000+00:00",
            "event_id": "event-1",
            "ingested_at": "2024-01-01T00:00:00+00:00",
            "source_file": "Eartquakes-1990-2023.csv",
            "kafka_topic": "earthquake-events",
            "kafka_partition": 0,
            "kafka_offset": 1,
            "kafka_timestamp": None,
        },
        {
            "time": "631153353991",
            "place": "Bad Place",
            "status": "reviewed",
            "tsunami": "0",
            "significance": "96",
            "data_type": "earthquake",
            "magnitudo": None,
            "state": "BadState",
            "longitude": "200",
            "latitude": "100",
            "depth": "-5",
            "date": "1990-01-01 00:22:33.990000+00:00",
            "event_id": "event-2",
            "ingested_at": "2024-01-01T00:00:00+00:00",
            "source_file": "Eartquakes-1990-2023.csv",
            "kafka_topic": "earthquake-events",
            "kafka_partition": 0,
            "kafka_offset": 2,
            "kafka_timestamp": None,
        },
    ]
    df = spark.createDataFrame(rows)
    cleaned = clean_silver_df(df)
    result = {row["event_id"]: row.asDict() for row in cleaned.collect()}

    assert result["event-1"]["status"] == "reviewed"
    assert result["event-1"]["data_type"] == "earthquake"
    assert result["event-1"]["state"] == "Alaska"
    assert result["event-1"]["is_valid_record"] is True
    assert result["event-2"]["invalid_latitude"] is True
    assert result["event-2"]["invalid_longitude"] is True
    assert result["event-2"]["missing_magnitudo"] is True
    assert result["event-2"]["negative_depth"] is True
    assert result["event-2"]["is_valid_record"] is False
