from __future__ import annotations

import logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.common.config import get_config
from src.common.logging_utils import configure_logging
from src.common.schemas import BRONZE_COLUMNS, get_bronze_event_schema
from src.spark.spark_session import create_spark_session
from src.utils.path_utils import ensure_dir

LOGGER = logging.getLogger(__name__)


def parse_kafka_stream(kafka_df: DataFrame) -> DataFrame:
    schema = get_bronze_event_schema()
    raw = kafka_df.select(
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.col("key").cast("string").alias("kafka_key"),
        F.col("value").cast("string").alias("raw_json"),
    )
    parsed = raw.withColumn("parsed", F.from_json(F.col("raw_json"), schema))
    return parsed.select(
        F.col("parsed.time").alias("time"),
        F.col("parsed.place").alias("place"),
        F.col("parsed.status").alias("status"),
        F.col("parsed.tsunami").alias("tsunami"),
        F.col("parsed.significance").alias("significance"),
        F.col("parsed.data_type").alias("data_type"),
        F.col("parsed.magnitudo").alias("magnitudo"),
        F.col("parsed.state").alias("state"),
        F.col("parsed.longitude").alias("longitude"),
        F.col("parsed.latitude").alias("latitude"),
        F.col("parsed.depth").alias("depth"),
        F.col("parsed.date").alias("date"),
        F.col("parsed.event_id").alias("event_id"),
        F.col("parsed.ingested_at").alias("ingested_at"),
        F.col("parsed.source_file").alias("source_file"),
        F.col("kafka_topic"),
        F.col("kafka_partition"),
        F.col("kafka_offset"),
        F.col("kafka_timestamp"),
        F.col("raw_json"),
    )


def write_bronze_batch(batch_df: DataFrame, batch_id: int, bronze_path: str, bad_records_path: str) -> None:
    if batch_df.rdd.isEmpty():
        LOGGER.info("Bronze batch_id=%s is empty", batch_id)
        return

    valid_df = batch_df.filter(F.col("event_id").isNotNull()).select(*BRONZE_COLUMNS)
    invalid_df = batch_df.filter(F.col("event_id").isNull()).select(
        "raw_json",
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
    )

    valid_count = valid_df.count()
    invalid_count = invalid_df.count()
    LOGGER.info("Bronze batch_id=%s valid=%s invalid=%s", batch_id, valid_count, invalid_count)

    if valid_count > 0:
        valid_df.write.format("delta").mode("append").option("mergeSchema", "true").save(bronze_path)
    if invalid_count > 0:
        invalid_df.write.format("delta").mode("append").option("mergeSchema", "true").save(bad_records_path)


def main() -> None:
    configure_logging()
    config = get_config()
    ensure_dir(config.bronze_path)
    ensure_dir(config.bronze_checkpoint_path)

    spark = create_spark_session("earthquake_stream_to_bronze")
    LOGGER.info("Reading Kafka topic=%s servers=%s", config.kafka_topic, config.kafka_bootstrap_servers)

    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config.kafka_bootstrap_servers)
        .option("subscribe", config.kafka_topic)
        .option("startingOffsets", config.kafka_starting_offsets)
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed_df = parse_kafka_stream(kafka_df)

    writer = (
        parsed_df.writeStream.foreachBatch(
            lambda batch_df, batch_id: write_bronze_batch(
                batch_df=batch_df,
                batch_id=batch_id,
                bronze_path=config.bronze_path,
                bad_records_path=config.bronze_bad_records_path,
            )
        )
        .option("checkpointLocation", config.bronze_checkpoint_path)
        .queryName("earthquake_kafka_to_bronze")
    )

    if config.stream_trigger_available_now:
        writer = writer.trigger(availableNow=True)
    else:
        writer = writer.trigger(processingTime=config.stream_processing_time)

    query = writer.start()
    query.awaitTermination()


if __name__ == "__main__":
    main()
