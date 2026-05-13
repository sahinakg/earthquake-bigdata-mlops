from __future__ import annotations

import logging
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.common.config import get_config
from src.common.logging_utils import configure_logging
from src.spark.spark_session import create_spark_session
from src.spark.transformations import clean_silver_df
from src.utils.path_utils import ensure_dir

LOGGER = logging.getLogger(__name__)


def delta_exists(spark, path: str) -> bool:
    try:
        return DeltaTable.isDeltaTable(spark, path)
    except Exception:
        return Path(path).exists() and any(Path(path).glob("_delta_log/*"))


def merge_valid_records(valid_df: DataFrame, path: str) -> None:
    spark = valid_df.sparkSession
    deduped = valid_df.dropDuplicates(["record_natural_key"])
    if deduped.rdd.isEmpty():
        return

    if delta_exists(spark, path):
        delta_table = DeltaTable.forPath(spark, path)
        (
            delta_table.alias("target")
            .merge(
                deduped.alias("source"),
                "target.record_natural_key = source.record_natural_key OR target.event_id = source.event_id",
            )
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        deduped.write.format("delta").mode("append").option("mergeSchema", "true").save(path)


def write_silver_batch(batch_df: DataFrame, batch_id: int, silver_path: str, quarantine_path: str) -> None:
    if batch_df.rdd.isEmpty():
        LOGGER.info("Silver batch_id=%s is empty", batch_id)
        return

    valid_df = batch_df.filter(F.col("is_valid_record"))
    invalid_df = batch_df.filter(~F.col("is_valid_record"))

    valid_count = valid_df.count()
    invalid_count = invalid_df.count()
    LOGGER.info("Silver batch_id=%s valid=%s invalid=%s", batch_id, valid_count, invalid_count)

    if valid_count > 0:
        merge_valid_records(valid_df, silver_path)
    if invalid_count > 0:
        invalid_df.write.format("delta").mode("append").option("mergeSchema", "true").save(quarantine_path)


def main() -> None:
    configure_logging()
    config = get_config()
    ensure_dir(config.silver_checkpoint_path)

    spark = create_spark_session("earthquake_bronze_to_silver")
    if not delta_exists(spark, config.bronze_path):
        raise FileNotFoundError(
            f"Bronze Delta table not found: {config.bronze_path}. Run make bronze after producing Kafka events."
        )

    bronze_stream = spark.readStream.format("delta").load(config.bronze_path)
    cleaned_stream = clean_silver_df(bronze_stream)

    writer = (
        cleaned_stream.writeStream.foreachBatch(
            lambda batch_df, batch_id: write_silver_batch(
                batch_df=batch_df,
                batch_id=batch_id,
                silver_path=config.silver_path,
                quarantine_path=config.quarantine_path,
            )
        )
        .option("checkpointLocation", config.silver_checkpoint_path)
        .queryName("earthquake_bronze_to_silver")
    )

    if config.stream_trigger_available_now:
        writer = writer.trigger(availableNow=True)
    else:
        writer = writer.trigger(processingTime=config.stream_processing_time)

    query = writer.start()
    query.awaitTermination()


if __name__ == "__main__":
    main()
