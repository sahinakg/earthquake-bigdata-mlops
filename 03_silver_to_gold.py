from __future__ import annotations

import logging
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.ml.feature import StringIndexer
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.common.config import get_config
from src.common.logging_utils import configure_logging
from src.common.schemas import GOLD_FEATURE_COLUMNS
from src.spark.spark_session import create_spark_session
from src.spark.transformations import add_gold_features

LOGGER = logging.getLogger(__name__)


def delta_exists(spark, path: str) -> bool:
    try:
        return DeltaTable.isDeltaTable(spark, path)
    except Exception:
        return Path(path).exists() and any(Path(path).glob("_delta_log/*"))


def write_delta(df: DataFrame, path: str, partition_cols: list[str] | None = None) -> None:
    writer = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.save(path)
    LOGGER.info("Wrote Delta table path=%s rows=%s", path, df.count())


def build_feature_table(silver_df: DataFrame) -> DataFrame:
    base = (
        silver_df.filter(F.col("is_valid_record"))
        .filter(F.col("event_timestamp").isNotNull())
        .filter(F.col("magnitudo").isNotNull())
        .withColumn("state", F.coalesce(F.col("state"), F.lit("unknown")))
    )
    features = add_gold_features(base)
    indexer = StringIndexer(inputCol="state", outputCol="state_index", handleInvalid="keep")
    indexed = indexer.fit(features).transform(features)
    return indexed.select(*GOLD_FEATURE_COLUMNS)


def build_daily_aggregates(features_df: DataFrame) -> DataFrame:
    return (
        features_df.withColumn("date_day", F.to_date(F.col("event_timestamp")))
        .groupBy("date_day")
        .agg(
            F.count("*").alias("event_count"),
            F.avg("magnitudo").alias("avg_magnitude"),
            F.max("magnitudo").alias("max_magnitude"),
            F.min("magnitudo").alias("min_magnitude"),
            F.avg("depth").alias("avg_depth"),
            F.max("depth").alias("max_depth"),
            F.sum(F.when(F.col("tsunami") == 1, 1).otherwise(0)).alias("tsunami_count"),
            F.sum(F.when(F.col("is_high_significance"), 1).otherwise(0)).alias("high_significance_count"),
        )
    )


def build_state_aggregates(features_df: DataFrame) -> DataFrame:
    return (
        features_df.groupBy("state")
        .agg(
            F.count("*").alias("event_count"),
            F.avg("magnitudo").alias("avg_magnitude"),
            F.max("magnitudo").alias("max_magnitude"),
            F.avg("depth").alias("avg_depth"),
            F.sum(F.when(F.col("tsunami") == 1, 1).otherwise(0)).alias("tsunami_count"),
            F.min("event_timestamp").alias("first_event_time"),
            F.max("event_timestamp").alias("last_event_time"),
        )
        .orderBy(F.desc("event_count"))
    )


def build_tsunami_risk_dataset(features_df: DataFrame) -> DataFrame:
    return features_df.select(
        "event_id",
        "latitude",
        "longitude",
        "depth",
        "magnitudo",
        "significance",
        "month",
        "hour",
        "state",
        "state_index",
        "tsunami",
    ).filter(
        F.col("tsunami").isNotNull()
        & F.col("latitude").isNotNull()
        & F.col("longitude").isNotNull()
        & F.col("depth").isNotNull()
        & F.col("magnitudo").isNotNull()
        & F.col("significance").isNotNull()
        & F.col("month").isNotNull()
        & F.col("hour").isNotNull()
    )


def main() -> None:
    configure_logging()
    config = get_config()
    spark = create_spark_session("earthquake_silver_to_gold")

    if not delta_exists(spark, config.silver_path):
        raise FileNotFoundError(f"Silver Delta table not found: {config.silver_path}. Run make silver first.")

    silver_df = spark.read.format("delta").load(config.silver_path)
    features_df = build_feature_table(silver_df).cache()

    if features_df.rdd.isEmpty():
        raise RuntimeError("No valid rows are available for Gold table generation.")

    write_delta(features_df, config.gold_features_path, partition_cols=["year"])
    write_delta(build_daily_aggregates(features_df), config.gold_daily_aggregates_path)
    write_delta(build_state_aggregates(features_df), config.gold_state_aggregates_path)
    write_delta(build_tsunami_risk_dataset(features_df), config.gold_tsunami_risk_path)

    class_distribution = features_df.groupBy("tsunami").count().orderBy("tsunami")
    LOGGER.info("Tsunami class distribution:")
    class_distribution.show(truncate=False)

    features_df.unpersist()


if __name__ == "__main__":
    main()
