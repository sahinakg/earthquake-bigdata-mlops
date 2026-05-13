from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def parse_timestamp_from_dataset() -> F.Column:
    parsed_direct = F.to_timestamp(F.col("date"))
    parsed_without_timezone = F.to_timestamp(F.regexp_replace(F.col("date"), r"\+00:00$", ""))
    parsed_from_epoch_ms = F.to_timestamp(F.from_unixtime((F.col("time").cast("double") / F.lit(1000)).cast("long")))
    return F.coalesce(parsed_direct, parsed_without_timezone, parsed_from_epoch_ms)


def clean_silver_df(df: DataFrame) -> DataFrame:
    typed = (
        df.withColumn("time", F.col("time").cast("long"))
        .withColumn("place", F.trim(F.col("place")))
        .withColumn("status", F.lower(F.trim(F.col("status"))))
        .withColumn("tsunami", F.col("tsunami").cast("int"))
        .withColumn("significance", F.col("significance").cast("int"))
        .withColumn("data_type", F.lower(F.trim(F.col("data_type"))))
        .withColumn("magnitudo", F.col("magnitudo").cast("double"))
        .withColumn("state", F.coalesce(F.trim(F.col("state")), F.lit("unknown")))
        .withColumn("longitude", F.col("longitude").cast("double"))
        .withColumn("latitude", F.col("latitude").cast("double"))
        .withColumn("depth", F.col("depth").cast("double"))
        .withColumn("date", parse_timestamp_from_dataset())
        .withColumn("ingested_at", F.to_timestamp(F.col("ingested_at")))
    )

    enriched = (
        typed.withColumn("event_timestamp", F.col("date"))
        .withColumn("year", F.year(F.col("event_timestamp")))
        .withColumn("month", F.month(F.col("event_timestamp")))
        .withColumn("day", F.dayofmonth(F.col("event_timestamp")))
        .withColumn("hour", F.hour(F.col("event_timestamp")))
        .withColumn(
            "record_natural_key",
            F.sha2(
                F.concat_ws(
                    "||",
                    F.coalesce(F.col("time").cast("string"), F.lit("")),
                    F.coalesce(F.col("place"), F.lit("")),
                    F.coalesce(F.col("date").cast("string"), F.lit("")),
                ),
                256,
            ),
        )
        .withColumn("invalid_latitude", F.col("latitude").isNull() | ~F.col("latitude").between(-90.0, 90.0))
        .withColumn("invalid_longitude", F.col("longitude").isNull() | ~F.col("longitude").between(-180.0, 180.0))
        .withColumn("missing_magnitudo", F.col("magnitudo").isNull())
        .withColumn("missing_depth", F.col("depth").isNull())
        .withColumn("negative_depth", F.col("depth") < F.lit(0.0))
    )

    scored = (
        enriched.withColumn(
            "quality_score",
            F.greatest(
                F.lit(0),
                F.lit(100)
                - F.when(F.col("invalid_latitude"), F.lit(25)).otherwise(F.lit(0))
                - F.when(F.col("invalid_longitude"), F.lit(25)).otherwise(F.lit(0))
                - F.when(F.col("missing_magnitudo"), F.lit(30)).otherwise(F.lit(0))
                - F.when(F.col("missing_depth"), F.lit(15)).otherwise(F.lit(0))
                - F.when(F.col("negative_depth"), F.lit(5)).otherwise(F.lit(0)),
            ),
        )
        .withColumn(
            "is_valid_record",
            ~F.col("invalid_latitude")
            & ~F.col("invalid_longitude")
            & ~F.col("missing_magnitudo")
            & ~F.col("missing_depth"),
        )
    )
    return scored


def add_gold_features(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("abs_latitude", F.abs(F.col("latitude")))
        .withColumn("abs_longitude", F.abs(F.col("longitude")))
        .withColumn(
            "depth_bucket",
            F.when(F.col("depth") < 0, F.lit("negative"))
            .when(F.col("depth") < 70, F.lit("shallow"))
            .when(F.col("depth") < 300, F.lit("medium"))
            .otherwise(F.lit("deep")),
        )
        .withColumn(
            "magnitude_bucket",
            F.when(F.col("magnitudo") < 3, F.lit("minor"))
            .when(F.col("magnitudo") < 5, F.lit("light"))
            .when(F.col("magnitudo") < 6, F.lit("moderate"))
            .when(F.col("magnitudo") < 7, F.lit("strong"))
            .otherwise(F.lit("major")),
        )
        .withColumn("is_shallow", (F.col("depth") >= 0) & (F.col("depth") < 70))
        .withColumn("is_medium_depth", (F.col("depth") >= 70) & (F.col("depth") < 300))
        .withColumn("is_deep", F.col("depth") >= 300)
        .withColumn("is_high_significance", F.col("significance") >= 500)
        .withColumn("is_tsunami", F.col("tsunami") == 1)
    )
