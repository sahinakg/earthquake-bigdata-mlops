from __future__ import annotations

import os
from typing import List, Optional

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from src.common.config import get_config


def get_spark_packages() -> List[str]:
    spark_version = os.getenv("SPARK_VERSION", "3.5.1")
    scala_binary = os.getenv("SCALA_BINARY_VERSION", "2.12")
    delta_version = os.getenv("DELTA_SPARK_VERSION", "3.2.0")
    return [
        f"io.delta:delta-spark_{scala_binary}:{delta_version}",
        f"org.apache.spark:spark-sql-kafka-0-10_{scala_binary}:{spark_version}",
    ]


def create_spark_session(app_name: str, master: Optional[str] = None) -> SparkSession:
    config = get_config()
    builder = SparkSession.builder.appName(app_name)
    if master:
        builder = builder.master(master)

    builder = (
        builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        .config("spark.sql.shuffle.partitions", str(config.spark_sql_shuffle_partitions))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.ui.showConsoleProgress", "true")
    )

    extra_packages = [pkg for pkg in get_spark_packages() if not pkg.startswith("io.delta")]
    spark = configure_spark_with_delta_pip(builder, extra_packages=extra_packages).getOrCreate()
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))
    return spark
