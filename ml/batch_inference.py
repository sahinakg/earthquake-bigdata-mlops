from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

import mlflow
import mlflow.spark
from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

from src.common.config import get_config
from src.common.logging_utils import configure_logging
from src.ml.ml_utils import get_best_run
from src.spark.spark_session import create_spark_session

LOGGER = logging.getLogger(__name__)
REGRESSION_EXPERIMENT = "earthquake_magnitude_regression"
CLASSIFICATION_EXPERIMENT = "earthquake_tsunami_classification"


def delta_exists(spark, path: str) -> bool:
    try:
        return DeltaTable.isDeltaTable(spark, path)
    except Exception:
        return Path(path).exists() and any(Path(path).glob("_delta_log/*"))


def load_model_from_best_run(experiment_name: str, order_by: list[str]):
    run = get_best_run(experiment_name, order_by=order_by)
    if run is None:
        LOGGER.warning("No MLflow run found for experiment=%s", experiment_name)
        return None, None
    model_uri = f"runs:/{run.info.run_id}/model"
    LOGGER.info("Loading model_uri=%s", model_uri)
    return mlflow.spark.load_model(model_uri), run.info.run_id


def write_predictions(df: DataFrame, path: str) -> None:
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path)
    LOGGER.info("Wrote predictions path=%s rows=%s", path, df.count())


def probability_one(value) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value[1])
    except Exception:
        return None


def run_regression_inference(features_df: DataFrame, output_path: str) -> None:
    model, run_id = load_model_from_best_run(REGRESSION_EXPERIMENT, order_by=["metrics.rmse ASC"])
    if model is None or run_id is None:
        return

    predicted_at = datetime.utcnow().isoformat() + "Z"
    predictions = model.transform(features_df).select(
        "event_id",
        F.col("prediction").cast("double").alias("prediction"),
        F.lit("magnitude_regression").alias("model_name"),
        F.lit(run_id).alias("run_id"),
        F.lit(run_id).alias("model_version"),
        F.lit(predicted_at).alias("predicted_at"),
    )
    write_predictions(predictions, output_path)


def run_classification_inference(features_df: DataFrame, output_path: str) -> None:
    model, run_id = load_model_from_best_run(CLASSIFICATION_EXPERIMENT, order_by=["metrics.f1 DESC"])
    if model is None or run_id is None:
        return

    probability_udf = F.udf(probability_one, DoubleType())
    predicted_at = datetime.utcnow().isoformat() + "Z"
    transformed = model.transform(features_df)
    if "probability" in transformed.columns:
        transformed = transformed.withColumn("probability_tsunami", probability_udf(F.col("probability")))
    else:
        transformed = transformed.withColumn("probability_tsunami", F.lit(None).cast("double"))

    predictions = transformed.select(
        "event_id",
        F.col("prediction").cast("double").alias("prediction"),
        F.col("probability_tsunami"),
        F.lit("tsunami_classification").alias("model_name"),
        F.lit(run_id).alias("run_id"),
        F.lit(run_id).alias("model_version"),
        F.lit(predicted_at).alias("predicted_at"),
    )
    write_predictions(predictions, output_path)


def main() -> None:
    configure_logging()
    config = get_config()
    mlflow.set_tracking_uri(config.mlflow_tracking_uri)
    spark = create_spark_session("earthquake_batch_inference")

    if not delta_exists(spark, config.gold_features_path):
        raise FileNotFoundError(f"Gold feature table not found: {config.gold_features_path}. Run make gold first.")

    features_df = spark.read.format("delta").load(config.gold_features_path).cache()
    if features_df.rdd.isEmpty():
        raise RuntimeError("Gold feature table is empty; cannot run inference.")

    run_regression_inference(features_df, config.magnitude_predictions_path)
    run_classification_inference(features_df, config.tsunami_predictions_path)
    features_df.unpersist()


if __name__ == "__main__":
    main()
