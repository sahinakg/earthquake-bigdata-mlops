from __future__ import annotations

import logging
from pathlib import Path
from typing import Dict, List, Tuple

import mlflow
import mlflow.spark
from delta.tables import DeltaTable
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.regression import DecisionTreeRegressor, GBTRegressor, GeneralizedLinearRegression, LinearRegression, RandomForestRegressor
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.common.config import get_config
from src.common.logging_utils import configure_logging
from src.ml.ml_utils import log_environment_info, log_feature_list, log_prediction_sample, setup_mlflow, write_csv_artifact
from src.spark.spark_session import create_spark_session

LOGGER = logging.getLogger(__name__)
EXPERIMENT_NAME = "earthquake_magnitude_regression"

NUMERIC_FEATURES: List[str] = [
    "latitude",
    "longitude",
    "depth",
    "significance",
    "month",
    "hour",
    "tsunami",
]
ASSEMBLER_FEATURES: List[str] = NUMERIC_FEATURES + ["state_index_model"]


def delta_exists(spark, path: str) -> bool:
    try:
        return DeltaTable.isDeltaTable(spark, path)
    except Exception:
        return Path(path).exists() and any(Path(path).glob("_delta_log/*"))


def load_training_data() -> DataFrame:
    config = get_config()
    spark = create_spark_session("earthquake_train_magnitude_regression")
    if not delta_exists(spark, config.gold_features_path):
        raise FileNotFoundError(f"Gold feature table not found: {config.gold_features_path}. Run make gold first.")

    required_columns = ["event_id", "state", "magnitudo"] + NUMERIC_FEATURES
    df = spark.read.format("delta").load(config.gold_features_path).select(*required_columns)
    for column in NUMERIC_FEATURES + ["magnitudo"]:
        df = df.withColumn(column, F.col(column).cast("double"))
    df = df.withColumn("state", F.coalesce(F.col("state"), F.lit("unknown")))
    return df.dropna(subset=["magnitudo"] + NUMERIC_FEATURES + ["state"])


def build_pipeline(model_name: str):
    indexer = StringIndexer(inputCol="state", outputCol="state_index_model", handleInvalid="keep")
    assembler = VectorAssembler(inputCols=ASSEMBLER_FEATURES, outputCol="features", handleInvalid="skip")

    if model_name == "linear_regression_baseline":
        model = LinearRegression(labelCol="magnitudo", featuresCol="features", maxIter=50, regParam=0.05, elasticNetParam=0.0)
    elif model_name == "random_forest_regressor":
        model = RandomForestRegressor(
            labelCol="magnitudo",
            featuresCol="features",
            numTrees=80,
            maxDepth=8,
            maxBins=128,
            minInstancesPerNode=2,
            subsamplingRate=0.8,
            seed=get_config().random_seed,
        )
    elif model_name == "decision_tree_regressor":
        model = DecisionTreeRegressor(
            labelCol="magnitudo",
            featuresCol="features",
            maxDepth=8,
            maxBins=128,
            seed=get_config().random_seed,
        )
    elif model_name == "gbt_regressor":
        model = GBTRegressor(
            labelCol="magnitudo",
            featuresCol="features",
            maxIter=50,
            maxDepth=6,
            maxBins=128,
            seed=get_config().random_seed,
        )
    elif model_name == "generalized_linear_regression":
        model = GeneralizedLinearRegression(
            labelCol="magnitudo",
            featuresCol="features",
            family="gaussian",
            link="identity",
            maxIter=50,
            regParam=0.05,
        )
    else:
        raise ValueError(f"Unknown model_name={model_name}")
    return Pipeline(stages=[indexer, assembler, model])


def evaluate(predictions: DataFrame) -> Dict[str, float]:
    metrics: Dict[str, float] = {}
    for metric_name in ["rmse", "mae", "r2"]:
        evaluator = RegressionEvaluator(labelCol="magnitudo", predictionCol="prediction", metricName=metric_name)
        metrics[metric_name] = float(evaluator.evaluate(predictions))
    return metrics


def model_params(model_name: str) -> Dict[str, object]:
    if model_name == "linear_regression_baseline":
        return {"maxIter": 50, "regParam": 0.05, "elasticNetParam": 0.0}
    if model_name == "random_forest_regressor":
        return {"numTrees": 80, "maxDepth": 8, "minInstancesPerNode": 2, "subsamplingRate": 0.8}
    if model_name == "decision_tree_regressor":
        return {"maxDepth": 8, "maxBins": 128}
    if model_name == "gbt_regressor":
        return {"maxIter": 50, "maxDepth": 6, "maxBins": 128}
    if model_name == "generalized_linear_regression":
        return {"family": "gaussian", "link": "identity", "maxIter": 50, "regParam": 0.05}
    return {}


def feature_rows_from_model(pipeline_model, model_name: str) -> List[Dict[str, object]]:
    final_model = pipeline_model.stages[-1]
    rows: List[Dict[str, object]] = []
    if hasattr(final_model, "featureImportances"):
        importances = final_model.featureImportances.toArray().tolist()
        rows = [
            {"feature": feature, "importance": float(importance)}
            for feature, importance in zip(ASSEMBLER_FEATURES, importances)
        ]
    elif hasattr(final_model, "coefficients"):
        coefficients = final_model.coefficients.toArray().tolist()
        rows = [
            {"feature": feature, "coefficient": float(coefficient)}
            for feature, coefficient in zip(ASSEMBLER_FEATURES, coefficients)
        ]
        rows.append({"feature": "intercept", "coefficient": float(final_model.intercept)})
    else:
        rows = [{"feature": "not_available", "model": model_name}]
    return rows


def train_and_log(model_name: str, train_df: DataFrame, test_df: DataFrame, data_count: int) -> Tuple[str, Dict[str, float]]:
    pipeline = build_pipeline(model_name)
    pipeline_model = pipeline.fit(train_df)
    predictions = pipeline_model.transform(test_df).cache()
    metrics = evaluate(predictions)

    with mlflow.start_run(run_name=model_name) as run:
        mlflow.set_tag("model_family", "regression")
        mlflow.set_tag("target", "magnitudo")
        mlflow.log_param("model_name", model_name)
        mlflow.log_param("train_test_split", get_config().train_test_split)
        mlflow.log_param("data_row_count", data_count)
        mlflow.log_param("train_row_count", train_df.count())
        mlflow.log_param("test_row_count", test_df.count())
        for key, value in model_params(model_name).items():
            mlflow.log_param(key, value)
        log_feature_list(ASSEMBLER_FEATURES)
        log_environment_info()
        for metric_name, metric_value in metrics.items():
            mlflow.log_metric(metric_name, metric_value)

        write_csv_artifact(feature_rows_from_model(pipeline_model, model_name), "feature_importance_or_coefficients.csv", "model_diagnostics")
        log_prediction_sample(
            predictions,
            columns=["event_id", "state", "magnitudo", "prediction", "latitude", "longitude", "depth", "significance"],
            artifact_file_name="regression_prediction_sample.csv",
        )
        mlflow.spark.log_model(pipeline_model, artifact_path="model")
        run_id = run.info.run_id

    LOGGER.info("Trained model=%s run_id=%s metrics=%s", model_name, run_id, metrics)
    predictions.unpersist()
    return run_id, metrics


def main() -> None:
    configure_logging()
    setup_mlflow(EXPERIMENT_NAME)
    config = get_config()

    data = load_training_data().cache()
    data_count = data.count()
    if data_count < 100:
        raise RuntimeError(f"Not enough rows for regression training. Found rows={data_count}")

    train_df, test_df = data.randomSplit([config.train_test_split, 1.0 - config.train_test_split], seed=config.random_seed)
    train_df = train_df.cache()
    test_df = test_df.cache()

    results = []
    for model_name in ["linear_regression_baseline", "decision_tree_regressor", "random_forest_regressor", "gbt_regressor", "generalized_linear_regression"]:
        run_id, metrics = train_and_log(model_name, train_df, test_df, data_count)
        results.append({"model_name": model_name, "run_id": run_id, **metrics})

    best = sorted(results, key=lambda item: item["rmse"])[0]
    LOGGER.info("Best regression model by RMSE: %s", best)
    print(f"Best regression model by RMSE: {best}")

    data.unpersist()
    train_df.unpersist()
    test_df.unpersist()


if __name__ == "__main__":
    main()
