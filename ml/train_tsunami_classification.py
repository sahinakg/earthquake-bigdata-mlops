from __future__ import annotations

import logging
from pathlib import Path
from typing import Dict, List, Tuple

import mlflow
import mlflow.spark
from delta.tables import DeltaTable
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.common.config import get_config
from src.common.logging_utils import configure_logging
from src.ml.ml_utils import (
    log_environment_info,
    log_feature_list,
    log_prediction_sample,
    setup_mlflow,
    write_csv_artifact,
    write_json_artifact,
)
from src.spark.spark_session import create_spark_session

LOGGER = logging.getLogger(__name__)
EXPERIMENT_NAME = "earthquake_tsunami_classification"

NUMERIC_FEATURES: List[str] = [
    "latitude",
    "longitude",
    "depth",
    "magnitudo",
    "significance",
    "month",
    "hour",
]
ASSEMBLER_FEATURES: List[str] = NUMERIC_FEATURES + ["state_index_model"]


def delta_exists(spark, path: str) -> bool:
    try:
        return DeltaTable.isDeltaTable(spark, path)
    except Exception:
        return Path(path).exists() and any(Path(path).glob("_delta_log/*"))


def load_training_data() -> DataFrame:
    config = get_config()
    spark = create_spark_session("earthquake_train_tsunami_classification")
    if not delta_exists(spark, config.gold_tsunami_risk_path):
        raise FileNotFoundError(f"Gold tsunami risk table not found: {config.gold_tsunami_risk_path}. Run make gold first.")

    required_columns = ["event_id", "state", "tsunami"] + NUMERIC_FEATURES
    df = spark.read.format("delta").load(config.gold_tsunami_risk_path).select(*required_columns)
    for column in NUMERIC_FEATURES:
        df = df.withColumn(column, F.col(column).cast("double"))
    df = df.withColumn("label", F.col("tsunami").cast("double"))
    df = df.withColumn("state", F.coalesce(F.col("state"), F.lit("unknown")))
    return df.dropna(subset=["label"] + NUMERIC_FEATURES + ["state"]).filter(F.col("label").isin(0.0, 1.0))


def add_class_weights(train_df: DataFrame) -> tuple[DataFrame, Dict[str, float]]:
    counts = {float(row["label"]): int(row["count"]) for row in train_df.groupBy("label").count().collect()}
    if len(counts) < 2:
        raise RuntimeError(f"Classification training needs both tsunami classes. Found distribution={counts}")
    total = sum(counts.values())
    number_of_classes = len(counts)
    weights = {label: total / (number_of_classes * count) for label, count in counts.items()}
    weighted = train_df.withColumn(
        "class_weight",
        F.when(F.col("label") == 1.0, F.lit(float(weights.get(1.0, 1.0))))
        .when(F.col("label") == 0.0, F.lit(float(weights.get(0.0, 1.0))))
        .otherwise(F.lit(1.0)),
    )
    return weighted, {str(key): float(value) for key, value in weights.items()}


def build_pipeline(model_name: str):
    indexer = StringIndexer(inputCol="state", outputCol="state_index_model", handleInvalid="keep")
    assembler = VectorAssembler(inputCols=ASSEMBLER_FEATURES, outputCol="features", handleInvalid="skip")

    if model_name == "logistic_regression_baseline":
        model = LogisticRegression(
            labelCol="label",
            featuresCol="features",
            weightCol="class_weight",
            maxIter=60,
            regParam=0.05,
            elasticNetParam=0.0,
        )
    elif model_name == "random_forest_classifier":
        model = RandomForestClassifier(
            labelCol="label",
            featuresCol="features",
            weightCol="class_weight",
            numTrees=100,
            maxDepth=8,
            maxBins=128,
            minInstancesPerNode=2,
            subsamplingRate=0.8,
            seed=get_config().random_seed,
        )
    else:
        raise ValueError(f"Unknown model_name={model_name}")
    return Pipeline(stages=[indexer, assembler, model])


def evaluate(predictions: DataFrame) -> Dict[str, float]:
    metrics: Dict[str, float] = {}
    multiclass_metrics = {
        "accuracy": "accuracy",
        "precision": "weightedPrecision",
        "recall": "weightedRecall",
        "f1": "f1",
    }
    for output_name, spark_metric in multiclass_metrics.items():
        evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName=spark_metric)
        metrics[output_name] = float(evaluator.evaluate(predictions))

    try:
        auc_evaluator = BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="rawPrediction", metricName="areaUnderROC")
        metrics["roc_auc"] = float(auc_evaluator.evaluate(predictions))
    except Exception as exc:
        LOGGER.warning("ROC-AUC could not be computed: %s", exc)
        metrics["roc_auc"] = float("nan")
    return metrics


def model_params(model_name: str) -> Dict[str, object]:
    if model_name == "logistic_regression_baseline":
        return {"maxIter": 60, "regParam": 0.05, "elasticNetParam": 0.0}
    if model_name == "random_forest_classifier":
        return {"numTrees": 100, "maxDepth": 8, "minInstancesPerNode": 2, "subsamplingRate": 0.8}
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


def log_confusion_matrix(predictions: DataFrame) -> None:
    rows = [
        {"label": float(row["label"]), "prediction": float(row["prediction"]), "count": int(row["count"])}
        for row in predictions.groupBy("label", "prediction").count().orderBy("label", "prediction").collect()
    ]
    write_csv_artifact(rows, "confusion_matrix.csv", "model_diagnostics")


def train_and_log(
    model_name: str,
    train_df: DataFrame,
    test_df: DataFrame,
    data_count: int,
    class_distribution: Dict[str, int],
    class_weights: Dict[str, float],
) -> Tuple[str, Dict[str, float]]:
    pipeline = build_pipeline(model_name)
    pipeline_model = pipeline.fit(train_df)
    predictions = pipeline_model.transform(test_df).cache()
    metrics = evaluate(predictions)

    with mlflow.start_run(run_name=model_name) as run:
        mlflow.set_tag("model_family", "classification")
        mlflow.set_tag("target", "tsunami")
        mlflow.log_param("model_name", model_name)
        mlflow.log_param("train_test_split", get_config().train_test_split)
        mlflow.log_param("data_row_count", data_count)
        mlflow.log_param("train_row_count", train_df.count())
        mlflow.log_param("test_row_count", test_df.count())
        for key, value in model_params(model_name).items():
            mlflow.log_param(key, value)
        log_feature_list(ASSEMBLER_FEATURES)
        log_environment_info()
        write_json_artifact({"class_distribution": class_distribution, "class_weights": class_weights}, "class_imbalance.json", "metadata")

        for metric_name, metric_value in metrics.items():
            if metric_value == metric_value:  # skip NaN
                mlflow.log_metric(metric_name, metric_value)

        write_csv_artifact(feature_rows_from_model(pipeline_model, model_name), "feature_importance_or_coefficients.csv", "model_diagnostics")
        log_confusion_matrix(predictions)
        log_prediction_sample(
            predictions,
            columns=["event_id", "state", "label", "prediction", "latitude", "longitude", "depth", "magnitudo", "significance"],
            artifact_file_name="classification_prediction_sample.csv",
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
        raise RuntimeError(f"Not enough rows for classification training. Found rows={data_count}")

    class_distribution = {str(row["label"]): int(row["count"]) for row in data.groupBy("label").count().collect()}
    LOGGER.info("Tsunami class distribution: %s", class_distribution)

    train_df, test_df = data.randomSplit([config.train_test_split, 1.0 - config.train_test_split], seed=config.random_seed)
    train_df = train_df.cache()
    test_df = test_df.cache()
    weighted_train_df, class_weights = add_class_weights(train_df)
    weighted_train_df = weighted_train_df.cache()

    results = []
    for model_name in ["logistic_regression_baseline", "random_forest_classifier"]:
        run_id, metrics = train_and_log(
            model_name=model_name,
            train_df=weighted_train_df,
            test_df=test_df,
            data_count=data_count,
            class_distribution=class_distribution,
            class_weights=class_weights,
        )
        results.append({"model_name": model_name, "run_id": run_id, **metrics})

    best = sorted(results, key=lambda item: item.get("f1", 0.0), reverse=True)[0]
    LOGGER.info("Best classification model by F1: %s", best)
    print(f"Best classification model by F1: {best}")

    data.unpersist()
    train_df.unpersist()
    test_df.unpersist()
    weighted_train_df.unpersist()


if __name__ == "__main__":
    main()
