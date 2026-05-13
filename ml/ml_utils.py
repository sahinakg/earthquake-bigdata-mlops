from __future__ import annotations

import csv
import json
import platform
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import mlflow
from mlflow import MlflowClient
from pyspark.sql import DataFrame

from src.common.config import get_config
from src.utils.path_utils import ensure_dir


def setup_mlflow(experiment_name: str) -> None:
    config = get_config()
    mlflow.set_tracking_uri(config.mlflow_tracking_uri)
    mlflow.set_experiment(experiment_name)


def write_json_artifact(data: Dict[str, Any], artifact_file_name: str, artifact_path: Optional[str] = None) -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        file_path = Path(tmpdir) / artifact_file_name
        file_path.write_text(json.dumps(data, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        mlflow.log_artifact(str(file_path), artifact_path=artifact_path)


def write_csv_artifact(rows: Iterable[Dict[str, Any]], artifact_file_name: str, artifact_path: Optional[str] = None) -> None:
    rows = list(rows)
    with tempfile.TemporaryDirectory() as tmpdir:
        file_path = Path(tmpdir) / artifact_file_name
        fieldnames: List[str] = sorted({key for row in rows for key in row.keys()}) if rows else ["empty"]
        with file_path.open("w", encoding="utf-8", newline="") as file_obj:
            writer = csv.DictWriter(file_obj, fieldnames=fieldnames)
            writer.writeheader()
            for row in rows:
                writer.writerow(row)
        mlflow.log_artifact(str(file_path), artifact_path=artifact_path)


def log_environment_info() -> None:
    import mlflow as mlflow_pkg
    import pyspark

    info = {
        "run_timestamp_utc": datetime.utcnow().isoformat() + "Z",
        "python_version": platform.python_version(),
        "platform": platform.platform(),
        "mlflow_version": mlflow_pkg.__version__,
        "pyspark_version": pyspark.__version__,
    }
    write_json_artifact(info, "environment_info.json", artifact_path="metadata")


def log_feature_list(features: List[str]) -> None:
    mlflow.log_param("features", ",".join(features))
    write_json_artifact({"features": features}, "features.json", artifact_path="metadata")


def log_prediction_sample(predictions: DataFrame, columns: List[str], artifact_file_name: str) -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        file_path = Path(tmpdir) / artifact_file_name
        sample_pdf = predictions.select(*columns).limit(100).toPandas()
        sample_pdf.to_csv(file_path, index=False)
        mlflow.log_artifact(str(file_path), artifact_path="samples")


def get_best_run(experiment_name: str, order_by: List[str]) -> Optional[Any]:
    config = get_config()
    client = MlflowClient(tracking_uri=config.mlflow_tracking_uri)
    experiment = client.get_experiment_by_name(experiment_name)
    if experiment is None:
        return None
    runs = client.search_runs([experiment.experiment_id], order_by=order_by, max_results=1)
    return runs[0] if runs else None


def write_local_csv(rows: List[Dict[str, Any]], path: str) -> None:
    ensure_dir(str(Path(path).parent))
    fieldnames = sorted({key for row in rows for key in row.keys()}) if rows else ["empty"]
    with open(path, "w", encoding="utf-8", newline="") as file_obj:
        writer = csv.DictWriter(file_obj, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
