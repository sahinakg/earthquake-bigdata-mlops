from __future__ import annotations

import logging
from typing import Any, Dict, List

from mlflow import MlflowClient

from src.common.config import get_config
from src.common.logging_utils import configure_logging
from src.ml.ml_utils import write_local_csv

LOGGER = logging.getLogger(__name__)
EXPERIMENTS = [
    "earthquake_magnitude_regression",
    "earthquake_tsunami_classification",
]


def flatten_run(experiment_name: str, run) -> Dict[str, Any]:
    row: Dict[str, Any] = {
        "experiment_name": experiment_name,
        "run_id": run.info.run_id,
        "status": run.info.status,
        "start_time": run.info.start_time,
        "end_time": run.info.end_time,
        "artifact_uri": run.info.artifact_uri,
    }
    for key, value in run.data.params.items():
        row[f"param_{key}"] = value
    for key, value in run.data.metrics.items():
        row[f"metric_{key}"] = value
    for key, value in run.data.tags.items():
        if key.startswith("mlflow."):
            continue
        row[f"tag_{key}"] = value
    return row


def main() -> None:
    configure_logging()
    config = get_config()
    client = MlflowClient(tracking_uri=config.mlflow_tracking_uri)
    rows: List[Dict[str, Any]] = []

    for experiment_name in EXPERIMENTS:
        experiment = client.get_experiment_by_name(experiment_name)
        if experiment is None:
            LOGGER.warning("Experiment not found: %s", experiment_name)
            continue
        runs = client.search_runs([experiment.experiment_id], max_results=100, order_by=["attributes.start_time DESC"])
        for run in runs:
            rows.append(flatten_run(experiment_name, run))

    output_path = f"{config.reports_dir}/mlflow_model_summary.csv"
    write_local_csv(rows, output_path)
    LOGGER.info("Wrote MLflow summary: %s rows=%s", output_path, len(rows))
    print(f"MLflow model summary written to {output_path}. rows={len(rows)}")


if __name__ == "__main__":
    main()
