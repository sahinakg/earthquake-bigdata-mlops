from __future__ import annotations

import csv
import json
import logging
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict

from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.common.config import get_config
from src.common.logging_utils import configure_logging
from src.spark.spark_session import create_spark_session
from src.utils.path_utils import ensure_dir

LOGGER = logging.getLogger(__name__)


def delta_exists(spark, path: str) -> bool:
    try:
        return DeltaTable.isDeltaTable(spark, path)
    except Exception:
        return Path(path).exists() and any(Path(path).glob("_delta_log/*"))


def json_default(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return str(value)


def collect_distribution(df: DataFrame, column: str, limit: int = 50) -> list[dict[str, Any]]:
    if column not in df.columns:
        return []
    return [row.asDict(recursive=True) for row in df.groupBy(column).count().orderBy(F.desc("count")).limit(limit).collect()]


def generate_report(df: DataFrame) -> Dict[str, Any]:
    total_count = df.count()
    null_counts = {column: df.filter(F.col(column).isNull()).count() for column in df.columns}

    report: Dict[str, Any] = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "total_record_count": total_count,
        "null_counts": null_counts,
        "invalid_latitude_count": df.filter(F.col("latitude").isNull() | ~F.col("latitude").between(-90.0, 90.0)).count()
        if "latitude" in df.columns
        else None,
        "invalid_longitude_count": df.filter(F.col("longitude").isNull() | ~F.col("longitude").between(-180.0, 180.0)).count()
        if "longitude" in df.columns
        else None,
        "null_magnitudo_count": df.filter(F.col("magnitudo").isNull()).count() if "magnitudo" in df.columns else None,
        "negative_depth_count": df.filter(F.col("depth") < 0).count() if "depth" in df.columns else None,
        "tsunami_distribution": collect_distribution(df, "tsunami", limit=10),
        "status_distribution": collect_distribution(df, "status", limit=30),
        "data_type_distribution": collect_distribution(df, "data_type", limit=30),
        "yearly_record_count": [
            row.asDict(recursive=True)
            for row in df.groupBy("year").count().orderBy("year").collect()
        ]
        if "year" in df.columns
        else [],
        "top_magnitudo_records": [
            row.asDict(recursive=True)
            for row in df.select("event_id", "event_timestamp", "place", "state", "magnitudo", "depth")
            .orderBy(F.desc("magnitudo"))
            .limit(20)
            .collect()
        ]
        if "magnitudo" in df.columns
        else [],
        "top_states_by_event_count": [
            row.asDict(recursive=True)
            for row in df.groupBy("state").count().orderBy(F.desc("count")).limit(20).collect()
        ]
        if "state" in df.columns
        else [],
    }
    return report


def write_report(report: Dict[str, Any], reports_dir: str) -> None:
    ensure_dir(reports_dir)
    json_path = Path(reports_dir) / "data_quality_report.json"
    csv_path = Path(reports_dir) / "data_quality_report.csv"

    json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=json_default), encoding="utf-8")

    with csv_path.open("w", encoding="utf-8", newline="") as file_obj:
        writer = csv.writer(file_obj)
        writer.writerow(["metric", "value"])
        for key, value in report.items():
            if isinstance(value, (dict, list)):
                writer.writerow([key, json.dumps(value, ensure_ascii=False, default=json_default)])
            else:
                writer.writerow([key, value])

    LOGGER.info("Wrote data quality report JSON=%s CSV=%s", json_path, csv_path)


def main() -> None:
    configure_logging()
    config = get_config()
    spark = create_spark_session("earthquake_data_quality_report")

    if delta_exists(spark, config.silver_path):
        source_path = config.silver_path
    elif delta_exists(spark, config.bronze_path):
        source_path = config.bronze_path
    else:
        raise FileNotFoundError("No Bronze or Silver Delta table found. Run make bronze and make silver first.")

    LOGGER.info("Generating data quality report from %s", source_path)
    df = spark.read.format("delta").load(source_path)
    report = generate_report(df)
    print(json.dumps(report, ensure_ascii=False, indent=2, default=json_default))
    write_report(report, config.reports_dir)


if __name__ == "__main__":
    main()
