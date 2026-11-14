import csv
import json
import logging
import os
from pathlib import Path

logger = logging.getLogger(__name__)

DATA_ROOT = Path(os.getenv("AIRFLOW_DATA_DIR", "/opt/airflow"))
INPUT_PATH = DATA_ROOT / "input_files" / "modified_data_cleaned.csv"
OUTPUT_PATH = DATA_ROOT / "output_files" / "modified_data_filtered.csv"
METRICS_PATH = DATA_ROOT / "output_files" / "real_estate_metrics.json"


def filter_function() -> None:
    if not INPUT_PATH.is_file():
        raise FileNotFoundError(f"Cleaned data file does not exist: {INPUT_PATH}")

    with INPUT_PATH.open(newline="", encoding="utf-8") as input_file:
        reader = csv.DictReader(input_file)
        if not reader.fieldnames or not {"id", "price"}.issubset(reader.fieldnames):
            raise ValueError("Cleaned data is missing required columns: ['id', 'price']")
        rows = list(reader)

    for row_number, row in enumerate(rows, start=2):
        listing_id = (row.get("id") or "").strip()
        if not listing_id.isdigit():
            raise ValueError(f"Invalid listing id at CSV row {row_number}")
        try:
            float(row["price"])
        except (TypeError, ValueError) as exc:
            raise ValueError(f"Invalid price at CSV row {row_number}") from exc

    filtered_rows = [row for row in rows if row["id"].startswith("5")]
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    METRICS_PATH.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT_PATH.open("w", newline="", encoding="utf-8") as output_file:
        writer = csv.DictWriter(output_file, fieldnames=reader.fieldnames or [])
        writer.writeheader()
        writer.writerows(filtered_rows)

    metrics = {
        "total_listings": len(rows),
        "filtered_listings": len(filtered_rows),
        "average_filtered_price": round(
            sum(float(row["price"]) for row in filtered_rows) / len(filtered_rows), 2
        )
        if filtered_rows
        else 0,
    }
    METRICS_PATH.write_text(json.dumps(metrics, indent=2), encoding="utf-8")
    logger.info("Filtered %d of %d listings into %s", len(filtered_rows), len(rows), OUTPUT_PATH)
