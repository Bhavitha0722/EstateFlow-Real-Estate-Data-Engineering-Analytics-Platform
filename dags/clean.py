import csv
import logging
import os
from datetime import date
from pathlib import Path

logger = logging.getLogger(__name__)

DATA_ROOT = Path(os.getenv("AIRFLOW_DATA_DIR", "/opt/airflow"))
INPUT_PATH = DATA_ROOT / "input_files" / "modified_data.csv"
CLEANED_PATH = DATA_ROOT / "input_files" / "modified_data_cleaned.csv"
REQUIRED_COLUMNS = {
    "id",
    "name",
    "host_id",
    "host_name",
    "neighbourhood_group",
    "neighbourhood",
    "latitude",
    "longitude",
    "room_type",
    "price",
    "minimum_nights",
    "number_of_reviews",
    "reviews_per_month",
    "calculated_host_listings_count",
    "availability_365",
}


def validate_input_file(path: Path = INPUT_PATH) -> None:
    if not path.is_file():
        raise FileNotFoundError(f"Input data file does not exist: {path}")

    with path.open(newline="", encoding="utf-8") as input_file:
        reader = csv.DictReader(input_file)
        missing = sorted(REQUIRED_COLUMNS - set(reader.fieldnames or []))
        if missing:
            raise ValueError(f"Input data is missing required columns: {missing}")
        if next(reader, None) is None:
            raise ValueError(f"Input data contains no listings: {path}")

    logger.info("Validated input CSV %s", path)


def pre_process(process_date: str | None = None) -> None:
    validate_input_file(INPUT_PATH)

    with INPUT_PATH.open(newline="", encoding="utf-8") as input_file:
        reader = csv.DictReader(input_file)
        rows = []
        for row in reader:
            row = {key: value.strip() if value is not None else "" for key, value in row.items()}
            row["process_date"] = process_date or date.today().isoformat()
            rows.append(row)

    CLEANED_PATH.parent.mkdir(parents=True, exist_ok=True)
    with CLEANED_PATH.open("w", newline="", encoding="utf-8") as output_file:
        writer = csv.DictWriter(output_file, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)

    logger.info("Cleaned %d listings into %s", len(rows), CLEANED_PATH)
