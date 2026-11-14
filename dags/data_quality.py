import csv
import logging
import os
from collections import Counter
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

DATA_ROOT = Path(os.getenv("AIRFLOW_DATA_DIR", "/opt/airflow"))
INPUT_PATH = DATA_ROOT / "input_files" / "modified_data.csv"
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
MIN_ROW_COUNT = int(os.getenv("DATA_QUALITY_MIN_ROWS", "40000"))
MAX_ROW_COUNT = int(os.getenv("DATA_QUALITY_MAX_ROWS", "60000"))
MAX_NULL_RATE = float(os.getenv("DATA_QUALITY_MAX_NULL_RATE", "0.25"))
MIN_PRICE = Decimal(os.getenv("DATA_QUALITY_MIN_PRICE", "0"))
MAX_PRICE = Decimal(os.getenv("DATA_QUALITY_MAX_PRICE", "10000"))


def validate_data_quality(
    path: Path = INPUT_PATH,
    min_row_count: int = MIN_ROW_COUNT,
    max_row_count: int = MAX_ROW_COUNT,
    max_null_rate: float = MAX_NULL_RATE,
    min_price: Decimal = MIN_PRICE,
    max_price: Decimal = MAX_PRICE,
) -> dict[str, Any]:
    if not path.is_file():
        raise FileNotFoundError(f"Input data file does not exist: {path}")

    errors = []
    null_counts: Counter[str] = Counter()
    listing_ids: set[int] = set()
    row_count = 0
    min_seen_price: Decimal | None = None
    max_seen_price: Decimal | None = None

    with path.open(newline="", encoding="utf-8") as input_file:
        reader = csv.DictReader(input_file)
        columns = set(reader.fieldnames or [])
        missing_columns = sorted(REQUIRED_COLUMNS - columns)
        if missing_columns:
            raise ValueError(f"Input data is missing required columns: {missing_columns}")

        for row_number, row in enumerate(reader, start=2):
            row_count += 1
            for column in REQUIRED_COLUMNS:
                if not (row.get(column) or "").strip():
                    null_counts[column] += 1

            raw_id = (row.get("id") or "").strip()
            if raw_id:
                try:
                    listing_id = int(raw_id)
                except ValueError:
                    errors.append(f"Invalid listing id at CSV row {row_number}")
                else:
                    if listing_id in listing_ids:
                        errors.append(f"Duplicate listing id {listing_id} at CSV row {row_number}")
                    listing_ids.add(listing_id)

            raw_price = (row.get("price") or "").strip()
            if raw_price:
                try:
                    price = Decimal(raw_price)
                    if not price.is_finite():
                        raise InvalidOperation
                except (InvalidOperation, ValueError):
                    errors.append(f"Invalid price at CSV row {row_number}")
                else:
                    min_seen_price = price if min_seen_price is None else min(min_seen_price, price)
                    max_seen_price = price if max_seen_price is None else max(max_seen_price, price)
                    if price < min_price or price > max_price:
                        errors.append(
                            f"Price {price} outside allowed range {min_price}..{max_price} "
                            f"at CSV row {row_number}"
                        )

    if row_count < min_row_count or row_count > max_row_count:
        errors.append(
            f"Row count {row_count} outside allowed range {min_row_count}..{max_row_count}"
        )

    null_rates = {
        column: null_counts[column] / row_count if row_count else 0.0
        for column in sorted(REQUIRED_COLUMNS)
    }
    for column, null_count in null_counts.items():
        null_rate = null_rates[column]
        if column in {"id", "price"} and null_count:
            errors.append(f"Required field {column} contains {null_count} null values")
        elif null_rate > max_null_rate:
            errors.append(f"Null rate for {column} is {null_rate:.2%}, above {max_null_rate:.2%}")

    result = {
        "row_count": row_count,
        "null_rates": null_rates,
        "min_price": str(min_seen_price) if min_seen_price is not None else None,
        "max_price": str(max_seen_price) if max_seen_price is not None else None,
    }
    if errors:
        raise ValueError("Data quality checks failed: " + "; ".join(errors))

    logger.info(
        "Data quality checks passed: rows=%d, null_rates=%s, price_range=%s..%s",
        row_count,
        null_rates,
        result["min_price"],
        result["max_price"],
    )
    return result
