import csv
import logging
import os
from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

DATA_ROOT = Path(os.getenv("AIRFLOW_DATA_DIR", "/opt/airflow"))
CURATED_PATH = DATA_ROOT / "output_files" / "modified_data_filtered.csv"
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
    "process_date",
}


@dataclass(frozen=True)
class Listing:
    listing_id: int
    name: str
    host_id: int
    host_name: str
    neighbourhood_group: str
    neighbourhood: str
    latitude: Decimal
    longitude: Decimal
    room_type: str
    price: Decimal
    minimum_nights: int
    number_of_reviews: int
    reviews_per_month: Decimal | None
    calculated_host_listings_count: int
    availability_365: int
    process_date: date

    def as_tuple(self) -> tuple[Any, ...]:
        return (
            self.listing_id,
            self.name,
            self.host_id,
            self.host_name,
            self.neighbourhood_group,
            self.neighbourhood,
            self.latitude,
            self.longitude,
            self.room_type,
            self.price,
            self.minimum_nights,
            self.number_of_reviews,
            self.reviews_per_month,
            self.calculated_host_listings_count,
            self.availability_365,
            self.process_date,
        )


def _required_int(row: dict[str, str], column: str, row_number: int) -> int:
    try:
        return int(row[column])
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError(f"Invalid {column!r} at CSV row {row_number}") from exc


def _required_decimal(row: dict[str, str], column: str, row_number: int) -> Decimal:
    try:
        return Decimal(row[column])
    except (KeyError, InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(f"Invalid {column!r} at CSV row {row_number}") from exc


def _optional_decimal(row: dict[str, str], column: str, row_number: int) -> Decimal | None:
    value = row.get(column, "").strip()
    return _required_decimal(row, column, row_number) if value else None


def read_curated_listings(path: Path = CURATED_PATH) -> list[Listing]:
    if not path.is_file():
        raise FileNotFoundError(f"Curated data file does not exist: {path}")

    with path.open(newline="", encoding="utf-8") as input_file:
        reader = csv.DictReader(input_file)
        columns = set(reader.fieldnames or [])
        missing = sorted(REQUIRED_COLUMNS - columns)
        if missing:
            raise ValueError(f"Curated data is missing required columns: {missing}")

        listings = []
        seen_ids = set()
        for row_number, row in enumerate(reader, start=2):
            listing_id = _required_int(row, "id", row_number)
            if listing_id in seen_ids:
                raise ValueError(f"Duplicate listing id {listing_id} at CSV row {row_number}")
            seen_ids.add(listing_id)
            price = _required_decimal(row, "price", row_number)
            if price < 0:
                raise ValueError(f"Price cannot be negative at CSV row {row_number}")

            try:
                processed_on = date.fromisoformat(row["process_date"])
            except (KeyError, TypeError, ValueError) as exc:
                raise ValueError(f"Invalid process_date at CSV row {row_number}") from exc

            listings.append(
                Listing(
                    listing_id=listing_id,
                    name=row["name"],
                    host_id=_required_int(row, "host_id", row_number),
                    host_name=row["host_name"],
                    neighbourhood_group=row["neighbourhood_group"],
                    neighbourhood=row["neighbourhood"],
                    latitude=_required_decimal(row, "latitude", row_number),
                    longitude=_required_decimal(row, "longitude", row_number),
                    room_type=row["room_type"],
                    price=price,
                    minimum_nights=_required_int(row, "minimum_nights", row_number),
                    number_of_reviews=_required_int(row, "number_of_reviews", row_number),
                    reviews_per_month=_optional_decimal(row, "reviews_per_month", row_number),
                    calculated_host_listings_count=_required_int(
                        row, "calculated_host_listings_count", row_number
                    ),
                    availability_365=_required_int(row, "availability_365", row_number),
                    process_date=processed_on,
                )
            )

    if not listings:
        raise ValueError(f"Curated data file contains no listings: {path}")
    return listings


CREATE_TABLES_SQL = """
CREATE TABLE IF NOT EXISTS real_estate_listings (
    listing_id BIGINT PRIMARY KEY,
    name TEXT NOT NULL,
    host_id BIGINT NOT NULL,
    host_name TEXT NOT NULL,
    neighbourhood_group TEXT NOT NULL,
    neighbourhood TEXT NOT NULL,
    latitude NUMERIC(9, 6) NOT NULL,
    longitude NUMERIC(9, 6) NOT NULL,
    room_type TEXT NOT NULL,
    price NUMERIC(12, 2) NOT NULL CHECK (price >= 0),
    minimum_nights INTEGER NOT NULL CHECK (minimum_nights >= 0),
    number_of_reviews INTEGER NOT NULL CHECK (number_of_reviews >= 0),
    reviews_per_month NUMERIC(12, 2) CHECK (reviews_per_month >= 0),
    calculated_host_listings_count INTEGER NOT NULL CHECK (calculated_host_listings_count >= 0),
    availability_365 INTEGER NOT NULL CHECK (availability_365 BETWEEN 0 AND 365),
    process_date DATE NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS real_estate_neighbourhood_metrics (
    neighbourhood_group TEXT NOT NULL,
    neighbourhood TEXT NOT NULL,
    listing_count BIGINT NOT NULL,
    average_price NUMERIC(12, 2) NOT NULL,
    median_price NUMERIC(12, 2) NOT NULL,
    average_availability NUMERIC(12, 2) NOT NULL,
    refreshed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (neighbourhood_group, neighbourhood)
);

CREATE TABLE IF NOT EXISTS real_estate_room_type_metrics (
    room_type TEXT PRIMARY KEY,
    listing_count BIGINT NOT NULL,
    average_price NUMERIC(12, 2) NOT NULL,
    average_reviews NUMERIC(12, 2) NOT NULL,
    average_availability NUMERIC(12, 2) NOT NULL,
    refreshed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
"""


LISTING_COLUMNS = """(
    listing_id, name, host_id, host_name, neighbourhood_group, neighbourhood,
    latitude, longitude, room_type, price, minimum_nights, number_of_reviews,
    reviews_per_month, calculated_host_listings_count, availability_365, process_date
)"""


def load_curated_data(postgres_conn_id: str = "analytics_db") -> int:
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    from psycopg2.extras import execute_values

    listings = read_curated_listings(CURATED_PATH)
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    connection = hook.get_conn()
    try:
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(CREATE_TABLES_SQL)
                cursor.execute(
                    "ALTER TABLE real_estate_listings ALTER COLUMN reviews_per_month DROP NOT NULL"
                )
                cursor.execute(
                    "CREATE TEMP TABLE listing_stage "
                    "(LIKE real_estate_listings INCLUDING DEFAULTS) ON COMMIT DROP"
                )
                execute_values(
                    cursor,
                    f"INSERT INTO listing_stage {LISTING_COLUMNS} VALUES %s",
                    [listing.as_tuple() for listing in listings],
                    page_size=1000,
                )
                cursor.execute(
                    f"""
                    INSERT INTO real_estate_listings {LISTING_COLUMNS}
                    SELECT {LISTING_COLUMNS[1:-1]} FROM listing_stage
                    ON CONFLICT (listing_id) DO UPDATE SET
                        name = EXCLUDED.name,
                        host_id = EXCLUDED.host_id,
                        host_name = EXCLUDED.host_name,
                        neighbourhood_group = EXCLUDED.neighbourhood_group,
                        neighbourhood = EXCLUDED.neighbourhood,
                        latitude = EXCLUDED.latitude,
                        longitude = EXCLUDED.longitude,
                        room_type = EXCLUDED.room_type,
                        price = EXCLUDED.price,
                        minimum_nights = EXCLUDED.minimum_nights,
                        number_of_reviews = EXCLUDED.number_of_reviews,
                        reviews_per_month = EXCLUDED.reviews_per_month,
                        calculated_host_listings_count = EXCLUDED.calculated_host_listings_count,
                        availability_365 = EXCLUDED.availability_365,
                        process_date = EXCLUDED.process_date,
                        loaded_at = CURRENT_TIMESTAMP
                    """
                )
                cursor.execute(
                    """
                    DELETE FROM real_estate_listings
                    WHERE listing_id NOT IN (SELECT listing_id FROM listing_stage)
                    """
                )
                cursor.execute("DELETE FROM real_estate_neighbourhood_metrics")
                cursor.execute(
                    """
                    INSERT INTO real_estate_neighbourhood_metrics
                    (neighbourhood_group, neighbourhood, listing_count, average_price,
                     median_price, average_availability)
                    SELECT neighbourhood_group, neighbourhood, COUNT(*),
                           ROUND(AVG(price), 2), ROUND(PERCENTILE_CONT(0.5)
                           WITHIN GROUP (ORDER BY price)::numeric, 2),
                           ROUND(AVG(availability_365), 2)
                    FROM real_estate_listings
                    GROUP BY neighbourhood_group, neighbourhood
                    """
                )
                cursor.execute("DELETE FROM real_estate_room_type_metrics")
                cursor.execute(
                    """
                    INSERT INTO real_estate_room_type_metrics
                    (room_type, listing_count, average_price, average_reviews, average_availability)
                    SELECT room_type, COUNT(*), ROUND(AVG(price), 2),
                           ROUND(AVG(number_of_reviews), 2), ROUND(AVG(availability_365), 2)
                    FROM real_estate_listings
                    GROUP BY room_type
                    """
                )
        logger.info("Loaded %d curated listings and refreshed analytics tables", len(listings))
        return len(listings)
    except Exception:
        connection.rollback()
        logger.exception("Failed to load curated real-estate data")
        raise
    finally:
        connection.close()
