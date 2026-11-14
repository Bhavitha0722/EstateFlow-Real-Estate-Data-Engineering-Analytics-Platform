import csv
import os
import uuid
from decimal import Decimal
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import analytics_db
import pytest

pytestmark = pytest.mark.skipif(
    os.getenv("RUN_POSTGRES_INTEGRATION_TESTS") != "1"
    or not os.getenv("TEST_ANALYTICS_DATABASE_DSN"),
    reason="requires the opt-in disposable PostgreSQL integration database",
)


@pytest.fixture
def isolated_schema(monkeypatch):
    psycopg2 = pytest.importorskip("psycopg2")
    from psycopg2 import sql

    dsn = os.environ["TEST_ANALYTICS_DATABASE_DSN"]
    schema = f"analytics_test_{uuid.uuid4().hex}"
    admin_connection = psycopg2.connect(dsn)
    admin_connection.autocommit = True
    with admin_connection.cursor() as cursor:
        cursor.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))

    parsed = urlsplit(dsn)
    query = [(key, value) for key, value in parse_qsl(parsed.query) if key != "options"]
    query.append(("options", f"-csearch_path={schema}"))
    monkeypatch.setenv(
        "AIRFLOW_CONN_ANALYTICS_DB",
        urlunsplit((parsed.scheme, parsed.netloc, parsed.path, urlencode(query), parsed.fragment)),
    )
    try:
        yield admin_connection, schema
    finally:
        with admin_connection.cursor() as cursor:
            cursor.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))
        admin_connection.close()


def write_curated_csv(path: Path, rows: list[dict[str, str]]) -> None:
    columns = [
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
    ]
    with path.open("w", newline="", encoding="utf-8") as output_file:
        writer = csv.DictWriter(output_file, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)


def listing(
    listing_id: str, name: str, price: str, neighbourhood: str, room_type: str
) -> dict[str, str]:
    return {
        "id": listing_id,
        "name": name,
        "host_id": "10",
        "host_name": "Test Host",
        "neighbourhood_group": "Brooklyn",
        "neighbourhood": neighbourhood,
        "latitude": "40.6",
        "longitude": "-73.9",
        "room_type": room_type,
        "price": price,
        "minimum_nights": "1",
        "number_of_reviews": "2",
        "reviews_per_month": "0.5",
        "calculated_host_listings_count": "1",
        "availability_365": "20",
        "process_date": "2026-11-13",
    }


def test_database_load_upserts_rebuilds_aggregates_and_rolls_back(
    tmp_path, monkeypatch, isolated_schema
):
    psycopg2 = pytest.importorskip("psycopg2")
    from psycopg2 import sql

    admin_connection, schema = isolated_schema
    curated_path = tmp_path / "curated.csv"
    monkeypatch.setattr(analytics_db, "CURATED_PATH", curated_path)

    write_curated_csv(
        curated_path,
        [
            listing("5001", "First", "100", "Kensington", "Private room"),
            listing("5002", "Second", "200", "Astoria", "Entire home/apt"),
        ],
    )
    assert analytics_db.load_curated_data() == 2

    write_curated_csv(
        curated_path,
        [
            listing("5001", "First updated", "150", "Kensington", "Private room"),
            listing("5003", "Third", "300", "Astoria", "Entire home/apt"),
        ],
    )
    assert analytics_db.load_curated_data() == 2

    with admin_connection.cursor() as cursor:
        cursor.execute(
            sql.SQL(
                "SELECT listing_id, name, price FROM {}.real_estate_listings ORDER BY listing_id"
            ).format(sql.Identifier(schema))
        )
        assert cursor.fetchall() == [
            (5001, "First updated", Decimal("150.00")),
            (5003, "Third", Decimal("300.00")),
        ]
        cursor.execute(
            sql.SQL("SELECT COUNT(*) FROM {}.real_estate_neighbourhood_metrics").format(
                sql.Identifier(schema)
            )
        )
        assert cursor.fetchone()[0] == 2
        cursor.execute(
            sql.SQL("SELECT COUNT(*) FROM {}.real_estate_room_type_metrics").format(
                sql.Identifier(schema)
            )
        )
        assert cursor.fetchone()[0] == 2
        cursor.execute(
            sql.SQL(
                "CREATE FUNCTION {}() RETURNS trigger LANGUAGE plpgsql AS $$ "
                "BEGIN RAISE EXCEPTION 'simulated aggregate failure'; END; $$"
            ).format(sql.Identifier(schema, "reject_room_metric"))
        )
        cursor.execute(
            sql.SQL(
                "CREATE TRIGGER reject_room_metric BEFORE INSERT ON "
                "{}.real_estate_room_type_metrics "
                "FOR EACH ROW EXECUTE FUNCTION {}()"
            ).format(
                sql.Identifier(schema),
                sql.Identifier(schema, "reject_room_metric"),
            )
        )

    write_curated_csv(
        curated_path,
        [
            listing("5001", "Should rollback", "999", "Kensington", "Private room"),
            listing("5004", "Also rollback", "400", "Queens", "Shared room"),
        ],
    )
    with pytest.raises(psycopg2.errors.RaiseException, match="simulated aggregate failure"):
        analytics_db.load_curated_data()

    with admin_connection.cursor() as cursor:
        cursor.execute(
            sql.SQL(
                "SELECT listing_id, name, price FROM {}.real_estate_listings ORDER BY listing_id"
            ).format(sql.Identifier(schema))
        )
        assert cursor.fetchall() == [
            (5001, "First updated", Decimal("150.00")),
            (5003, "Third", Decimal("300.00")),
        ]
