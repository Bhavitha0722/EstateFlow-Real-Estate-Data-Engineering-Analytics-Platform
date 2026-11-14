import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

import api.main as api
from api.main import (
    data_quality as data_quality_summary,
)
from api.main import (
    listings,
    pipeline_status,
    retry_pipeline,
    room_type_metrics,
    summary,
    trigger_pipeline,
)


class FakeCursor:
    def __init__(self, rows):
        self.rows = rows
        self.executed = None

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, query, parameters=None):
        self.executed = (query, parameters)

    def fetchone(self):
        return self.rows[0] if self.rows else None

    def fetchall(self):
        return self.rows


class FakeConnection:
    def __init__(self, rows):
        self.cursor_instance = FakeCursor(rows)

    def cursor(self, **_kwargs):
        return self.cursor_instance


def test_summary_maps_database_row():
    connection = FakeConnection(
        [
            {
                "listing_count": 10,
                "neighbourhood_count": 2,
                "room_type_count": 3,
                "average_price": 125.5,
                "average_availability": 100.0,
                "last_loaded_at": "2026-08-26 12:00:00+00",
            }
        ]
    )

    response = summary(connection)

    assert response.listing_count == 10
    assert response.average_price == 125.5
    assert "ROUND(AVG(availability_365), 1)" in connection.cursor_instance.executed[0]


def test_summary_supports_empty_dataset():
    response = summary(
        FakeConnection(
            [
                {
                    "listing_count": 0,
                    "neighbourhood_count": 0,
                    "room_type_count": 0,
                    "average_price": None,
                    "average_availability": None,
                    "last_loaded_at": None,
                }
            ]
        )
    )

    assert response.listing_count == 0
    assert response.average_price is None
    assert response.average_availability is None


def test_summary_http_endpoint_uses_database_dependency(monkeypatch):
    connection = FakeConnection(
        [
            {
                "listing_count": 4,
                "neighbourhood_count": 2,
                "room_type_count": 3,
                "average_price": 125.5,
                "average_availability": 100.0,
                "last_loaded_at": "2026-11-13 12:00:00+00",
            }
        ]
    )
    monkeypatch.setitem(api.app.dependency_overrides, api.get_connection, lambda: connection)

    response = TestClient(api.app).get("/api/v1/summary")

    assert response.status_code == 200
    assert response.json()["listing_count"] == 4
    assert response.json()["average_price"] == 125.5
    assert response.headers["x-content-type-options"] == "nosniff"


def make_quality_row(total_records=100):
    row = {
        "total_records": total_records,
        "missing_values": 0,
        "duplicate_ids": 0,
        "invalid_prices": 0,
    }
    row.update({f"missing_{column}": 0 for column in api.DATA_QUALITY_MISSING_EXPRESSIONS})
    return row


def test_data_quality_reports_live_table_checks():
    row = make_quality_row()
    row["missing_values"] = 11
    row["missing_reviews_per_month"] = 10
    row["missing_name"] = 1
    connection = FakeConnection([row])

    response = data_quality_summary(connection)

    assert response.total_records == 100
    assert response.missing_values == 11
    assert response.duplicate_ids == 0
    assert response.invalid_prices == 0
    assert response.validation_status == "passed"
    query, parameters = connection.cursor_instance.executed
    assert "FROM real_estate_listings" in query
    assert "COUNT(DISTINCT listing_id)" in query
    assert parameters == (api.DATA_QUALITY_MIN_PRICE, api.DATA_QUALITY_MAX_PRICE)


def test_data_quality_fails_duplicates_invalid_prices_and_excess_nulls():
    row = make_quality_row(total_records=10)
    row.update(
        missing_values=3,
        missing_reviews_per_month=3,
        duplicate_ids=1,
        invalid_prices=1,
    )

    response = data_quality_summary(FakeConnection([row]))

    assert response.validation_status == "failed"
    assert response.duplicate_ids == 1
    assert response.invalid_prices == 1


def test_data_quality_marks_empty_table():
    response = data_quality_summary(FakeConnection([make_quality_row(total_records=0)]))

    assert response.total_records == 0
    assert response.validation_status == "empty"


def test_room_type_metrics_are_typed():
    response = room_type_metrics(
        FakeConnection(
            [
                {
                    "room_type": "Private room",
                    "listing_count": 4,
                    "average_price": 80.0,
                    "average_reviews": 12.5,
                    "average_availability": 200.0,
                }
            ]
        )
    )

    assert response[0].room_type == "Private room"
    assert response[0].listing_count == 4


def test_listings_bound_filters_and_pagination():
    connection = FakeConnection(
        [
            {
                "listing_id": 5001,
                "name": "Listing",
                "host_name": "Host",
                "neighbourhood_group": "Brooklyn",
                "neighbourhood": "Kensington",
                "room_type": "Private room",
                "price": 100.0,
                "minimum_nights": 1,
                "number_of_reviews": 2,
                "availability_365": 10,
            }
        ]
    )

    response = listings(
        connection,
        neighbourhood_group="Brooklyn",
        room_type="Private room",
        limit=10,
        offset=20,
    )

    assert len(response.items) == 1
    assert connection.cursor_instance.executed[1] == ["Brooklyn", "Private room", 10, 20]


def test_operations_key_rejects_missing_or_invalid_key(monkeypatch):
    monkeypatch.setattr(api, "OPERATIONS_API_KEY", "expected")

    with pytest.raises(HTTPException) as missing:
        api.require_authenticated(None, None, None)
    assert missing.value.status_code == 401

    with pytest.raises(HTTPException) as invalid:
        api.require_authenticated(None, None, "wrong")
    assert invalid.value.status_code == 401


def test_local_role_override_enforces_valid_roles_only(monkeypatch):
    monkeypatch.setattr(api, "ENVIRONMENT", "development")
    monkeypatch.setattr(api, "OPERATIONS_API_KEY", "expected")

    viewer = api.require_authenticated(None, None, "expected", "viewer")
    assert viewer["roles"] == {"viewer"}
    assert api.require_viewer(viewer)["roles"] == {"viewer"}

    operator = api.require_authenticated(None, None, "expected", "operator")
    assert operator["roles"] == {"operator"}
    assert api.require_operator(operator)["roles"] == {"operator"}

    admin = api.require_authenticated(None, None, "expected", "admin")
    assert admin["roles"] == {"admin"}
    assert api.require_operator(admin)["roles"] == {"admin"}

    with pytest.raises(HTTPException) as invalid:
        api.require_authenticated(None, None, "expected", "superuser")
    assert invalid.value.status_code == 401


def test_role_checks_enforce_boundary_between_viewer_and_operator():
    assert api.require_viewer({"roles": {"viewer"}})["roles"] == {"viewer"}

    with pytest.raises(HTTPException) as forbidden:
        api.require_operator({"roles": {"viewer"}})
    assert forbidden.value.status_code == 403

    assert api.require_operator({"roles": {"operator"}})["roles"] == {"operator"}


def test_trigger_pipeline_returns_airflow_run(monkeypatch):
    monkeypatch.setattr(
        api,
        "airflow_request",
        lambda method, path, json: {"dag_run_id": "api__test", "state": "queued"},
    )

    response = trigger_pipeline({"roles": {"admin"}})

    assert response.dag_id == "real_estate_analytics"
    assert response.dag_run_id == "api__test"
    assert response.state == "queued"


def test_retry_pipeline_requires_failed_run_and_preserves_source(monkeypatch):
    requests = []

    def fake_airflow_request(method, path, json=None):
        requests.append((method, path, json))
        if method == "GET":
            return {"dag_run_id": "manual__failed", "state": "failed"}
        return {"dag_run_id": "api_retry__test", "state": "queued"}

    monkeypatch.setattr(api, "airflow_request", fake_airflow_request)

    response = retry_pipeline("manual__failed", {"roles": {"operator"}})

    assert response.retry_of == "manual__failed"
    assert response.state == "queued"
    assert requests[0] == (
        "GET",
        "/dags/real_estate_analytics/dagRuns/manual__failed",
        None,
    )
    assert requests[1][0:2] == ("POST", "/dags/real_estate_analytics/dagRuns")
    assert requests[1][2]["conf"]["retry_of"] == "manual__failed"


def test_retry_pipeline_rejects_non_failed_run(monkeypatch):
    monkeypatch.setattr(
        api,
        "airflow_request",
        lambda method, path, json=None: {"dag_run_id": "manual__success", "state": "success"},
    )

    with pytest.raises(HTTPException) as conflict:
        retry_pipeline("manual__success", {"roles": {"operator"}})

    assert conflict.value.status_code == 409


def test_pipeline_status_combines_airflow_and_data_freshness(monkeypatch):
    monkeypatch.setattr(
        api,
        "airflow_request",
        lambda method, path: {
            "dag_runs": [
                {
                    "dag_run_id": "manual__test",
                    "state": "failed",
                    "execution_date": "2026-08-27T12:00:00+00:00",
                },
                {
                    "dag_run_id": "manual__success",
                    "state": "success",
                    "execution_date": "2026-08-26T12:00:00+00:00",
                    "end_date": "2026-08-26T12:05:00+00:00",
                },
            ]
        },
    )
    connection = FakeConnection([{"data_loaded_at": "2026-08-26 12:00:00+00"}])

    response = pipeline_status(connection, {"roles": {"viewer"}})

    assert response.state == "failed"
    assert response.latest_successful_run_at == "2026-08-26T12:05:00+00:00"
    assert response.freshness in {"fresh", "stale"}


def test_pipeline_history_returns_recent_runs_and_failed_tasks(monkeypatch):
    def fake_airflow_request(method, path, json=None):
        if path.endswith("dagRuns?limit=10&order_by=-execution_date"):
            return {
                "dag_runs": [
                    {
                        "dag_run_id": "manual__failed",
                        "state": "failed",
                        "execution_date": "2026-08-26T12:00:00+00:00",
                        "start_date": "2026-08-26T12:00:00+00:00",
                        "end_date": "2026-08-26T12:05:00+00:00",
                    },
                    {
                        "dag_run_id": "manual__success",
                        "state": "success",
                        "execution_date": "2026-08-25T12:00:00+00:00",
                        "start_date": "2026-08-25T12:00:00+00:00",
                        "end_date": "2026-08-25T12:02:00+00:00",
                    },
                ]
            }
        if path.endswith("dagRuns/manual__failed/taskInstances?limit=200"):
            return {
                "task_instances": [
                    {"task_id": "clean_data", "state": "failed"},
                    {"task_id": "validate_file", "state": "success"},
                ]
            }
        if path.endswith("dagRuns/manual__success/taskInstances?limit=200"):
            return {"task_instances": [{"task_id": "load_data", "state": "success"}]}
        raise AssertionError(f"Unexpected airflow path: {path}")

    monkeypatch.setattr(api, "airflow_request", fake_airflow_request)

    response = api.pipeline_history({"roles": {"viewer"}})

    assert response.dag_id == "real_estate_analytics"
    assert response.latest_failed == "manual__failed"
    assert response.recent_runs[0].failed_task_count == 1
    assert response.recent_runs[1].failed_task_count == 0
