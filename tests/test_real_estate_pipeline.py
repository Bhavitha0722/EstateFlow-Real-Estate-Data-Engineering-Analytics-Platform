import csv
import json

import analytics_db
import clean
import data_quality
import filter
import pytest
from analytics_db import read_curated_listings


def write_listings(path, rows):
    fieldnames = [
        "id",
        "name",
        "neighbourhood_group",
        "room_type",
        "price",
        "availability_365",
        "host_id",
        "host_name",
        "neighbourhood",
        "latitude",
        "longitude",
        "minimum_nights",
        "number_of_reviews",
        "reviews_per_month",
        "calculated_host_listings_count",
        "process_date",
    ]
    with path.open("w", newline="", encoding="utf-8") as output_file:
        writer = csv.DictWriter(output_file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def test_pre_process_validates_and_cleans_input(tmp_path, monkeypatch):
    input_dir = tmp_path / "input_files"
    input_dir.mkdir()
    write_listings(
        input_dir / "modified_data.csv",
        [
            {
                "id": "5001",
                "name": " Listing ",
                "neighbourhood_group": "Brooklyn",
                "room_type": "Private room",
                "price": "100",
                "availability_365": "10",
            }
        ],
    )
    monkeypatch.setenv("AIRFLOW_DATA_DIR", str(tmp_path))
    monkeypatch.setattr(clean, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(clean, "INPUT_PATH", input_dir / "modified_data.csv")
    monkeypatch.setattr(clean, "CLEANED_PATH", input_dir / "modified_data_cleaned.csv")

    clean.pre_process(process_date="2026-09-30")

    with (input_dir / "modified_data_cleaned.csv").open(
        newline="", encoding="utf-8"
    ) as cleaned_file:
        cleaned = next(csv.DictReader(cleaned_file))
    assert cleaned["name"] == "Listing"
    assert cleaned["host_name"] == ""
    assert cleaned["reviews_per_month"] == ""
    assert cleaned["id"] == "5001"
    assert cleaned["process_date"] == "2026-09-30"


def test_pre_process_rejects_missing_required_columns(tmp_path, monkeypatch):
    input_path = tmp_path / "modified_data.csv"
    input_path.write_text("id,name\n5001,Listing\n", encoding="utf-8")
    monkeypatch.setattr(clean, "INPUT_PATH", input_path)
    monkeypatch.setattr(clean, "CLEANED_PATH", tmp_path / "cleaned.csv")

    with pytest.raises(ValueError, match="missing required columns"):
        clean.pre_process()


def test_pre_process_rejects_empty_input(tmp_path, monkeypatch):
    input_path = tmp_path / "modified_data.csv"
    write_listings(input_path, [])
    monkeypatch.setattr(clean, "INPUT_PATH", input_path)
    monkeypatch.setattr(clean, "CLEANED_PATH", tmp_path / "cleaned.csv")

    with pytest.raises(ValueError, match="contains no listings"):
        clean.pre_process()


def test_filter_function_writes_metrics(tmp_path, monkeypatch):
    input_dir = tmp_path / "input_files"
    output_dir = tmp_path / "output_files"
    input_dir.mkdir()
    output_dir.mkdir()
    write_listings(
        input_dir / "modified_data_cleaned.csv",
        [
            {
                "id": "5001",
                "name": "A",
                "neighbourhood_group": "Brooklyn",
                "room_type": "Private room",
                "price": "100",
                "availability_365": "10",
            },
            {
                "id": "4001",
                "name": "B",
                "neighbourhood_group": "Queens",
                "room_type": "Entire home/apt",
                "price": "200",
                "availability_365": "20",
            },
        ],
    )
    monkeypatch.setattr(filter, "INPUT_PATH", input_dir / "modified_data_cleaned.csv")
    monkeypatch.setattr(filter, "OUTPUT_PATH", output_dir / "modified_data_filtered.csv")
    monkeypatch.setattr(filter, "METRICS_PATH", output_dir / "real_estate_metrics.json")

    filter.filter_function()

    metrics = json.loads((output_dir / "real_estate_metrics.json").read_text(encoding="utf-8"))
    assert metrics == {
        "total_listings": 2,
        "filtered_listings": 1,
        "average_filtered_price": 100.0,
    }
    with (output_dir / "modified_data_filtered.csv").open(
        newline="", encoding="utf-8"
    ) as output_file:
        assert [row["id"] for row in csv.DictReader(output_file)] == ["5001"]


def test_filter_function_handles_no_matching_listings(tmp_path, monkeypatch):
    input_dir = tmp_path / "input_files"
    output_dir = tmp_path / "output_files"
    input_dir.mkdir()
    output_dir.mkdir()
    write_listings(
        input_dir / "modified_data_cleaned.csv",
        [{"id": "4001", "price": "200", "name": "B"}],
    )
    monkeypatch.setattr(filter, "INPUT_PATH", input_dir / "modified_data_cleaned.csv")
    monkeypatch.setattr(filter, "OUTPUT_PATH", output_dir / "modified_data_filtered.csv")
    monkeypatch.setattr(filter, "METRICS_PATH", output_dir / "real_estate_metrics.json")

    filter.filter_function()

    metrics = json.loads((output_dir / "real_estate_metrics.json").read_text(encoding="utf-8"))
    assert metrics == {
        "total_listings": 1,
        "filtered_listings": 0,
        "average_filtered_price": 0,
    }
    with (output_dir / "modified_data_filtered.csv").open(
        newline="", encoding="utf-8"
    ) as output_file:
        assert list(csv.DictReader(output_file)) == []


def test_filtered_output_is_loadable_and_preserves_missing_review_rate(tmp_path, monkeypatch):
    input_dir = tmp_path / "input_files"
    output_dir = tmp_path / "output_files"
    input_dir.mkdir()
    output_dir.mkdir()
    source_path = input_dir / "modified_data.csv"
    cleaned_path = input_dir / "modified_data_cleaned.csv"
    filtered_path = output_dir / "modified_data_filtered.csv"
    write_listings(
        source_path,
        [
            {
                "id": "5001",
                "name": "A",
                "host_id": "1",
                "host_name": "Host",
                "neighbourhood_group": "Brooklyn",
                "neighbourhood": "Kensington",
                "latitude": "40.6",
                "longitude": "-73.9",
                "room_type": "Private room",
                "price": "100",
                "minimum_nights": "1",
                "number_of_reviews": "1",
                "reviews_per_month": "",
                "calculated_host_listings_count": "1",
                "availability_365": "10",
            },
            {
                "id": "4001",
                "name": "B",
                "host_id": "2",
                "host_name": "Host",
                "neighbourhood_group": "Queens",
                "neighbourhood": "Astoria",
                "latitude": "40.7",
                "longitude": "-73.8",
                "room_type": "Entire home/apt",
                "price": "200",
                "minimum_nights": "1",
                "number_of_reviews": "2",
                "reviews_per_month": "0.2",
                "calculated_host_listings_count": "1",
                "availability_365": "20",
            },
        ],
    )
    monkeypatch.setattr(clean, "INPUT_PATH", source_path)
    monkeypatch.setattr(clean, "CLEANED_PATH", cleaned_path)
    monkeypatch.setattr(filter, "INPUT_PATH", cleaned_path)
    monkeypatch.setattr(filter, "OUTPUT_PATH", filtered_path)
    monkeypatch.setattr(filter, "METRICS_PATH", output_dir / "metrics.json")
    monkeypatch.setattr(analytics_db, "CURATED_PATH", filtered_path)

    clean.pre_process(process_date="2026-11-13")
    filter.filter_function()
    listings = analytics_db.read_curated_listings(filtered_path)

    assert [listing.listing_id for listing in listings] == [5001]
    assert listings[0].reviews_per_month is None


def test_read_curated_listings_rejects_duplicate_ids(tmp_path):
    path = tmp_path / "curated.csv"
    write_listings(
        path,
        [
            {
                "id": "5001",
                "name": "A",
                "host_id": "1",
                "host_name": "Host",
                "neighbourhood_group": "Brooklyn",
                "neighbourhood": "Kensington",
                "latitude": "40.6",
                "longitude": "-73.9",
                "room_type": "Private room",
                "price": "100",
                "minimum_nights": "1",
                "number_of_reviews": "1",
                "reviews_per_month": "0.1",
                "calculated_host_listings_count": "1",
                "availability_365": "10",
                "process_date": "2026-08-26",
            }
        ],
    )
    rows = path.read_text(encoding="utf-8").replace("\n", "\n", 1)
    path.write_text(rows + rows.splitlines()[1] + "\n", encoding="utf-8")

    try:
        read_curated_listings(path)
    except ValueError as error:
        assert "Duplicate listing id" in str(error)
    else:
        raise AssertionError("Duplicate IDs must be rejected")


def test_data_quality_accepts_valid_rows_with_tolerated_nulls(tmp_path):
    path = tmp_path / "quality.csv"
    write_listings(
        path,
        [
            {
                "id": "5001",
                "name": "A",
                "host_id": "1",
                "host_name": "Host A",
                "neighbourhood_group": "Brooklyn",
                "neighbourhood": "Kensington",
                "latitude": "40.6",
                "longitude": "-73.9",
                "room_type": "Private room",
                "price": "100",
                "minimum_nights": "1",
                "number_of_reviews": "1",
                "reviews_per_month": "0.1",
                "calculated_host_listings_count": "1",
                "availability_365": "10",
            },
            {
                "id": "5002",
                "name": "B",
                "host_id": "2",
                "host_name": "Host B",
                "neighbourhood_group": "Queens",
                "neighbourhood": "Astoria",
                "latitude": "40.7",
                "longitude": "-73.8",
                "room_type": "Entire home/apt",
                "price": "200",
                "minimum_nights": "1",
                "number_of_reviews": "2",
                "reviews_per_month": "",
                "calculated_host_listings_count": "1",
                "availability_365": "20",
            },
        ],
    )

    result = data_quality.validate_data_quality(
        path,
        min_row_count=2,
        max_row_count=2,
        max_null_rate=0.5,
    )

    assert result["row_count"] == 2
    assert result["null_rates"]["reviews_per_month"] == 0.5


def test_data_quality_rejects_null_ids_duplicate_ids_and_out_of_range_prices(tmp_path):
    path = tmp_path / "quality.csv"
    write_listings(
        path,
        [
            {"id": "5001", "price": "100", "name": "A"},
            {"id": "5001", "price": "10001", "name": "B"},
            {"id": "", "price": "100", "name": "C"},
        ],
    )

    try:
        data_quality.validate_data_quality(
            path,
            min_row_count=1,
            max_row_count=10,
            max_null_rate=1.0,
        )
    except ValueError as error:
        message = str(error)
        assert "null" in message.lower()
        assert "duplicate" in message.lower()
        assert "price" in message.lower()
    else:
        raise AssertionError("Invalid rows must fail the quality gate")


def test_data_quality_rejects_unexpected_row_count(tmp_path):
    path = tmp_path / "quality.csv"
    write_listings(path, [{"id": "5001", "price": "100", "name": "A"}])

    try:
        data_quality.validate_data_quality(path, min_row_count=2, max_row_count=10)
    except ValueError as error:
        assert "row count" in str(error).lower()
    else:
        raise AssertionError("Unexpected row counts must fail the quality gate")
