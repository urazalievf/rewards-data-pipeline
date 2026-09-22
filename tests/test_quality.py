"""Unit tests for the expectation engine."""

from __future__ import annotations

import pytest

from rewards_pipeline.quality import DataQualityError, run_checks

ROWS = [
    ("T1", "settled", 10.0),
    ("T2", "settled", -5.0),
    ("T3", "refunded", 900.0),
    ("T4", None, 12.0),
]
COLUMNS = ["txn_id", "status", "amount_usd"]


@pytest.fixture
def frame(spark):
    return spark.createDataFrame(ROWS, COLUMNS)


def test_passing_contract_returns_results(spark, config, frame):
    expectations = {
        "silver.demo": [
            {"type": "row_count_min", "value": 4},
            {"type": "unique", "column": "txn_id"},
            {"type": "not_null", "column": "amount_usd"},
        ]
    }
    results = run_checks(frame, config, "silver", "demo", spark, expectations)
    assert len(results) == 3
    assert all(result.passed for result in results)


def test_fail_severity_raises(spark, config, frame):
    expectations = {"silver.demo": [{"type": "not_null", "column": "status", "severity": "fail"}]}
    with pytest.raises(DataQualityError, match="not_null"):
        run_checks(frame, config, "silver", "demo", spark, expectations)


def test_warn_severity_does_not_raise(spark, config, frame):
    expectations = {"silver.demo": [{"type": "not_null", "column": "status", "severity": "warn"}]}
    results = run_checks(frame, config, "silver", "demo", spark, expectations)
    assert results[0].passed is False
    assert results[0].observed == 1


def test_range_flags_outliers(spark, config, frame):
    expectations = {
        "silver.demo": [
            {"type": "range", "column": "amount_usd", "min": 0, "max": 100, "severity": "warn"}
        ]
    }
    results = run_checks(frame, config, "silver", "demo", spark, expectations)
    assert results[0].observed == 2  # -5.0 and 900.0


def test_accepted_values(spark, config, frame):
    expectations = {
        "silver.demo": [
            {
                "type": "accepted_values",
                "column": "status",
                "values": ["settled", "refunded"],
                "severity": "warn",
            }
        ]
    }
    results = run_checks(frame, config, "silver", "demo", spark, expectations)
    assert results[0].passed is True  # NULL is not an unexpected value


def test_unknown_expectation_type_is_rejected(spark, config, frame):
    expectations = {"silver.demo": [{"type": "definitely_not_a_check"}]}
    with pytest.raises(ValueError, match="unknown expectation type"):
        run_checks(frame, config, "silver", "demo", spark, expectations)


def test_report_written_to_warehouse(spark, config, frame):
    expectations = {"silver.demo": [{"type": "row_count_min", "value": 1}]}
    run_checks(frame, config, "silver", "demo", spark, expectations)
    assert (config.quality_report_path() / "silver_demo.json").exists()
