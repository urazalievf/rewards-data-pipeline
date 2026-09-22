"""Declarative data-quality gate.

Expectations live in `conf/expectations.yaml`, not in job code, so adding a
check is a config change and reviewers can read the contract in one file.
Each check returns a structured result; a breach at `fail` severity stops the
run before bad data reaches the next layer.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .config import Config, load_expectations
from .session import get_logger

log = get_logger("quality")


class DataQualityError(RuntimeError):
    """Raised when one or more `fail`-severity expectations are breached."""


@dataclass
class CheckResult:
    table: str
    check: str
    column: str | None
    severity: str
    passed: bool
    observed: Any
    detail: str

    def render(self) -> str:
        status = "PASS" if self.passed else self.severity.upper()
        target = f"{self.table}.{self.column}" if self.column else self.table
        return f"[{status:4}] {target:<40} {self.check:<22} {self.detail}"


# --------------------------------------------------------------------------
# Individual checks
# --------------------------------------------------------------------------
def _row_count_min(df: DataFrame, spec: dict) -> tuple[bool, Any, str]:
    count = df.count()
    minimum = int(spec["value"])
    return count >= minimum, count, f"{count} rows (min {minimum})"


def _not_null(df: DataFrame, spec: dict) -> tuple[bool, Any, str]:
    column = spec["column"]
    nulls = df.filter(F.col(column).isNull()).count()
    return nulls == 0, nulls, f"{nulls} null(s)"


def _unique(df: DataFrame, spec: dict) -> tuple[bool, Any, str]:
    column = spec["column"]
    total, distinct = df.count(), df.select(column).distinct().count()
    return total == distinct, total - distinct, f"{total - distinct} duplicate key(s)"


def _unique_composite(df: DataFrame, spec: dict) -> tuple[bool, Any, str]:
    columns = spec["columns"]
    total, distinct = df.count(), df.select(*columns).distinct().count()
    return total == distinct, total - distinct, f"{total - distinct} duplicate combo(s)"


def _accepted_values(df: DataFrame, spec: dict) -> tuple[bool, Any, str]:
    column, allowed = spec["column"], spec["values"]
    offenders = df.filter(F.col(column).isNotNull() & ~F.col(column).isin(allowed))
    count = offenders.count()
    return count == 0, count, f"{count} row(s) outside {allowed}"


def _range(df: DataFrame, spec: dict) -> tuple[bool, Any, str]:
    column = spec["column"]
    condition = F.col(column).isNotNull()
    if (low := spec.get("min")) is not None:
        condition &= F.col(column) < F.lit(low)
    if (high := spec.get("max")) is not None:
        condition = condition | (F.col(column) > F.lit(high))
    count = df.filter(condition).count()
    return count == 0, count, f"{count} row(s) outside [{spec.get('min')}, {spec.get('max')}]"


def _corrupt_ratio_max(df: DataFrame, spec: dict) -> tuple[bool, Any, str]:
    column = spec.get("column", "_corrupt_record")
    if column not in df.columns:
        return True, 0.0, "no corrupt-record column present"
    total = df.count()
    if total == 0:
        return True, 0.0, "empty table"
    bad = df.filter(F.col(column).isNotNull()).count()
    ratio = bad / total
    limit = float(spec["value"])
    return ratio <= limit, round(ratio, 4), f"{bad}/{total} corrupt ({ratio:.2%}, max {limit:.2%})"


def _freshness_days_max(df: DataFrame, spec: dict) -> tuple[bool, Any, str]:
    column = spec["column"]
    newest = df.select(F.max(F.col(column)).alias("m")).collect()[0]["m"]
    if newest is None:
        return False, None, "no timestamps found"
    if isinstance(newest, str):
        return True, newest, "column is not a timestamp; skipped"
    newest_utc = newest if newest.tzinfo else newest.replace(tzinfo=timezone.utc)
    age = (datetime.now(timezone.utc) - newest_utc).days
    limit = int(spec["value"])
    if age < 0:
        return True, age, f"newest row is future-dated by {abs(age)}d"
    return age <= limit, age, f"newest row is {age}d old (max {limit}d)"


def _referential_integrity(
    df: DataFrame, spec: dict, spark: SparkSession, config: Config
) -> tuple[bool, Any, str]:
    from .io import read_table  # local import avoids a circular module graph

    ref_layer, ref_name = spec["ref_table"].split(".")
    reference = (
        read_table(spark, config, ref_layer, ref_name)
        .select(F.col(spec["ref_column"]).alias("_ref_key"))
        .distinct()
    )
    orphans = (
        df.select(F.col(spec["column"]).alias("_key"))
        .filter(F.col("_key").isNotNull())
        .distinct()
        .join(reference, F.col("_key") == F.col("_ref_key"), "left_anti")
        .count()
    )
    return orphans == 0, orphans, f"{orphans} orphan key(s) vs {spec['ref_table']}"


_CHECKS = {
    "row_count_min": _row_count_min,
    "not_null": _not_null,
    "unique": _unique,
    "unique_composite": _unique_composite,
    "accepted_values": _accepted_values,
    "range": _range,
    "corrupt_ratio_max": _corrupt_ratio_max,
    "freshness_days_max": _freshness_days_max,
}


# --------------------------------------------------------------------------
# Runner
# --------------------------------------------------------------------------
def run_checks(
    df: DataFrame,
    config: Config,
    layer: str,
    table: str,
    spark: SparkSession | None = None,
    expectations: dict[str, list[dict]] | None = None,
) -> list[CheckResult]:
    """Evaluate every expectation declared for `<layer>.<table>`."""
    key = f"{layer}.{table}"
    contract = (expectations or load_expectations()).get(key, [])
    if not contract:
        log.info("no expectations declared for %s", key)
        return []

    default_severity = config.quality.get("on_breach_default", "fail")
    df.cache()
    results: list[CheckResult] = []
    try:
        for spec in contract:
            kind = spec["type"]
            severity = spec.get("severity", default_severity)
            # `where` scopes any check to a subset, e.g. "one current row per
            # card" or "ignore rows the reader could not parse".
            target = df.filter(F.expr(spec["where"])) if spec.get("where") else df
            try:
                if kind == "referential_integrity":
                    if spark is None:
                        raise ValueError("referential_integrity needs a SparkSession")
                    passed, observed, detail = _referential_integrity(target, spec, spark, config)
                else:
                    passed, observed, detail = _CHECKS[kind](target, spec)
            except KeyError as exc:
                raise ValueError(f"unknown expectation type: {kind}") from exc

            result = CheckResult(
                table=key,
                check=kind,
                column=spec.get("column")
                or (",".join(spec["columns"]) if "columns" in spec else None),
                severity=severity,
                passed=passed,
                observed=observed,
                detail=detail,
            )
            results.append(result)
            log.info(result.render())
    finally:
        df.unpersist()

    _write_report(config, key, results)

    breaches = [r for r in results if not r.passed and r.severity == "fail"]
    if breaches:
        summary = "; ".join(f"{r.check} on {r.column or r.table}: {r.detail}" for r in breaches)
        raise DataQualityError(f"{key} failed {len(breaches)} expectation(s) -> {summary}")

    warnings = [r for r in results if not r.passed]
    if warnings:
        log.warning("%s passed with %d warning(s)", key, len(warnings))
    return results


def _write_report(config: Config, key: str, results: list[CheckResult]) -> None:
    directory = config.quality_report_path()
    directory.mkdir(parents=True, exist_ok=True)
    payload = {
        "table": key,
        "evaluated_at": datetime.now(timezone.utc).isoformat(),
        "passed": all(r.passed for r in results),
        "results": [asdict(r) for r in results],
    }
    with open(directory / f"{key.replace('.', '_')}.json", "w") as handle:
        json.dump(payload, handle, indent=2, default=str)
