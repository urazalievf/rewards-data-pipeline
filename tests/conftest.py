"""Shared pytest fixtures.

One SparkSession is created per test session — starting a JVM per test would
dominate the runtime. Everything else is built in-memory so the suite never
touches the real landing zone.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

SRC = Path(__file__).resolve().parents[1] / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from rewards_pipeline.config import load_config  # noqa: E402


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.appName("rewards-tests")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.host", "127.0.0.1")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def config(tmp_path):
    """A config pointed at a throwaway warehouse."""
    base = load_config()
    return type(base)(
        app_name=base.app_name,
        landing=tmp_path / "landing",
        warehouse=tmp_path / "warehouse",
        spark=base.spark,
        seed=base.seed,
        sources=base.sources,
        layers=base.layers,
        quality=base.quality,
        env="test",
        raw=base.raw,
    )


@pytest.fixture
def sql_runner(spark):
    """Run a packaged .sql file against views the test registered."""
    from rewards_pipeline.io import load_sql

    def _run(layer: str, name: str):
        return spark.sql(load_sql(layer, name))

    return _run
