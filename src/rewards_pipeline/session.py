"""SparkSession construction and structured logging."""

from __future__ import annotations

import logging
import os
import sys
from collections.abc import Iterator
from contextlib import contextmanager

from pyspark.sql import SparkSession

from .config import Config

_LOG_FORMAT = "%(asctime)s | %(levelname)-7s | %(name)s | %(message)s"


def configure_logging(level: str | None = None) -> None:
    resolved = level or os.getenv("RP_LOG_LEVEL") or "INFO"
    logging.basicConfig(
        level=resolved.upper(),
        format=_LOG_FORMAT,
        stream=sys.stdout,
        force=True,
    )
    # Spark's own logging is noisy at INFO and drowns the pipeline's messages.
    logging.getLogger("py4j").setLevel(logging.ERROR)


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(f"rewards.{name}")


def build_session(config: Config, app_suffix: str = "") -> SparkSession:
    """Create (or reuse) the SparkSession for a job.

    `SPARK_MASTER_URL` decides local vs cluster; no code change needed.
    """
    name = f"{config.app_name}{'-' + app_suffix if app_suffix else ''}"
    builder = SparkSession.builder.appName(name)

    master = os.getenv("SPARK_MASTER_URL")
    if master:
        builder = builder.master(master)
    else:
        builder = builder.master(os.getenv("SPARK_LOCAL_MASTER", "local[*]"))

    if not master:
        # Laptops (macOS especially) often resolve their hostname to an address
        # the driver cannot bind to. Pinning loopback avoids a 16-retry
        # BindException on a fresh clone; cluster runs are left alone.
        builder = builder.config(
            "spark.driver.bindAddress", os.getenv("SPARK_DRIVER_BIND_ADDRESS", "127.0.0.1")
        ).config("spark.driver.host", os.getenv("SPARK_DRIVER_HOST", "127.0.0.1"))

    builder = builder.config(
        "spark.sql.shuffle.partitions", str(config.spark.get("shuffle_partitions", 8))
    )
    for key, value in (config.spark.get("configs") or {}).items():
        builder = builder.config(key, str(value))

    if config.catalog.get("enabled"):
        # A persistent catalog is what makes deployed DDL meaningful: without
        # it, CREATE TABLE lives and dies with the session.
        builder = builder.enableHiveSupport()

    session = builder.getOrCreate()
    session.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))
    return session


@contextmanager
def spark_session(config: Config, app_suffix: str = "") -> Iterator[SparkSession]:
    session = build_session(config, app_suffix)
    try:
        yield session
    finally:
        # Long-lived sessions are the orchestrator's job, not a single task's.
        if os.getenv("RP_KEEP_SESSION", "0") != "1":
            session.stop()
