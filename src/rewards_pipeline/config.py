"""Configuration loading.

Precedence, lowest to highest: conf/pipeline.yaml -> RP_* environment
variables -> explicit CLI flags. Keeping this in one place means the same job
code runs locally, in Docker and on a cluster with nothing but env changes.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG_PATH = REPO_ROOT / "conf" / "pipeline.yaml"
DEFAULT_EXPECTATIONS_PATH = REPO_ROOT / "conf" / "expectations.yaml"


@dataclass(frozen=True)
class Config:
    app_name: str
    landing: Path
    warehouse: Path
    spark: dict[str, Any]
    seed: dict[str, Any]
    sources: dict[str, Any]
    layers: dict[str, Any]
    quality: dict[str, Any]
    env: str = "local"
    raw: dict[str, Any] = field(default_factory=dict, repr=False)

    # --- path helpers -----------------------------------------------------
    def landing_path(self, source: str) -> Path:
        return self.landing / self.sources[source]["path"]

    def layer_path(self, layer: str, table: str) -> Path:
        return self.warehouse / self.layers[layer]["path"] / table

    def quarantine_path(self, table: str) -> Path:
        return self.warehouse / self.quality["quarantine_path"] / table

    def quality_report_path(self) -> Path:
        return self.warehouse / "_quality"

    def checkpoint_path(self) -> Path:
        return self.warehouse / "_checkpoints"

    def tables(self, layer: str) -> list[str]:
        return list(self.layers[layer]["tables"])


def _resolve(value: str | os.PathLike[str]) -> Path:
    path = Path(value).expanduser()
    return path if path.is_absolute() else (REPO_ROOT / path).resolve()


def load_config(config_path: str | os.PathLike[str] | None = None) -> Config:
    """Read pipeline.yaml and overlay RP_* environment variables."""
    path = Path(config_path) if config_path else DEFAULT_CONFIG_PATH
    with open(path) as handle:
        raw: dict[str, Any] = yaml.safe_load(handle)

    spark = dict(raw.get("spark", {}))
    if shuffle := os.getenv("SPARK_SHUFFLE_PARTITIONS"):
        spark["shuffle_partitions"] = int(shuffle)

    return Config(
        app_name=os.getenv("RP_APP_NAME", raw["app_name"]),
        landing=_resolve(os.getenv("RP_LANDING_PATH", raw["paths"]["landing"])),
        warehouse=_resolve(os.getenv("RP_WAREHOUSE_PATH", raw["paths"]["warehouse"])),
        spark=spark,
        seed=raw.get("seed", {}),
        sources=raw.get("sources", {}),
        layers=raw.get("layers", {}),
        quality=raw.get("quality", {}),
        env=os.getenv("RP_ENV", "local"),
        raw=raw,
    )


@lru_cache(maxsize=1)
def load_expectations(path: str | os.PathLike[str] | None = None) -> dict[str, list[dict]]:
    """Load the declarative data-quality contract keyed by `<layer>.<table>`."""
    target = Path(path) if path else DEFAULT_EXPECTATIONS_PATH
    if not target.exists():
        return {}
    with open(target) as handle:
        return yaml.safe_load(handle) or {}
