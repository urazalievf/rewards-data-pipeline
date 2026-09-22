"""Configuration loading.

Precedence, lowest to highest:

    conf/pipeline.yaml
      -> conf/pipeline.<env>.yaml   (selected by RP_ENV, deep-merged)
      -> RP_* environment variables
      -> explicit CLI flags

Keeping this in one place is what lets the same job code run on a laptop, in
Docker and on a cluster against object storage with nothing but config
changes - environments are configuration here, never branches.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
CONF_DIR = REPO_ROOT / "conf"
DEFAULT_CONFIG_PATH = CONF_DIR / "pipeline.yaml"
DEFAULT_EXPECTATIONS_PATH = CONF_DIR / "expectations.yaml"


def is_uri(location: str) -> bool:
    """True for object-store / HDFS style locations such as s3a://bucket/x.

    These must never be pushed through pathlib: `Path("s3a://b/x")` collapses
    the double slash and yields `s3a:/b/x`, which no filesystem accepts.
    """
    return "://" in location


@dataclass(frozen=True)
class Config:
    app_name: str
    # Data locations are plain strings, not Paths, because they may be URIs.
    landing: str
    warehouse: str
    # Run reports and manifests are always written to the local filesystem,
    # even when the warehouse lives in object storage.
    reports: Path
    spark: dict[str, Any]
    seed: dict[str, Any]
    sources: dict[str, Any]
    layers: dict[str, Any]
    quality: dict[str, Any]
    env: str = "local"
    raw: dict[str, Any] = field(default_factory=dict, repr=False)

    # --- location helpers -------------------------------------------------
    @staticmethod
    def join(base: str, *parts: str) -> str:
        """Join location segments with '/', which is valid for both URIs and
        POSIX paths, instead of pathlib."""
        return "/".join([base.rstrip("/"), *(part.strip("/") for part in parts)])

    def landing_path(self, source: str) -> str:
        return self.join(self.landing, self.sources[source]["path"])

    def layer_path(self, layer: str, table: str) -> str:
        return self.join(self.warehouse, self.layers[layer]["path"], table)

    def quarantine_path(self, table: str) -> str:
        return self.join(self.warehouse, self.quality["quarantine_path"], table)

    def checkpoint_path(self) -> str:
        return self.join(self.warehouse, "_checkpoints")

    # --- local-only helpers -----------------------------------------------
    def quality_report_path(self) -> Path:
        return self.reports / "_quality"

    def run_manifest_path(self) -> Path:
        return self.reports / "_runs"

    def landing_dir(self) -> Path:
        """The landing zone as a local directory.

        Only valid for local runs; the seed generator is a development tool
        and has no business writing to a bucket.
        """
        if is_uri(self.landing):
            raise ValueError(
                f"landing is a remote location ({self.landing}); "
                "seeding only supports a local filesystem"
            )
        return Path(self.landing)

    def tables(self, layer: str) -> list[str]:
        return list(self.layers[layer]["tables"])


def _resolve_location(value: str) -> str:
    """Absolutise a filesystem path, pass a URI through untouched."""
    if is_uri(value):
        return value
    path = Path(value).expanduser()
    return str(path if path.is_absolute() else (REPO_ROOT / path).resolve())


def _deep_merge(base: dict[str, Any], overlay: dict[str, Any]) -> dict[str, Any]:
    """Recursively overlay one mapping onto another, returning a new dict.

    Scalars and lists replace; nested mappings merge, so an environment file
    only has to state what actually differs.
    """
    merged = dict(base)
    for key, value in overlay.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def _read_yaml(path: Path) -> dict[str, Any]:
    with open(path) as handle:
        return yaml.safe_load(handle) or {}


def load_config(config_path: str | os.PathLike[str] | None = None) -> Config:
    """Read pipeline.yaml, overlay the environment file, then RP_* variables."""
    env = os.getenv("RP_ENV", "local")
    path = Path(config_path) if config_path else DEFAULT_CONFIG_PATH
    raw = _read_yaml(path)

    overlay_path = path.with_name(f"{path.stem}.{env}{path.suffix}")
    if overlay_path.exists():
        raw = _deep_merge(raw, _read_yaml(overlay_path))

    spark = dict(raw.get("spark", {}))
    if shuffle := os.getenv("SPARK_SHUFFLE_PARTITIONS"):
        spark["shuffle_partitions"] = int(shuffle)

    paths = raw.get("paths", {})
    warehouse = _resolve_location(os.getenv("RP_WAREHOUSE_PATH", paths["warehouse"]))

    # Reports default to sitting beside a local warehouse, but must fall back
    # to a local directory when the warehouse is remote.
    reports_default = paths.get("reports")
    if reports_default is None:
        reports_default = warehouse if not is_uri(warehouse) else "data/reports"
    reports = Path(_resolve_location(os.getenv("RP_REPORTS_PATH", reports_default)))

    return Config(
        app_name=os.getenv("RP_APP_NAME", raw["app_name"]),
        landing=_resolve_location(os.getenv("RP_LANDING_PATH", paths["landing"])),
        warehouse=warehouse,
        reports=reports,
        spark=spark,
        seed=raw.get("seed", {}),
        sources=raw.get("sources", {}),
        layers=raw.get("layers", {}),
        quality=raw.get("quality", {}),
        env=env,
        raw=raw,
    )


@lru_cache(maxsize=1)
def load_expectations(path: str | os.PathLike[str] | None = None) -> dict[str, list[dict]]:
    """Load the declarative data-quality contract keyed by `<layer>.<table>`."""
    target = Path(path) if path else DEFAULT_EXPECTATIONS_PATH
    if not target.exists():
        return {}
    return _read_yaml(target)
