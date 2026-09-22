"""Declared table contracts.

Each warehouse table has one YAML file under `schemas/` stating the columns,
types, grain and ownership it promises. The contract is the source of truth;
everything else is derived from or checked against it:

* `render_ddl` generates the CREATE TABLE for a new table, including column
  comments, so DDL is not hand-typed;
* `check` compares the contract against the schema the pipeline actually
  wrote and reports drift.

That second one matters. With bare Parquet the catalog cannot enforce that the
writer and the DDL agree - nothing fails when a column is renamed, it just
disappears from under the consumers. Checking the contract against reality in
CI is what turns that convention into an enforced rule.

Nullability is deliberately *not* checked against Parquet metadata, which
reports almost everything as nullable regardless of content. `nullable: false`
is carried into the DDL and belongs in `conf/expectations.yaml` as a
`not_null` check, where it is tested against the data rather than the file
footer.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml
from pyspark.sql import SparkSession

from .config import REPO_ROOT, Config
from .session import get_logger

log = get_logger("contracts")

SCHEMA_DIR = REPO_ROOT / "schemas"


class ContractError(RuntimeError):
    """Raised when a contract is malformed or reality has drifted from it."""


@dataclass(frozen=True)
class Column:
    name: str
    type: str
    nullable: bool = True
    description: str | None = None
    pii: bool = False
    accepted_values: list[str] | None = None


@dataclass(frozen=True)
class Contract:
    table: str
    layer: str
    description: str
    owner: str
    grain: list[str]
    columns: list[Column]
    partitioned_by: list[str] = field(default_factory=list)
    path: Path | None = None

    @property
    def qualified(self) -> str:
        return f"{self.layer}.{self.table}"

    def column(self, name: str) -> Column | None:
        return next((column for column in self.columns if column.name == name), None)


def _parse(payload: dict[str, Any], path: Path | None = None) -> Contract:
    missing = {"table", "layer", "columns"} - payload.keys()
    if missing:
        raise ContractError(f"{path or '<dict>'} is missing: {', '.join(sorted(missing))}")

    columns = [Column(**column) for column in payload["columns"]]
    names = [column.name for column in columns]
    duplicates = {name for name in names if names.count(name) > 1}
    if duplicates:
        raise ContractError(f"{path or payload['table']} declares {duplicates} twice")

    grain = payload.get("grain", [])
    unknown_grain = set(grain) - set(names)
    if unknown_grain:
        raise ContractError(
            f"{payload['layer']}.{payload['table']} declares a grain on undeclared "
            f"column(s): {', '.join(sorted(unknown_grain))}"
        )

    unknown_partitions = set(payload.get("partitioned_by", [])) - set(names)
    if unknown_partitions:
        raise ContractError(
            f"{payload['layer']}.{payload['table']} partitions on undeclared "
            f"column(s): {', '.join(sorted(unknown_partitions))}"
        )

    return Contract(
        table=payload["table"],
        layer=payload["layer"],
        description=payload.get("description", "").strip(),
        owner=payload.get("owner", "unowned"),
        grain=grain,
        columns=columns,
        partitioned_by=payload.get("partitioned_by", []),
        path=path,
    )


def load_all(schema_dir: Path | None = None) -> list[Contract]:
    """Load every contract, ordered by layer then table."""
    directory = schema_dir or SCHEMA_DIR
    if not directory.exists():
        return []
    contracts = [
        _parse(yaml.safe_load(path.read_text()), path) for path in sorted(directory.rglob("*.yml"))
    ]
    return sorted(contracts, key=lambda contract: (contract.layer, contract.table))


def load(qualified: str, schema_dir: Path | None = None) -> Contract:
    for contract in load_all(schema_dir):
        if contract.qualified == qualified:
            return contract
    raise ContractError(f"no contract declared for {qualified}")


# --------------------------------------------------------------------------
# DDL generation
# --------------------------------------------------------------------------
def _escape(text: str) -> str:
    """Escape a string literal for Spark SQL.

    Spark does not use ANSI doubled quotes. `'month''s'` does not raise - it
    parses as two adjacent literals and silently yields "months", quietly
    dropping the apostrophe. Backslash is the escape Spark actually honours,
    so backslashes themselves must be doubled first.
    """
    return text.replace("\\", "\\\\").replace("'", "\\'")


def render_ddl(contract: Contract, config: Config, resolved: bool = False) -> str:
    """Generate the CREATE TABLE for a contract.

    By default the database and location stay as `{database}` / `{warehouse}`
    placeholders, because the output is meant to be committed as a migration
    and must target every environment. `resolved=True` substitutes them, which
    is only for reading on screen.

    External, not managed: the pipeline owns the files and the catalog owns
    the contract, so dropping the table never deletes data. The schema is
    stated explicitly rather than inferred, which is the whole point - an
    inferred table would just mirror whatever drift already happened.
    """
    database = config.catalog["database"] if resolved else "{database}"
    prefix = "" if contract.layer == "gold" else f"{contract.layer}_"
    name = f"{database}.{prefix}{contract.table}"

    if resolved:
        location = config.layer_path(contract.layer, contract.table)
    else:
        layer_dir = config.layers[contract.layer]["path"]
        location = "{warehouse}/" + f"{layer_dir}/{contract.table}"

    lines = [f"CREATE TABLE IF NOT EXISTS {name} ("]
    definitions = []
    for column in contract.columns:
        definition = f"    {column.name} {column.type.upper()}"
        if not column.nullable:
            definition += " NOT NULL"
        if column.description:
            definition += f" COMMENT '{_escape(column.description)}'"
        definitions.append(definition)
    lines.append(",\n".join(definitions))
    lines.append(")")
    lines.append("USING parquet")
    if contract.partitioned_by:
        lines.append(f"PARTITIONED BY ({', '.join(contract.partitioned_by)})")
    lines.append(f"LOCATION '{location}'")
    if contract.description:
        lines.append(f"COMMENT '{_escape(contract.description)}'")
    # `owner` is reserved by Spark, hence the prefix. Carrying grain and PII
    # into table properties means the catalog answers "who owns this and what
    # is sensitive" without anyone opening the repo.
    properties = [
        f"'owner_team' = '{_escape(contract.owner)}'",
        f"'grain' = '{_escape(', '.join(contract.grain))}'",
    ]
    pii = [column.name for column in contract.columns if column.pii]
    if pii:
        properties.append(f"'pii_columns' = '{_escape(', '.join(pii))}'")
    lines.append("TBLPROPERTIES (" + ", ".join(properties) + ")")
    return "\n".join(lines)


# --------------------------------------------------------------------------
# Drift detection
# --------------------------------------------------------------------------
@dataclass
class Drift:
    table: str
    missing: list[str]  # declared, but the pipeline did not write it
    undeclared: list[str]  # written, but no contract entry
    type_mismatches: list[tuple[str, str, str]]  # (column, declared, actual)
    checked: bool = True

    @property
    def clean(self) -> bool:
        return not (self.missing or self.undeclared or self.type_mismatches)

    def render(self) -> str:
        if not self.checked:
            return f"[SKIP] {self.table:<32} not written yet"
        if self.clean:
            return f"[OK]   {self.table:<32} matches contract"

        parts = []
        if self.missing:
            parts.append(f"missing {self.missing}")
        if self.undeclared:
            parts.append(f"undeclared {self.undeclared}")
        if self.type_mismatches:
            parts.append(
                "type "
                + ", ".join(
                    f"{name}: declared {declared}, actual {actual}"
                    for name, declared, actual in self.type_mismatches
                )
            )
        return f"[DRIFT] {self.table:<31} " + "; ".join(parts)


def check(
    spark: SparkSession,
    config: Config,
    schema_dir: Path | None = None,
) -> list[Drift]:
    """Compare every contract against the table the pipeline actually wrote."""
    from .io import location_exists

    results = []
    for contract in load_all(schema_dir):
        location = config.layer_path(contract.layer, contract.table)
        if not location_exists(spark, location):
            results.append(Drift(contract.qualified, [], [], [], checked=False))
            continue

        actual = {
            field_.name: field_.dataType.simpleString()
            for field_ in spark.read.parquet(location).schema.fields
        }
        declared = {column.name: column.type.lower() for column in contract.columns}

        results.append(
            Drift(
                table=contract.qualified,
                missing=[name for name in declared if name not in actual],
                undeclared=[name for name in actual if name not in declared],
                type_mismatches=[
                    (name, declared[name], actual[name])
                    for name in declared
                    if name in actual and declared[name] != actual[name]
                ],
            )
        )
    return results
