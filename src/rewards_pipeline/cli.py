"""Command-line entry point.

Every stage is callable on its own (`rewards silver --date 2026-09-22`) so
the orchestrator can schedule them as separate tasks and retry one without
re-running the others.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date, timedelta

from .config import load_config
from .jobs import bronze_ingest, generate_seed, gold_marts, silver_transform
from .quality import DataQualityError
from .session import configure_logging, get_logger, spark_session

log = get_logger("cli")

STAGES = {
    "bronze": bronze_ingest.run,
    "silver": silver_transform.run,
    "gold": gold_marts.run,
}


def _today() -> str:
    return date.today().isoformat()


def _preview(config, limit: int) -> None:
    from .io import read_table, table_exists

    with spark_session(config, "preview") as spark:
        for table in config.tables("gold"):
            if not table_exists(config, "gold", table):
                log.warning("gold.%s has not been built yet", table)
                continue
            print(f"\n=== gold.{table} ===")
            read_table(spark, config, "gold", table).show(limit, truncate=False)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="rewards",
        description="Card rewards medallion pipeline (bronze -> silver -> gold)",
    )
    parser.add_argument("--config", help="path to pipeline.yaml", default=None)
    parser.add_argument("--log-level", default=None, help="DEBUG, INFO, WARNING, ERROR")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("seed", help="regenerate the synthetic landing zone")

    for stage in STAGES:
        stage_parser = sub.add_parser(stage, help=f"run the {stage} stage")
        stage_parser.add_argument("--date", default=None, help="logical date, YYYY-MM-DD")

    all_parser = sub.add_parser("run-all", help="seed (optional) + bronze + silver + gold")
    all_parser.add_argument("--date", default=None)
    all_parser.add_argument("--seed", action="store_true", help="regenerate landing data first")

    backfill_parser = sub.add_parser(
        "backfill", help="ingest a range of days, then rebuild silver and gold once"
    )
    backfill_parser.add_argument("--days", type=int, default=14, help="how many days back")
    backfill_parser.add_argument("--end", default=None, help="last date, YYYY-MM-DD")
    backfill_parser.add_argument("--seed", action="store_true")

    preview_parser = sub.add_parser("preview", help="print the gold marts")
    preview_parser.add_argument("--limit", type=int, default=10)

    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    configure_logging(args.log_level)
    config = load_config(args.config)
    logical_date = getattr(args, "date", None) or _today()

    try:
        if args.command == "seed":
            generate_seed.generate(config)
            return 0

        if args.command == "preview":
            _preview(config, args.limit)
            return 0

        if args.command == "backfill":
            if args.seed:
                generate_seed.generate(config)
            end = date.fromisoformat(args.end) if args.end else date.today()
            dates = [
                (end - timedelta(days=offset)).isoformat() for offset in reversed(range(args.days))
            ]
            # Bronze is partitioned by ingest date, so each day is ingested on
            # its own; silver and gold recompute from the full bronze history
            # once, which is both cheaper and simpler than replaying them daily.
            for day in dates:
                log.info("backfilling bronze for %s", day)
                bronze_ingest.run(day, config)
            silver = silver_transform.run(dates[-1], config)
            gold = gold_marts.run(dates[-1], config)
            print(
                json.dumps(
                    {"backfilled": dates, "silver": silver, "gold": gold}, indent=2, default=str
                )
            )
            return 0

        if args.command == "run-all":
            if args.seed:
                generate_seed.generate(config)
            summaries = [runner(logical_date, config) for runner in STAGES.values()]
            print(json.dumps(summaries, indent=2, default=str))
            return 0

        summary = STAGES[args.command](logical_date, config)
        print(json.dumps(summary, indent=2, default=str))
        return 0

    except DataQualityError as exc:
        # A contract breach is an expected failure mode, not a crash: exit 2 so
        # the orchestrator can alert on it distinctly from a code error.
        log.error("data quality gate failed: %s", exc)
        return 2


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
