"""Tests that need no JVM: config resolution and the seed generator."""

from __future__ import annotations

import json
from pathlib import Path

from rewards_pipeline.config import load_config, load_expectations
from rewards_pipeline.jobs.generate_seed import generate


def test_config_paths_resolve_to_absolute(config):
    assert Path(config.landing).is_absolute()
    assert config.layer_path("gold", "card_roi_monthly").name == "card_roi_monthly"


def test_expectations_cover_every_gold_table():
    expectations = load_expectations()
    config = load_config()
    declared = {key.split(".", 1)[1] for key in expectations if key.startswith("gold.")}
    assert declared <= set(config.tables("gold"))
    assert "transaction_rewards" in declared


def test_seed_is_deterministic(config, tmp_path):
    config.seed.update({"days": 2, "transactions_per_day": 50, "members": 5, "cards": 3})
    generate(config)
    first = sorted(p.read_text() for p in config.landing.rglob("*.json"))
    generate(config)
    second = sorted(p.read_text() for p in config.landing.rglob("*.json"))
    assert first == second


def test_seed_writes_every_source(config):
    config.seed.update({"days": 2, "transactions_per_day": 30, "members": 5, "cards": 3})
    landing = generate(config)
    for source in ("transactions", "cards", "merchants", "reward_rules", "fx_rates"):
        assert (landing / source).exists(), f"missing landing source: {source}"


def test_seed_injects_the_dirt_the_pipeline_must_survive(config):
    config.seed.update({"days": 3, "transactions_per_day": 400, "members": 20, "cards": 4})
    landing = generate(config)
    lines = []
    for path in (landing / "transactions").rglob("*.json"):
        lines.extend(path.read_text().splitlines())

    parsed, broken = [], 0
    for line in lines:
        try:
            parsed.append(json.loads(line))
        except json.JSONDecodeError:
            broken += 1

    assert broken > 0, "expected unparseable rows in the landing zone"
    assert any(record["amount"] is None for record in parsed)
    assert len(parsed) > len({record["txn_id"] for record in parsed}), "expected duplicates"


def test_card_snapshots_contain_a_change_for_scd2(config):
    config.seed.update({"days": 6, "transactions_per_day": 10, "members": 5, "cards": 8})
    landing = generate(config)
    rows = (landing / "cards" / "cards.csv").read_text().splitlines()[1:]
    fees = {}
    for row in rows:
        fields = row.split(",")
        fees.setdefault(fields[0], set()).add(fields[4])
    assert any(len(values) > 1 for values in fees.values()), "no SCD2 change to capture"
