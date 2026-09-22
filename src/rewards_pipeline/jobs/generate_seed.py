"""Synthetic landing-zone generator.

Deterministic (seeded) so every run of the demo produces the same numbers, and
deliberately dirty: duplicate transaction ids, malformed JSON lines, missing
amounts, unknown currencies and mid-window card attribute changes. Without that
dirt the silver layer and the quality gate would have nothing to prove.

Pure Python on purpose — `make seed` works before Spark or Java is involved.
"""

from __future__ import annotations

import csv
import json
import random
import shutil
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Any

from ..config import Config, load_config
from ..session import configure_logging, get_logger

log = get_logger("seed")

CATEGORIES = [
    ("travel", "3000"),
    ("dining", "5812"),
    ("groceries", "5411"),
    ("gas", "5541"),
    ("streaming", "5815"),
    ("transit", "4111"),
    ("drugstore", "5912"),
    ("online_retail", "5999"),
    ("hotel", "7011"),
    ("other", "5399"),
]
NETWORKS = ["visa", "mastercard", "amex"]
ISSUERS = ["Northwind Bank", "Cascade Financial", "Harbor Trust", "Meridian Card Co"]
PRODUCT_WORDS = ["Sapphire", "Platinum", "Venture", "Gold", "Cash", "Summit", "Atlas", "Horizon"]
CURRENCIES = {"USD": 1.0, "EUR": 1.09, "GBP": 1.27, "CAD": 0.74, "JPY": 0.0067}
CHANNELS = ["in_store", "online", "recurring", "atm"]


def _write_csv(path: Path, rows: list[dict], header: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=header)
        writer.writeheader()
        writer.writerows(rows)
    log.info("wrote %d rows -> %s", len(rows), path)


def _write_jsonl(path: Path, rows: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(rows) + "\n")
    log.info("wrote %d lines -> %s", len(rows), path)


def generate(config: Config | None = None, *, clean: bool = True) -> Path:
    config = config or load_config()
    seed_cfg = config.seed
    rng = random.Random(seed_cfg.get("random_seed", 42))
    landing = config.landing

    if clean and landing.exists():
        shutil.rmtree(landing)
    landing.mkdir(parents=True, exist_ok=True)
    # The landing zone is git-tracked but empty; keep its placeholder so a
    # seed run does not leave the working tree dirty.
    (landing / ".gitkeep").touch()

    days = int(seed_cfg.get("days", 14))
    end_date = date.today()
    start_date = end_date - timedelta(days=days - 1)
    dates = [start_date + timedelta(days=offset) for offset in range(days)]

    # --- cards: daily snapshots, with a repricing event mid-window ---------
    card_ids = [f"CARD{index:03d}" for index in range(1, int(seed_cfg.get("cards", 12)) + 1)]
    base_cards: dict[str, dict[str, Any]] = {}
    for card_id in card_ids:
        base_cards[card_id] = {
            "card_id": card_id,
            "product_name": (
                f"{rng.choice(PRODUCT_WORDS)} "
                f"{rng.choice(['Preferred', 'Reserve', 'Everyday'])}"
            ),
            "issuer": rng.choice(ISSUERS),
            "network": rng.choice(NETWORKS),
            "annual_fee_usd": rng.choice([0.0, 95.0, 150.0, 250.0, 550.0, 695.0]),
            "base_earn_rate": rng.choice([1.0, 1.25, 1.5, 2.0]),
            "point_value_cents": round(rng.uniform(0.9, 2.1), 2),
        }

    repricing_day = dates[len(dates) // 2]
    repriced = rng.sample(card_ids, max(2, len(card_ids) // 4))
    card_rows = []
    for snapshot in dates:
        for card_id in card_ids:
            row = dict(base_cards[card_id])
            if card_id in repriced and snapshot >= repricing_day:
                # A fee/earn-rate change is exactly what SCD2 has to capture.
                row["annual_fee_usd"] = round(float(row["annual_fee_usd"]) * 1.2, 2)
                row["base_earn_rate"] = round(float(row["base_earn_rate"]) + 0.25, 2)
            row["snapshot_date"] = snapshot.isoformat()
            card_rows.append(row)
    _write_csv(
        landing / "cards" / "cards.csv",
        card_rows,
        [
            "card_id",
            "product_name",
            "issuer",
            "network",
            "annual_fee_usd",
            "base_earn_rate",
            "point_value_cents",
            "snapshot_date",
        ],
    )

    # --- merchants --------------------------------------------------------
    merchant_rows = []
    for index in range(1, int(seed_cfg.get("merchants", 150)) + 1):
        category, mcc = rng.choice(CATEGORIES)
        merchant_rows.append(
            {
                "merchant_id": f"MRC{index:04d}",
                "merchant_name": (
                    f"{rng.choice(['Blue', 'Iron', 'Pine', 'Vertex', 'Nova', 'Rally'])} "
                    f"{rng.choice(['Market', 'Cafe', 'Travel', 'Fuel', 'Supply', 'Media'])} "
                    f"{index}"
                ),
                "mcc": mcc,
                "category": category,
                "country": rng.choices(["US", "GB", "DE", "CA", "JP"], weights=[70, 8, 8, 9, 5])[0],
            }
        )
    _write_csv(
        landing / "merchants" / "merchants.csv",
        merchant_rows,
        ["merchant_id", "merchant_name", "mcc", "category", "country"],
    )

    # --- reward rules -----------------------------------------------------
    rule_lines = []
    rule_index = 0
    for card_id in card_ids:
        for category, _ in rng.sample(CATEGORIES, rng.randint(2, 4)):
            rule_index += 1
            rule_lines.append(
                json.dumps(
                    {
                        "rule_id": f"RULE{rule_index:04d}",
                        "card_id": card_id,
                        "category": category,
                        "multiplier": rng.choice([2.0, 3.0, 4.0, 5.0]),
                        "monthly_cap_usd": rng.choice([None, 500.0, 1500.0, 6000.0]),
                        "effective_from": (start_date - timedelta(days=90)).isoformat(),
                        "effective_to": None,
                    }
                )
            )
    _write_jsonl(landing / "reward_rules" / "reward_rules.json", rule_lines)

    # --- fx rates ---------------------------------------------------------
    fx_rows = []
    for rate_date in dates:
        for currency, base in CURRENCIES.items():
            drift = 1.0 if currency == "USD" else rng.uniform(0.985, 1.015)
            fx_rows.append(
                {
                    "rate_date": rate_date.isoformat(),
                    "currency": currency,
                    "usd_rate": round(base * drift, 6),
                }
            )
    _write_csv(
        landing / "fx_rates" / "fx_rates.csv",
        fx_rows,
        ["rate_date", "currency", "usd_rate"],
    )

    # --- transactions: one partition per ingest date -----------------------
    member_ids = [f"MBR{index:05d}" for index in range(1, int(seed_cfg.get("members", 400)) + 1)]
    per_day = int(seed_cfg.get("transactions_per_day", 2500))
    merchant_ids = [row["merchant_id"] for row in merchant_rows]
    mcc_by_merchant = {row["merchant_id"]: row["mcc"] for row in merchant_rows}
    total = 0

    for txn_date in dates:
        lines: list[str] = []
        for index in range(per_day):
            merchant_id = rng.choice(merchant_ids)
            currency = rng.choices(list(CURRENCIES), weights=[85, 4, 4, 4, 3])[0]
            is_refund = rng.random() < 0.04
            amount = round(rng.lognormvariate(3.2, 0.9), 2)
            stamp = datetime.combine(
                txn_date,
                time(rng.randint(0, 23), rng.randint(0, 59), rng.randint(0, 59)),
            )
            record = {
                "txn_id": f"TXN-{txn_date.isoformat()}-{index:06d}",
                "member_id": rng.choice(member_ids),
                "card_id": rng.choice(card_ids),
                "merchant_id": merchant_id,
                "mcc": mcc_by_merchant[merchant_id],
                "amount": amount,
                "currency": currency,
                "txn_ts": stamp.isoformat(timespec="seconds"),
                "status": "refunded" if is_refund else "settled",
                "is_refund": is_refund,
                "channel": rng.choice(CHANNELS),
                "source_system": rng.choice(["core_auth", "batch_settle"]),
            }

            # Deliberate dirt: ~0.6% of rows arrive without an amount.
            if rng.random() < 0.006:
                record["amount"] = None
            # ~0.3% carry a currency the FX feed does not publish.
            if rng.random() < 0.003:
                record["currency"] = "XBT"
            lines.append(json.dumps(record))

            # ~0.5% are re-delivered by the source system as exact duplicates.
            if rng.random() < 0.005:
                lines.append(json.dumps(record))

        # A handful of unparseable lines per file, as any real feed produces.
        for _ in range(3):
            lines.insert(rng.randint(0, len(lines)), '{"txn_id": "TXN-BROKEN", "amount": }')

        partition = landing / "transactions" / f"ingest_date={txn_date.isoformat()}"
        _write_jsonl(partition / f"transactions-{txn_date.isoformat()}.json", lines)
        total += len(lines)

    log.info("seed complete: %d transaction lines across %d day(s)", total, days)
    log.info("landing zone: %s", landing)
    return landing


def main() -> None:
    configure_logging()
    generate()


if __name__ == "__main__":
    main()
