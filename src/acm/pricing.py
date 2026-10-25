"""The price book (config/pricing.yaml): effective-dated list prices, as tables the warehouse joins.

Both the pipeline and the simulator's ground truth read prices from here, because prices are a
business input, not something either of them computes.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
CONFIG = ROOT / "config"
FAR_FUTURE = 4102444800  # 2100-01-01


def ts(value: str) -> int:
    """ISO-8601 UTC to epoch seconds."""
    return int(datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC).timestamp())


def iso(epoch: int) -> str:
    return datetime.fromtimestamp(epoch, tz=UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


@dataclass(frozen=True)
class Price:
    sku: str          # gpu:h100:eu-north1 | inference:llama-70b:input | storage:object
    valid_from: int   # epoch seconds, inclusive
    valid_to: int     # exclusive
    usd: float        # per GPU-hour, per million tokens, or per GB-month


def load(path: Path | None = None) -> dict:
    with open(path or CONFIG / "pricing.yaml") as f:
        return yaml.safe_load(f)


def _ranges(entries: list[dict], key: str) -> list[tuple[int, int, float]]:
    out = []
    for i, e in enumerate(entries):
        start = ts(e["from"])
        end = ts(entries[i + 1]["from"]) if i + 1 < len(entries) else FAR_FUTURE
        if end <= start:
            raise ValueError(f"price entries out of order at {e['from']}")
        out.append((start, end, float(e[key])))
    return out


def price_rows(book: dict | None = None) -> list[Price]:
    book = book or load()
    rows: list[Price] = []
    for gpu, regions in book["gpu"].items():
        for region, entries in regions.items():
            rows += [Price(f"gpu:{gpu}:{region}", a, b, p) for a, b, p in _ranges(entries, "price")]
    for model, entries in book["inference"].items():
        rows += [Price(f"inference:{model}:input", a, b, p) for a, b, p in _ranges(entries, "input")]
        rows += [Price(f"inference:{model}:output", a, b, p) for a, b, p in _ranges(entries, "output")]
    for kind, entries in book["storage"].items():
        rows += [Price(f"storage:{kind}", a, b, p) for a, b, p in _ranges(entries, "price")]
    return rows


def lookup(rows: list[Price], sku: str, at: int) -> Price | None:
    for p in rows:
        if p.sku == sku and p.valid_from <= at < p.valid_to:
            return p
    return None
