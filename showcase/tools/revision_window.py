"""Resolve the refreshable window against this deployment, and print the dates.

The *rule* -- which past partitions may still be rebuilt, and why a date behind
the accumulator's watermark may not be -- lives in `generator.revision`, because
it is a property of what the generated models mean rather than of how this stack
runs them. What lives here is the deployment-specific half of it:

    late_arrival_days   read from the committed registry for the feature
    watermark W         read from the accumulator table in the warehouse
    first served        the oldest partition the mart holds, also from the warehouse

Emitting the dates here, rather than computing them in the DAG, keeps one
resolver behind both `make dbt-revise` and the DAG's refresh task, so the dates
a run refreshes and the dates it publishes cannot drift apart.
"""

from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path

import duckdb
from generator.registry import Registry

from tools.paths import DB, REGISTRY_DIR


def read_watermark(state_model: str, db: Path) -> date | None:
    """The accumulator's `_state_as_of_date`, or None if it has never been built."""
    con = duckdb.connect(str(db), read_only=True)
    try:
        return con.execute(
            f"select max(_state_as_of_date) from intermediate.{state_model}"
        ).fetchone()[0]
    except duckdb.Error:
        # No accumulator yet: nothing has been sealed, so nothing is final.
        return None
    finally:
        con.close()


def read_first_served(mart: str, db: Path) -> date | None:
    """The oldest as-of date the mart holds, or None if it has never been built."""
    con = duckdb.connect(str(db), read_only=True)
    try:
        return con.execute(f"select min(target_date) from marts.{mart}").fetchone()[0]
    except duckdb.Error:
        return None
    finally:
        con.close()


def refreshable_dates(feature_name: str, target: date, db: Path) -> list[date]:
    registry = Registry.load(REGISTRY_DIR / f"{feature_name}.json")
    state_model = registry.models.alltime_state
    watermark = read_watermark(state_model, db) if state_model else None
    first_served = read_first_served(registry.models.mart, db)
    return registry.refreshable_dates(target, watermark, first_served)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("feature_name")
    ap.add_argument("target_date")
    ap.add_argument("--db", type=Path, default=DB)
    ap.add_argument(
        "--include-target",
        action="store_true",
        help="also emit the target date itself (for the publish step)",
    )
    a = ap.parse_args()

    target = date.fromisoformat(a.target_date)
    dates = refreshable_dates(a.feature_name, target, a.db)
    if a.include_target:
        dates = [target, *dates]
    print(" ".join(d.isoformat() for d in dates))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
