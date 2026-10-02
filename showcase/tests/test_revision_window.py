"""The resolver that binds the refreshable-window rule to this deployment.

The rule itself is unit-tested in the core repo (`tests/test_revision_window.py`).
What is worth testing here is the wiring, because both halves of it are silent
when wrong: reading `late_arrival_days` from the wrong place gives a plausible
window of the wrong width, and failing to see the accumulator's watermark gives
one that reaches back into final partitions.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb
import pytest
from generator.registry import Registry

from tools.revision_window import REGISTRY_DIR, refreshable_dates

FEATURE = "fact_agg_features_login_history_v2"
LATE = 3  # settings.late_arrival_days in the spec


@pytest.fixture
def db(tmp_path: Path):
    """A stand-in accumulator whose watermark the test controls."""

    seq = [0]

    def _make(watermark: date | None, first_served: date | None = None) -> Path:
        # A fresh file per call, so a sweep over watermark positions does not
        # accumulate state between iterations.
        seq[0] += 1
        path = tmp_path / f"w{seq[0]}.duckdb"
        con = duckdb.connect(str(path))
        con.execute("create schema if not exists intermediate")
        con.execute(
            f"create table intermediate.int_{FEATURE}__alltime_state "
            "(safe_id varchar, _state_as_of_date date)"
        )
        if watermark is not None:
            con.execute(
                f"insert into intermediate.int_{FEATURE}__alltime_state values ('x', ?)",
                [watermark],
            )
        if first_served is not None:
            con.execute("create schema if not exists marts")
            con.execute(
                f"create table marts.{FEATURE} as select ?::date as target_date", [first_served]
            )
        con.close()
        return path

    return _make


def test_late_arrival_days_comes_from_the_committed_registry(db):
    """The window's width is the spec's, not a constant duplicated here."""
    registry = Registry.load(REGISTRY_DIR / f"{FEATURE}.json")
    assert registry.late_arrival_days == LATE
    assert len(refreshable_dates(FEATURE, date(2026, 9, 3), db(None))) == LATE


def test_the_warehouse_watermark_bounds_the_window(db):
    """A partition the accumulator has sealed past is final and must not be listed."""
    dates = refreshable_dates(FEATURE, date(2026, 9, 3), db(date(2026, 9, 2)))
    assert dates == [date(2026, 9, 2)]


def test_in_order_run_refreshes_the_whole_window(db):
    """W = T - late is the ordinary case, and the full window is refreshable."""
    dates = refreshable_dates(FEATURE, date(2026, 9, 3), db(date(2026, 8, 31)))
    assert dates == [date(2026, 9, 2), date(2026, 9, 1), date(2026, 8, 31)]


def test_an_unbuilt_accumulator_is_not_an_error(db, tmp_path):
    """A first run has no accumulator table at all; nothing is sealed."""
    empty = tmp_path / "empty.duckdb"
    duckdb.connect(str(empty)).close()
    assert len(refreshable_dates(FEATURE, date(2026, 9, 3), empty)) == LATE


def test_the_oldest_served_partition_bounds_the_window(db):
    """The day after an initial load, the window must not build dates before it."""
    dates = refreshable_dates(FEATURE, date(2026, 8, 21), db(date(2026, 8, 17), date(2026, 8, 20)))
    assert dates == [date(2026, 8, 20)]
