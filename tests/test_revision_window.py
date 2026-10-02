"""The refreshable-window rule.

Getting this boundary wrong is silent in both directions: too narrow and
revisions are lost, too wide and a final partition is rebuilt with sealed state
that postdates it, leaking the future into its all_time features. So the rule is
pinned here rather than left to whatever runs the models.
"""

from __future__ import annotations

from datetime import date

from generator.revision import refreshable_dates

LATE = 3  # settings.late_arrival_days in the showcase specs


def test_in_order_run_refreshes_the_whole_window():
    """W = T - late is the ordinary case, and the full window is refreshable."""
    dates = refreshable_dates(date(2026, 9, 3), LATE, date(2026, 8, 31))
    assert dates == [date(2026, 9, 2), date(2026, 9, 1), date(2026, 8, 31)]


def test_dates_the_accumulator_has_sealed_past_are_final():
    """A partition older than the watermark is final; rebuilding it would leak."""
    dates = refreshable_dates(date(2026, 9, 3), LATE, date(2026, 9, 2))
    assert dates == [date(2026, 9, 2)]


def test_window_can_be_empty():
    """Nothing is refreshable when the watermark has passed the whole window."""
    assert refreshable_dates(date(2026, 9, 3), LATE, date(2026, 9, 3)) == []


def test_no_date_is_ever_behind_the_watermark():
    """The property that matters, over a sweep of watermark positions."""
    target = date(2026, 9, 10)
    for offset in range(0, 8):
        wm = date(2026, 9, 10 - offset)
        for d in refreshable_dates(target, LATE, wm):
            assert d >= wm, f"{d} is behind watermark {wm}"
            assert d < target, f"{d} is not in the past of {target}"


def test_window_never_exceeds_late_arrival_days():
    """A watermark far in the past must not widen the window."""
    dates = refreshable_dates(date(2026, 9, 3), LATE, date(2026, 1, 1))
    assert len(dates) == LATE
    assert min(dates) == date(2026, 8, 31)


def test_missing_accumulator_means_nothing_is_sealed():
    """A first run has sealed nothing, so the whole window is provisional."""
    assert len(refreshable_dates(date(2026, 9, 3), LATE, None)) == LATE


def test_a_spec_without_late_arrival_has_nothing_to_refresh():
    """late_arrival_days = 0 means a partition is final the day it is built."""
    assert refreshable_dates(date(2026, 9, 3), 0, None) == []


def test_a_refresh_never_creates_history_before_the_first_partition():
    """The day after an initial load at 2026-08-20, only that load is refreshable."""
    dates = refreshable_dates(date(2026, 8, 21), LATE, date(2026, 8, 17), date(2026, 8, 20))
    assert dates == [date(2026, 8, 20)]


def test_an_established_mart_is_not_narrowed_by_its_first_partition():
    dates = refreshable_dates(date(2026, 9, 3), LATE, date(2026, 8, 31), date(2026, 8, 20))
    assert dates == [date(2026, 9, 2), date(2026, 9, 1), date(2026, 8, 31)]
