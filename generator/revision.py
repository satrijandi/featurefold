"""The refreshable-window rule.

A mart partition is not final the day it is built. The partial layer keeps
absorbing late-arriving events for `late_arrival_days`, so the marts for those
days have stale inputs until that window closes. Each run therefore rebuilds
the recent window as well as its own date.

But the window is not simply `T-1 .. T-late_arrival_days`. It is bounded below
by the all_time accumulator's watermark W, and that bound is a consequence of
how sealing works rather than a safety margin:

    d >= W    the accumulator has not yet sealed past this date, so its
              all_time state is still valid for d and the partition is
              genuinely provisional -- rebuild it.

    d <  W    the accumulator sealed past d already, which means d's own
              late-arrival window closed before the seal. The partition is
              FINAL. Rebuilding it now would fold sealed state containing
              events after d into d's all_time features, i.e. leak the future.

So the refreshable window is [max(W, T - late_arrival_days), T - 1]. The two
bounds coincide on an ordinary in-order run, where W = T - late_arrival_days;
they diverge after a replay or a run that had nothing to fold, and this is what
keeps that case from turning into either lost revisions or leaked features.

A refresh revises history; it never creates it. The day after an initial load
at T0, the window reaches back past T0 to dates that were never served, and
building them there would publish partitions nobody asked for, each one a
snapshot of a date the deployment chose not to start from. So the window is
also bounded by the oldest partition the mart already holds.

The rule lives in the generator rather than in an operational script because it
is a property of what the generated models mean, not of how any one deployment
runs them. Resolving `late_arrival_days` from a registry and W from a warehouse
is the caller's job; see ../showcase/tools/revision_window.py.
"""

from __future__ import annotations

from datetime import date, timedelta


def refreshable_dates(
    target: date,
    late_arrival_days: int,
    watermark: date | None = None,
    first_served: date | None = None,
) -> list[date]:
    """As-of dates strictly before `target` whose partitions may still be rebuilt.

    `watermark` is the all_time accumulator's `_state_as_of_date`, or None when
    no accumulator exists yet -- a first run has sealed nothing, so nothing is
    final. `first_served` is the oldest as-of date the mart holds, or None when
    that is not known; nothing older is listed. Dates are returned newest first,
    which is the order a refresh should walk them in: the newest partition is
    the one most likely to be read next.
    """
    if late_arrival_days <= 0:
        return []

    oldest = target - timedelta(days=late_arrival_days)
    if watermark is not None:
        oldest = max(oldest, watermark)
    if first_served is not None:
        oldest = max(oldest, first_served)

    return [
        d
        for d in (target - timedelta(days=i) for i in range(1, late_arrival_days + 1))
        if d >= oldest
    ]


def reset_accumulator_command(state_model: str, as_of: str) -> str:
    """The one-line remedy when the accumulator's watermark is ahead of a date to serve.

    A full refresh at `as_of` rebuilds the sealed fold from scratch, so its
    watermark lands at `as_of - late_arrival_days` and every date from there on
    can be served again.
    """
    return f"dbt run --full-refresh --select {state_model} --vars 'target_date: {as_of}'"
