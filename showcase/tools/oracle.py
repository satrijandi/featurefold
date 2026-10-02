"""The independent oracle's reading of the source, shared by every verifier.

Each verifier recomputes features the naive way, straight over the raw source,
so it can catch an error the pipeline would agree with itself about. They all
need the same two answers first, and restating them in each file would let the
verifiers drift apart from one another instead of from the pipeline:

  horizon   the newest knowledge a partition dated T was ever offered:
            min(T + late_arrival_days, newest as-of date built). It is rebuilt
            by the runs up to T + late_arrival_days and final afterwards, and no
            run has looked past the newest date built.

  events    the source rows a partition dated T must reflect: events on or
            before T on the business clock, in the row versions that were
            current when the horizon day ended -- loaded before that instant
            and not superseded by it.

None of this borrows from the compiler or the generated SQL. The time zone
arithmetic uses DuckDB's own functions; only the configuration (which zone,
which columns, how many days) comes from the registry.
"""

from __future__ import annotations

from datetime import date, timedelta

import duckdb
from generator.registry import Registry

SOURCE_TABLE = "bronze_events.customer_login"


def horizon(con: duckdb.DuckDBPyConnection, registry: Registry, target: str) -> date:
    mart = f"marts.{registry.models.mart}"
    (frontier,) = con.execute(f"select max(target_date) from {mart}").fetchone()
    settled = date.fromisoformat(target) + timedelta(days=registry.late_arrival_days)
    return min(settled, frontier)


def events_as_of(registry: Registry, target: str, horizon_date: date) -> str:
    """SQL for the rows partition `target` must reflect, on the business clock."""
    src = registry.source
    tz = registry.timezone

    def to_utc(col: str) -> str:
        return f"timezone('UTC', timezone('{src.timezone}', {col}))"

    superseded = to_utc(src.superseded_at) if src.superseded_at else "null"
    # The UTC instant the horizon day ends on the business clock.
    cutoff = f"timezone('UTC', timezone('{tz}', cast(date '{horizon_date}' + 1 as timestamp)))"
    return f"""
        select *, cast(event_timestamp as date) as event_date
        from (
            select
                customer_id as safe_id,
                device_id, event_id, os_name, event_status, login_source,
                timezone('{tz}', timezone('{src.timezone}', event_timestamp)) as event_timestamp,
                {to_utc(src.loaded_at)} as loaded_utc,
                {superseded} as superseded_utc
            from {SOURCE_TABLE}
            where customer_id is not null
        ) as versions
        where cast(event_timestamp as date) <= date '{target}'     -- as-of-event
          and loaded_utc < {cutoff}                                 -- known by the horizon
          and (superseded_utc is null or superseded_utc >= {cutoff}) -- and still current
    """
