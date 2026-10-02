"""Independent end-to-end verification of the feature mart.

The generated dbt tests check that the pipeline is self-consistent. They cannot
catch a systematic error -- an off-by-one window bound, a merge that drops the
unsealed tail, a days_since derivation with a flipped sign -- because every
layer would agree on the same wrong answer.

So this recomputes a representative sample of features the naive way: one flat
query straight over the raw source at the as-of date, with no partials, no
sealing, no incrementality. If the incremental machinery is right, the two must
agree exactly, for every entity.

WHICH "TRUTH" IS THE RIGHT ONE. There are two defensible readings of a feature
dated T, and they differ precisely on what arrives or changes after T:

  as-of-knowledge  only what was knowable when T ended. A frozen snapshot of
                   that evening.
  as-of-event      every event that HAPPENED on or before T, as the source now
                   records it, whenever it was loaded, corrected or deleted.

A partition implements as-of-event up to a horizon: it is rebuilt by the runs
at T+1 .. T+late_arrival_days, each knowing more than the last, and is final
afterwards. A login on Monday is a Monday login even if it reached the
warehouse on Wednesday, and one corrected on Wednesday is counted as corrected.

So the comparison takes events dated on or before T, in the row versions that
were current when the horizon day ended. tools/oracle.py defines both, and why
the horizon is min(T + late_arrival_days, newest as-of date built).

A partition inside its revision window is therefore provisional by design, and
comparing it against knowledge it has not been offered yet would be testing the
wrong contract.

CLOCK. Dates and hours are read on the spec's business clock, and a day ends at
midnight on that clock.

ENTITY SPINE. Which entities a partition is supposed to contain is also part of
the contract, and it is set per spec:

  all_time       every entity ever seen gets a row on every as-of date.
  active_window  only entities with activity inside the widest bounded window.

Under active_window a dormant entity is ABSENT by design, so the comparison
restricts itself to the entities the spine promises. It still checks that the
mart contains exactly those and no others -- an entity missing from an
active_window partition that should be there is as much a defect as a wrong
number.
"""

from __future__ import annotations

import sys
from datetime import date, timedelta

import duckdb
from generator.registry import Registry

from tools import oracle
from tools.paths import DB, REGISTRY_DIR

TARGET = sys.argv[1] if len(sys.argv) > 1 else "2026-09-03"
FEATURE = "fact_agg_features_login_history_v2"  # the spec the probes below are written against

IOS = "upper(os_name) = 'IOS'"
ANDROID = "upper(os_name) = 'ANDROID'"
OTHERS = "upper(os_name) not in ('IOS', 'ANDROID')"
SUCCESS = "upper(event_status) = 'SUCCESS'"
FAILED = "upper(event_status) = 'FAILED'"
LATE_NIGHT = "extract(hour from event_timestamp) between 0 and 4"
OFFICE = "extract(hour from event_timestamp) between 9 and 17"


def w(days: int) -> str:
    """Inclusive window: [T - (days-1), T]."""
    return f"event_date >= date '{TARGET}' - {days - 1}"


# Every distinct code path in the generator gets at least one probe:
# marginal and combined conditions, the widest window (which omits its
# predicate), the state-merge path for all_time, exact set union, both
# extremum merges, and both directions of the days_since derivation.
PROBES: dict[str, str] = {
    "count_event_id_l7d": f"count(case when {w(7)} then event_id end)",
    "count_event_id_l30d": f"count(case when {w(30)} then event_id end)",
    "count_event_id_all_time": "count(event_id)",
    "count_event_id_is_ios_is_late_night_l7d": f"count(case when {w(7)} and {IOS} and {LATE_NIGHT} then event_id end)",
    "count_event_id_is_login_success_is_android_is_office_hours_l14d": f"count(case when {w(14)} and {SUCCESS} and {ANDROID} and {OFFICE} then event_id end)",
    "count_event_id_is_others_l30d": f"count(case when {w(30)} and {OTHERS} then event_id end)",
    "count_event_id_is_login_failed_all_time": f"count(case when {FAILED} then event_id end)",
    "count_distinct_device_id_l7d": f"count(distinct case when {w(7)} then device_id end)",
    "count_distinct_device_id_all_time": "count(distinct device_id)",
    "count_distinct_device_id_is_ios_l30d": f"count(distinct case when {w(30)} and {IOS} then device_id end)",
    "min_event_timestamp_all_time": "min(event_timestamp)",
    "max_event_timestamp_l7d": f"max(case when {w(7)} then event_timestamp end)",
    "max_event_timestamp_is_login_failed_all_time": f"max(case when {FAILED} then event_timestamp end)",
    "min_days_since_login_l7d": f"date_diff('day', cast(max(case when {w(7)} then event_timestamp end) as date),"
    f" date '{TARGET}')",
    "max_days_since_login_all_time": f"date_diff('day', cast(min(event_timestamp) as date), date '{TARGET}')",
    "min_days_since_login_is_ios_l30d": f"date_diff('day', cast(max(case when {w(30)} and {IOS} then event_timestamp end) as date),"
    f" date '{TARGET}')",
}


def spine_predicate(registry: Registry) -> str:
    """The rows the spine promises, as a HAVING clause over the brute-force groups.

    The window arithmetic is restated here rather than borrowed from the
    compiler on purpose: an oracle that shares the code it checks would agree
    with it on the same wrong answer.
    """
    if registry.entity_spine == "all_time":
        return "true"
    widest = registry.widest_window_days
    inclusive = registry.window_convention == "inclusive"
    lo = widest - 1 if inclusive else widest
    hi = 0 if inclusive else 1
    return (
        f"max(case when event_date >= date '{TARGET}' - {lo} "
        f"and event_date <= date '{TARGET}' - {hi} then 1 else 0 end) = 1"
    )


def main() -> int:
    registry = Registry.load(REGISTRY_DIR / f"{FEATURE}.json")
    mart = f"marts.{registry.models.mart}"
    con = duckdb.connect(str(DB), read_only=True)

    # The newest knowledge this partition was ever offered. See tools.oracle.
    horizon = oracle.horizon(con, registry, TARGET)
    settled = date.fromisoformat(TARGET) + timedelta(days=registry.late_arrival_days)

    # Deliberately flat: raw source, one filter, one GROUP BY. No reuse of
    # anything the pipeline builds.
    brute = f"""
        with src as ({oracle.events_as_of(registry, TARGET, horizon)})
        select safe_id, {", ".join(f"{sql} as {name}" for name, sql in PROBES.items())}
        from src group by safe_id
        having {spine_predicate(registry)}
    """
    cols = ", ".join(PROBES)
    actual = f"""
        select safe_id, {cols}
        from {mart}
        where target_date = date '{TARGET}'
    """

    comparisons = " or ".join(f"b.{n} is distinct from a.{n}" for n in PROBES)
    diff = con.execute(f"""
        with b as ({brute}), a as ({actual})
        select coalesce(b.safe_id, a.safe_id) as safe_id,
               {", ".join(f"b.{n} as exp_{n}, a.{n} as got_{n}" for n in PROBES)}
        from b full outer join a on b.safe_id = a.safe_id
        where {comparisons} or b.safe_id is null or a.safe_id is null
    """).fetchall()

    n_entities = con.execute(f"select count(*) from ({actual})").fetchone()[0]
    n_brute = con.execute(f"select count(*) from ({brute})").fetchone()[0]

    print(f"as-of date           : {TARGET}")
    print(f"entity spine         : {registry.entity_spine}")
    print(
        f"ingestion horizon    : {horizon}"
        f"{'  (still inside its revision window)' if horizon < settled else '  (settled)'}"
    )
    print(f"entities in mart     : {n_entities}")
    print(f"entities brute-force : {n_brute}")
    print(f"features probed      : {len(PROBES)}")
    print(f"values compared      : {n_entities * len(PROBES)}")

    if diff:
        print(f"\nMISMATCH on {len(diff)} entities:")
        names = list(PROBES)
        for row in diff[:5]:
            print(f"  safe_id={row[0]}")
            for i, name in enumerate(names):
                exp, got = row[1 + 2 * i], row[2 + 2 * i]
                if exp != got:
                    print(f"    {name}: expected={exp!r} got={got!r}")
        return 1

    print("\nEXACT MATCH on every probed feature, for every entity.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
