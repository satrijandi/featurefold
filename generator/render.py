"""Render a FeaturePlan into committed dbt models.

The generated SQL is meant to be read in a pull request. That drives three
choices: real column names rather than loops, one section comment per atomic
field, and dialect differences pushed into dispatch macros so the model bodies
stay plain SQL.

Model graph produced per spec, above the shared per-source staging model that
generator.project emits:

    stg_<source>__<table>             view         1:1 with the source (shared)
      -> int_<name>__events           view         every row version, business clock
           -> int_<name>__daily_partials incr      REUSABLE per-entity/per-day state
                -> int_<name>__window_rollup  view bounded windows
                -> int_<name>__alltime_state  incr sealed accumulator
                -> int_<name>__alltime_recent view unsealed tail
                     -> <name>               incr  the published wide mart
"""

from __future__ import annotations

import textwrap
from dataclasses import dataclass
from pathlib import Path

from generator.expand import ComputedFeature, FeaturePlan, StoredFeature
from generator.expr import Expr, macro
from generator.revision import reset_accumulator_command
from generator.spec import (
    LOADED_AT,
    SUPERSEDED_AT,
    TARGET_DATE_RE,
    FeatureSpec,
    TimeWindow,
    substitute_relations,
)

GENERATOR = "featuremart"

# Logical type -> the SQL type the mart's contract declares. Spelled the way
# DuckDB, Databricks and Snowflake all accept in DDL.
CONTRACT_TYPES = {
    "bigint": "bigint",
    "double": "double",
    "timestamp": "timestamp",
    "date": "date",
    # Databricks rejects a VARCHAR without a length; STRING is a synonym on all three.
    "varchar": "string",
    "boolean": "boolean",
}


def group_name(owner: str) -> str:
    """The dbt group a spec's owner maps to: owners are free text, groups identifiers."""
    return "".join(c if c.isalnum() else "_" for c in owner.lower()).strip("_") or "unowned"


def _banner(spec: FeatureSpec, kind: str) -> str:
    rel = spec.spec_path.as_posix() if spec.spec_path else "<unknown>"
    return textwrap.dedent(f"""\
        -- ============================================================================
        -- GENERATED FILE - DO NOT EDIT BY HAND
        --
        --   layer      : {kind}
        --   feature    : {spec.feature_name}
        --   spec       : {rel}
        --   spec hash  : {spec.spec_hash}
        --   generator  : {GENERATOR}
        --
        -- Edit the spec and run `make generate`. CI fails when a generated file
        -- differs from what the spec produces, so this file and the spec cannot
        -- drift apart.
        -- ============================================================================
        """)


@dataclass
class RenderedFile:
    path: Path
    content: str


class Renderer:
    def __init__(self, plan: FeaturePlan) -> None:
        self.plan = plan
        self.spec = plan.spec
        self.s = plan.spec.settings
        self.fn = plan.spec.feature_name
        self.ents = list(plan.spec.entities)
        self.hash = plan.spec.spec_hash

    # -- model names --------------------------------------------------------
    @property
    def m_stg(self) -> str:
        return self.spec.relation.staging_model

    @property
    def m_events(self) -> str:
        return f"int_{self.fn}__events"

    @property
    def m_partials(self) -> str:
        return f"int_{self.fn}__daily_partials"

    @property
    def m_rollup(self) -> str:
        return f"int_{self.fn}__window_rollup"

    @property
    def m_state(self) -> str:
        return f"int_{self.fn}__alltime_state"

    @property
    def m_recent(self) -> str:
        return f"int_{self.fn}__alltime_recent"

    @property
    def m_mart(self) -> str:
        return self.fn

    @property
    def has_recent(self) -> bool:
        return self.spec.has_all_time and self.s.late_arrival_days > 0

    # -- window bound helpers ----------------------------------------------
    def _offset(self, days_back: int) -> str:
        return f"{{{{ fs_date_offset_lit({days_back}) }}}}"

    def _window_lo_offset(self, w: TimeWindow) -> int:
        """Days back from the as-of date to the first day inside window `w`."""
        return w.days - 1 if self.s.window_convention == "inclusive" else w.days

    @property
    def _hi_offset(self) -> int:
        return 0 if self.s.window_convention == "inclusive" else 1

    # -- shared column emitters --------------------------------------------
    def _ent_cols(self, prefix: str = "") -> str:
        p = f"{prefix}." if prefix else ""
        return ", ".join(f"{p}{e}" for e in self.ents)

    def _ent_join(self, left: str, right: str) -> str:
        return " and ".join(f"{left}.{e} = {right}.{e}" for e in self.ents)

    def _field_sections(self, items: list[tuple[str, str]], indent: str = "    ") -> str:
        """Emit `expr as name` lines grouped under a per-field comment."""
        lines: list[str] = []
        current: str | None = None
        for group, line in items:
            if group != current:
                if lines:
                    lines.append("")
                lines.append(f"{indent}-- {group} " + "-" * max(4, 68 - len(group)))
                current = group
            lines.append(f"{indent}{line},")
        return "\n".join(lines)

    # ======================================================================
    # 1. events -- the spec's projection, every row version, business clock
    # ======================================================================
    def _to_business_clock(self, expr: str) -> str:
        rel_tz, biz_tz = self.spec.relation.timezone, self.s.timezone
        return macro("fs_convert_tz", Expr.sql(expr), f"'{rel_tz}'", f"'{biz_tz}'").render()

    @property
    def event_payload_columns(self) -> list[str]:
        """Columns of the events model besides the entity keys and event_date."""
        ps = self.spec.parsed_source
        assert ps is not None
        derived = {f.name for f in self.spec.fields if f.is_derived}
        kept = [
            alias
            for alias, expr in ps.items
            if alias not in derived and not TARGET_DATE_RE.search(expr)
        ]
        return [a for a in kept if a not in self.ents] + [LOADED_AT, SUPERSEDED_AT]

    def render_events(self) -> str:
        ps = self.spec.parsed_source
        assert ps is not None

        def subst(sql: str) -> str:
            return substitute_relations(sql, self.spec.relations)

        # An expression whose value moves with the as-of date is removed here,
        # not merely ignored downstream. Letting one reach the partial layer
        # would silently invalidate every stored partial the following day.
        derived = {f.name for f in self.spec.fields if f.is_derived}
        clocked = set(self.spec.timestamp_aliases)
        dropped, keep = [], []
        for alias, expr in ps.items:
            if alias in derived or TARGET_DATE_RE.search(expr):
                dropped.append(alias)
            else:
                keep.append((alias, expr))

        rel = self.spec.relation
        ts_expr = dict(ps.items)[self.spec.timestamp_col.lower()]
        L: list[str] = [_banner(self.spec, "intermediate / events").rstrip("\n"), ""]
        L += [
            "-- The spec's projection over every row version the source holds, with no",
            "-- as-of filter. Deciding which versions a run may see is the partial",
            "-- layer's job, because it needs the versions it may NOT see as well: a",
            "-- version superseded since the last run is how a correction or a deletion",
            "-- announces which day it changed.",
            "--",
            f"-- CLOCK. Timestamps are recorded in {rel.timezone} and read here on the",
            f"-- business clock, {self.s.timezone}, so an event's date and the hour a",
            "-- condition sees never depend on the warehouse session's time zone.",
            "",
            f"{{{{ config(materialized='view', tags=['feature_store', '{self.fn}']) }}}}",
            "",
            "select",
        ]
        for alias, expr in keep:
            e = subst(expr)
            if alias in clocked:
                L.append(f"    {self._to_business_clock(e)} as {alias},")
            else:
                L.append(f"    {alias}," if e == alias else f"    {e} as {alias},")
        L.append(f"    cast({self._to_business_clock(subst(ts_expr))} as date) as event_date,")

        if dropped:
            L += [
                "",
                "    -- Removed from this projection on purpose: " + ", ".join(sorted(dropped)),
                "    -- Each is a function of the as-of date, so storing it in the daily",
                "    -- partial layer would make yesterday's partials wrong today. The mart",
                "    -- rebuilds them from stored timestamps instead, which is exact and",
                "    -- keeps the partials reusable across every as-of date.",
                "",
            ]
        L += [
            f"    {LOADED_AT},",
            f"    {SUPERSEDED_AT}",
            subst(ps.from_clause),
        ]
        return "\n".join(L) + "\n"

    # ======================================================================
    # 2. daily partials -- the reusable state
    # ======================================================================
    @property
    def _cutoff(self) -> str:
        """Literal UTC instant at which the as-of date ends on the business clock."""
        return f"{{{{ fs_knowledge_cutoff('{self.s.timezone}') }}}}"

    def render_partials(self) -> str:
        items = [
            (f"{p.field} / {p.agg_key}", f"{p.partial_sql.render()} as {p.name}")
            for p in self.plan.partials
        ]
        cutoff = self._cutoff
        join_on = "\n            and ".join(f"keys.{e} = visible.{e}" for e in self.ents)
        return (
            _banner(self.spec, "intermediate / daily partial aggregates")
            + textwrap.dedent(f"""
            -- One row per entity per event_date holding COMPOSABLE state, not finished
            -- features. Every window and all_time is a fold over these rows, so a day is
            -- read from the source exactly once per change, however many windows use it.
            --
            -- WHAT A RUN MAY KNOW. As-of date T sees a row version when it was loaded
            -- before T ended on the business clock and was not superseded by then.
            -- Corrections and deletions are therefore read, not ignored: a superseded
            -- version drops out and its successor, if any, takes its place.
            --
            -- WHAT A RUN REWRITES. Exactly the event_dates whose content can differ
            -- from what is stored: dates newly inside the as-of horizon, and dates with
            -- a row version loaded or superseded since the previous run's knowledge
            -- ended. A late arrival, a correction and a deletion all announce the day
            -- they change this way, however far back it lies, so no fixed look-back
            -- window can step over one. A day is rewritten whole.

            {{{{ config(
                materialized='incremental',
                incremental_strategy=fs_partition_replace_strategy(),
                unique_key=['event_date'],
                partition_by=fs_partition_config(['event_date']),
                cluster_by=fs_cluster_config(['event_date']),
                on_schema_change='fail',
                tags=['feature_store', '{self.fn}']
            ) }}}}

            with

            {{% if is_incremental() %}}
            run_state as (

                -- The previous run's as-of date and the instant its knowledge ended.
                -- Taken from the stored rows rather than from the calendar, so a run
                -- that follows missed days covers every change made while it was away.
                select
                    coalesce(max(_computed_for), cast('1900-01-01' as date)) as as_of,
                    coalesce(max(_known_through), cast('1900-01-01' as timestamp)) as known_through
                from {{{{ this }}}}

            ),

            -- MONOTONICITY GUARD.
            -- Each run sees the source as of ITS OWN target_date, so recomputing an
            -- event_date under an earlier as-of date would see less than a later run
            -- already stored and silently drop what it had captured. Runs are not
            -- guaranteed to arrive in order -- backfills, manual replays and retried
            -- tasks all break that assumption -- so an event_date already computed
            -- under a LATER as-of date is left alone.
            already_fresher as (

                select distinct event_date
                from {{{{ this }}}}
                where _computed_for > {self._offset(0)}

            ),
            {{% else %}}
            run_state as (

                -- First build: nothing is stored, so every day is new.
                select
                    cast('1900-01-01' as date) as as_of,
                    cast('1900-01-01' as timestamp) as known_through

            ),

            already_fresher as (

                select cast(null as date) as event_date
                where 1 = 0

            ),
            {{% endif %}}

            backfill as (

                -- Initial load or replay: rebuild every day from the requested start.
                select
                    cast('{{{{ var('fs_backfill_from', '1900-01-01') }}}}' as date) as rebuild_from,
                    {{{{ 'true' if var('fs_backfill_from', none) else 'false' }}}} as rebuild_all

            ),

            recompute as (

                select distinct e.event_date
                from {{{{ ref('{self.m_events}') }}}} as e
                cross join run_state as r
                cross join backfill as b
                where
                    e.event_date <= {self._offset(0)}
                    and e.event_date >= b.rebuild_from
                    and (
                        b.rebuild_all
                        or e.event_date > r.as_of
                        or (e.{LOADED_AT} >= r.known_through and e.{LOADED_AT} < {cutoff})
                        or (e.{SUPERSEDED_AT} >= r.known_through and e.{SUPERSEDED_AT} < {cutoff})
                    )
                    and e.event_date not in (select already_fresher.event_date from already_fresher)

            ),

            visible as (

                select e.*
                from {{{{ ref('{self.m_events}') }}}} as e
                where
                    e.event_date in (select recompute.event_date from recompute)
                    and e.{LOADED_AT} < {cutoff}
                    and (e.{SUPERSEDED_AT} is null or e.{SUPERSEDED_AT} >= {cutoff})

            ),

            keys as (

                -- Every entity-day this run writes, once. An entity-day stored earlier
                -- whose every row has since been superseded is written again as a
                -- TOMBSTONE: folded over no rows, which is each monoid's identity
                -- state, with _n_rows = 0. Writing nothing instead would leave the
                -- stale row in place wherever a day is only partly rewritten.
                select distinct
                    {self._ent_cols()},
                    event_date
                from visible
                {{% if is_incremental() %}}

                union

                select distinct
                    {self._ent_cols()},
                    event_date
                from {{{{ this }}}}
                where event_date in (select recompute.event_date from recompute)
                {{% endif %}}

            ),

            keyed_rows as (

                -- One row per visible source row, and one all-NULL row per tombstone.
                select
            """).rstrip("\n")
            + "\n"
            + ",\n".join(
                [f"        keys.{c}" for c in [*self.ents, "event_date"]]
                + [f"        visible.{c}" for c in self.event_payload_columns]
            )
            + textwrap.dedent(f"""
                from keys
                left join visible
                    on
                        {join_on}
                        and keys.event_date = visible.event_date

            )

            select
                {self._ent_cols()},
                event_date,

            """).rstrip("\n")
            + "\n"
            + self._field_sections(items)
            + textwrap.dedent(f"""

                count({LOADED_AT}) as _n_rows,
                {self._offset(0)} as _computed_for,
                {cutoff} as _known_through,
                '{self.spec.state_version}' as _state_version
            from keyed_rows
            group by {self._ent_cols()}, event_date
            """)
        )

    # ======================================================================
    # 3. bounded window roll-up
    # ======================================================================
    def render_rollup(self) -> str:
        bounded = [f for f in self.plan.stored if not f.window.is_all_time]
        widest = self.spec.max_window_days

        bound_lines = [f"        {self._offset(self._hi_offset)} as d_hi,"]
        for w in self.spec.bounded_windows:
            bound_lines.append(f"        {self._offset(self._window_lo_offset(w))} as lo_{w.name},")
        bound_lines[-1] = bound_lines[-1].rstrip(",")

        items = []
        for f in bounded:
            # The widest window equals the outer scan, so it needs no extra
            # predicate; narrower ones are cut against a bound column. Bounds
            # are plain columns rather than inline literals so that the
            # predicate stays pure SQL even inside a dispatch-macro argument.
            pred = None if f.window.days == widest else f"event_date >= lo_{f.window.name}"
            expr = f.partial.aggregate.window_expr(f.partial.name, pred)
            items.append((f"{f.field} / {f.agg_key}", f"{expr.render()} as {f.name}"))

        return (
            _banner(self.spec, "intermediate / bounded window roll-up")
            + textwrap.dedent(f"""
            -- Folds at most {widest} rows of stored partial state per entity into the
            -- bounded windows. Nothing is recomputed from the raw source here.
            --
            -- An entity whose rows in the widest window are all tombstones has no
            -- activity there, so it is left out exactly as if it had never had any.

            {{{{ config(materialized='view', tags=['feature_store', '{self.fn}']) }}}}

            with bounds as (

                select
            """).rstrip("\n")
            + "\n"
            + "\n".join(bound_lines)
            + textwrap.dedent(f"""

            ),

            partials as (

                select
                    p.*,
                    b.*
                from {{{{ ref('{self.m_partials}') }}}} as p
                cross join bounds as b
                where
                    p.event_date >= b.lo_{self.spec.bounded_windows[-1].name}
                    and p.event_date <= b.d_hi

            )

            select
                {self._ent_cols()},

            """).rstrip("\n")
            + "\n"
            + self._field_sections(items)
            + "\n\n    max(case when _n_rows > 0 then event_date end) as _rollup_last_event_date\n"
            + f"from partials\ngroup by {self._ent_cols()}\nhaving sum(_n_rows) > 0\n"
        )

    # ======================================================================
    # 4. all_time sealed accumulator
    # ======================================================================
    def render_alltime_state(self) -> str:
        late = self.s.late_arrival_days
        agg_items, merge_items = [], []
        for p in self.plan.partials:
            agg = p.aggregate.state_agg_expr(Expr.sql(f"p.{p.name}"))
            agg_items.append((f"{p.field} / {p.agg_key}", f"{agg.render()} as {p.name}"))
            merged = p.aggregate.merge_expr(Expr.sql(f"prev.{p.name}"), Expr.sql(f"n.{p.name}"))
            merge_items.append((f"{p.field} / {p.agg_key}", f"{merged.render()} as {p.name}"))

        first_restated = self.ents[0]
        return (
            _banner(self.spec, "intermediate / all_time sealed state")
            + textwrap.dedent(f"""
            -- The running all_time accumulator: one row per entity, holding the same
            -- composable state as a daily partial but folded over all sealed history.
            --
            -- SEALING. Only event_dates at or before target_date - {late} are folded in,
            -- because more recent days are still provisional and are rewritten often.
            -- The mart completes all_time by merging this with the unsealed tail, using
            -- the same merge function, so the published number is never stale.
            --
            -- SELF-HEALING AND IDEMPOTENT. The consumed range is
            -- (stored watermark, seal date], not "yesterday". A retried run consumes an
            -- empty range and changes nothing; a run that follows a missed day picks the
            -- gap up automatically. Neither case needs an operator.
            --
            -- RESTATEMENT. A sealed day can still change: a correction, a deletion or
            -- a very late arrival rewrites its partial row, stamped with the knowledge
            -- of the run that rewrote it. Most aggregations have no inverse -- a max
            -- cannot be "un-merged" -- so an entity whose sealed history changed after
            -- it was folded is not patched but re-folded from every stored day. That
            -- touches only the entities a change reached, and is exact by
            -- construction: it is the same fold, over the same days.
            --
            -- Only entities with activity in the range, or restated, are written. An
            -- entity with neither has nothing to fold, so leaving its row untouched is
            -- correct and keeps write volume proportional to change, not population.
            --
            -- A consequence worth knowing: across a range with NO activity at all,
            -- nothing is written and the watermark does not advance. That is accurate
            -- rather than stuck -- the state really is sealed only through the old
            -- watermark -- and the next run with data simply consumes the wider range.
            -- It does mean the watermark tracks the last day with events, not the last
            -- day attempted.

            {{{{ config(
                materialized='incremental',
                incremental_strategy=fs_upsert_strategy(),
                unique_key={self.ents!r},
                on_schema_change='fail',
                tags=['feature_store', '{self.fn}']
            ) }}}}

            with watermark as (

                {{% if is_incremental() %}}
                select
                    coalesce(max(_state_as_of_date), cast('1900-01-01' as date)) as wm,
                    coalesce(max(_folded_through), cast('1900-01-01' as timestamp)) as folded_through
                from {{{{ this }}}}
                {{% else %}}
                select
                    cast('1900-01-01' as date) as wm,
                    cast('1900-01-01' as timestamp) as folded_through
                {{% endif %}}

            ),

            seal as (

                -- Never behind the stored watermark. A run for an earlier as-of date
                -- has nothing new to seal, and must not re-fold a restated entity to
                -- a shorter history than every other entity holds.
                select
                    w.wm,
                    w.folded_through,
                    {{{{ fs_greatest2('w.wm', fs_date_offset_lit({late})) }}}} as through
                from watermark as w

            ),

            restated as (

                select distinct {self._ent_cols("p")}
                from {{{{ ref('{self.m_partials}') }}}} as p
                cross join seal as s
                where
                    p.event_date <= s.wm
                    and p._known_through > s.folded_through

            ),

            new_days as (

                -- A restated entity folds every stored day through the seal; any
                -- other folds only the days after the watermark.
                select
                    {self._ent_cols("p")},

            """).rstrip("\n")
            + "\n"
            + self._field_sections(agg_items, indent="        ")
            + textwrap.dedent(f"""

                    sum(p._n_rows) as _n_rows,
                    min(case when p._n_rows > 0 then p.event_date end) as _min_event_date,
                    max(case when p._n_rows > 0 then p.event_date end) as _max_event_date,
                    max(case when r.{first_restated} is null then 0 else 1 end) as _restated
                from {{{{ ref('{self.m_partials}') }}}} as p
                cross join seal as s
                left join restated as r on {self._ent_join("p", "r")}
                where
                    p.event_date <= s.through
                    and (p.event_date > s.wm or r.{first_restated} is not null)
                group by {self._ent_cols("p")}

            ),

            prev as (

                {{% if is_incremental() %}}
                select * from {{{{ this }}}}
                {{% else %}}
                -- First build: no rows, so the merge below is the only projection in
                -- the model and cannot diverge between branches.
                select * from new_days
                where 1 = 0
                {{% endif %}}

            )

            select
                {self._ent_cols("n")},

            """).rstrip("\n")
            + "\n"
            + self._field_sections(merge_items)
            + textwrap.dedent(f"""

                (coalesce(prev._n_rows, 0) + n._n_rows) as _n_rows,
                {{{{ fs_least2('prev._min_event_date', 'n._min_event_date') }}}} as _min_event_date,
                {{{{ fs_greatest2('prev._max_event_date', 'n._max_event_date') }}}} as _max_event_date,
                s.through as _state_as_of_date,
                {self._cutoff} as _folded_through,
                '{self.spec.state_version}' as _state_version
            from new_days as n
            cross join seal as s
            -- A restated entity is re-folded from scratch, so it takes nothing from
            -- its previous state: merging with no row is each monoid's identity.
            left join prev on {self._ent_join("n", "prev")} and n._restated = 0
            """).rstrip()
            + "\n"
        )

    # ======================================================================
    # 5. unsealed tail
    # ======================================================================
    def render_alltime_recent(self) -> str:
        items = []
        for p in self.plan.partials:
            agg = p.aggregate.state_agg_expr(Expr.sql(f"p.{p.name}"))
            items.append((f"{p.field} / {p.agg_key}", f"{agg.render()} as {p.name}"))

        return (
            _banner(self.spec, "intermediate / all_time unsealed tail")
            + textwrap.dedent(f"""
            -- The days the sealed accumulator has not absorbed yet. Merging this into
            -- the sealed state gives an all_time value current to the as-of date while
            -- still tolerating late-arriving events.
            --
            -- The range starts at the accumulator's ACTUAL watermark, not at
            -- target_date - late_arrival_days. Those two coincide on an ordinary
            -- forward run, but they diverge whenever a past as-of date is replayed: the
            -- watermark is global and would then sit AHEAD of the nominal seal, so a
            -- target-derived range would re-add days the accumulator already holds and
            -- silently double-count every all_time feature. Anchoring to the watermark
            -- makes sealed and unsealed complementary by construction, for any as-of
            -- date and in any order.

            {{{{ config(materialized='view', tags=['feature_store', '{self.fn}']) }}}}

            with watermark as (

                select coalesce(max(_state_as_of_date), cast('1900-01-01' as date)) as wm
                from {{{{ ref('{self.m_state}') }}}}

            )

            select
                {self._ent_cols("p")},

            """).rstrip("\n")
            + "\n"
            + self._field_sections(items)
            + textwrap.dedent(f"""

                sum(p._n_rows) as _n_rows,
                min(case when p._n_rows > 0 then p.event_date end) as _min_event_date,
                max(case when p._n_rows > 0 then p.event_date end) as _max_event_date
            from {{{{ ref('{self.m_partials}') }}}} as p
            cross join watermark as w
            where
                p.event_date > w.wm
                and p.event_date <= {self._offset(0)}
            group by {self._ent_cols("p")}
            """).rstrip()
            + "\n"
        )

    # ======================================================================
    # 6. the published mart
    # ======================================================================
    def _merge_for(self, partial_name: str, sealed: Expr) -> Expr:
        partial = next(p for p in self.plan.partials if p.name == partial_name)
        return partial.aggregate.merge_expr(sealed, Expr.sql(f"at_recent.{partial_name}"))

    def render_mart(self) -> str:
        has_bounded = bool(self.spec.bounded_windows)
        has_at = self.spec.has_all_time

        ctes: list[tuple[str, str]] = []
        joins: list[str] = []
        spine_parts: list[str] = []
        # A row whose state folds no rows at all (every event since superseded)
        # holds identity state, and an entity known only through such rows was
        # never seen as far as this as-of date can tell.
        if has_bounded:
            ctes.append(("rollup", self.m_rollup))
            spine_parts.append(f"select {self._ent_cols()} from rollup")
            joins.append(f"left join rollup on {self._ent_join('spine', 'rollup')}")
        if has_at:
            ctes.append(("at_state", self.m_state))
            spine_parts.append(f"select {self._ent_cols()} from at_state\nwhere _n_rows > 0")
            joins.append(f"left join at_state on {self._ent_join('spine', 'at_state')}")
        if self.has_recent:
            ctes.append(("at_recent", self.m_recent))
            spine_parts.append(f"select {self._ent_cols()} from at_recent\nwhere _n_rows > 0")
            joins.append(f"left join at_recent on {self._ent_join('spine', 'at_recent')}")

        if self.s.entity_spine == "active_window" and has_bounded:
            widest = self.spec.max_window_days
            spine_note = [
                "-- entity_spine: active_window. Only entities with activity inside the",
                f"-- widest bounded window ({widest} days) get a row for this as-of date.",
                "--",
                "-- READ THIS BEFORE JOINING. A dormant entity gets NO ROW for this date,",
                "-- not a row of zeros. Rows are partitioned by target_date, so an equi-join",
                "-- on (entity, target_date) MISSES for a dormant entity rather than",
                "-- resolving to an older snapshot -- its last row sits at an earlier",
                "-- target_date and only a range join would find it.",
                "--",
                "-- So the consumer has to decide what a missing row means. Treating it as",
                "-- genuine inactivity is usually right for count features and wrong for",
                "-- extrema and all_time, which are not zero for a dormant entity, merely",
                "-- unpublished. Switch to entity_spine: all_time if that call is not one",
                "-- the consumers should be making.",
            ]
            spine_body = [f"select {self._ent_cols()} from rollup"]
        else:
            spine_note = [
                "-- entity_spine: all_time. Every entity ever seen gets a row on every",
                "-- as-of date, so a training-set join never silently drops a dormant",
                "-- population. This is the expensive option by design; switch to",
                "-- active_window in the spec if the daily row count outgrows its value.",
            ]
            spine_body = []
            for i, part in enumerate(spine_parts):
                if i:
                    spine_body.append("union")
                spine_body.append(part)

        # --- joined: resolve every stored feature, bounded and all_time ------
        joined_items: list[tuple[str, str]] = []
        for f in self.plan.stored:
            group = f"{f.field} / {f.agg_key}"
            if f.window.is_all_time:
                sealed = Expr.sql(f"at_state.{f.partial.name}")
                state = self._merge_for(f.partial.name, sealed) if self.has_recent else sealed
                expr = f.partial.aggregate.finalize_expr(state)
            elif f.partial.aggregate.zero_filled:
                # The left join reintroduces NULL for an entity absent from the
                # roll-up. "No matching rows" is a count of 0, not unknown.
                expr = Expr.sql(f"coalesce(rollup.{f.name}, 0)")
            else:
                # Already named for what it is; aliasing it to itself adds nothing.
                joined_items.append((group, f"rollup.{f.name}"))
                continue
            joined_items.append((group, f"{expr.render()} as {f.name}"))

        if has_at and self.has_recent:
            first = "{{ fs_least2('at_state._min_event_date', 'at_recent._min_event_date') }}"
            last = "{{ fs_greatest2('at_state._max_event_date', 'at_recent._max_event_date') }}"
        elif has_at:
            first, last = "at_state._min_event_date", "at_state._max_event_date"
        else:
            first, last = "cast(null as date)", "rollup._rollup_last_event_date"

        # --- final projection, in spec order ---------------------------------
        by_name: dict[str, StoredFeature | ComputedFeature] = {
            **{s.name: s for s in self.plan.stored},
            **{c.name: c for c in self.plan.computed},
        }
        out_items: list[tuple[str, str]] = []
        for name in self.plan.output_order:
            item = by_name[name]
            group = f"{item.field} / {item.agg_key}"
            value = item.expr.render() if isinstance(item, ComputedFeature) else item.name
            out_items.append((group, f"cast({value} as {CONTRACT_TYPES[item.dtype]}) as {name}"))

        L: list[str] = [_banner(self.spec, "mart / published feature table").rstrip("\n"), ""]
        L += [
            f"-- One row per entity per as-of date, {self.plan.feature_count} feature columns wide.",
            "--",
            "-- POINT-IN-TIME SAFETY. Nothing here can see past target_date: the partial",
            "-- layer reads only what was knowable when that day ended, refuses events",
            "-- after it, and every window is bounded above by it, so joining this table",
            "-- to labels on target_date is leak-free by construction.",
            "--",
            "-- CONTRACT. This is the public interface, so its columns and their types are",
            "-- enforced, every value is cast explicitly, and a change to the column set",
            "-- FAILS the incremental build rather than syncing it. Syncing would publish",
            "-- a new feature as NULL across every earlier partition, which reads as",
            "-- genuine absence. Ship a changed feature set as a new spec version, or",
            "-- replay the history it needs.",
            "",
            "{{ config(",
            f"    materialized='{self.s.materialized_mart}',",
            "    incremental_strategy=fs_partition_replace_strategy(),",
            "    unique_key=['target_date'],",
            "    partition_by=fs_partition_config(['target_date']),",
            "    cluster_by=fs_cluster_config(['target_date']),",
            "    on_schema_change='fail',",
            f"    tags=['feature_store', '{self.fn}', 'mart']",
            ") }}",
            "",
            "with",
            "",
        ]
        for alias, model in ctes:
            L += [f"{alias} as (", "", f"    select * from {{{{ ref('{model}') }}}}", "", "),", ""]
        L += ["spine as ("]
        spine_lines = [line for part in spine_body for line in part.split("\n")]
        L += ["", *[f"    {n}" for n in spine_note], *[f"    {b}" for b in spine_lines], ""]
        L += ["),", "", "joined as (", "", "    select", f"        {self._ent_cols('spine')},", ""]
        L += [self._field_sections(joined_items, indent="        ")]
        L += [
            "",
            f"        {first} as _first_event_date,",
            f"        {last} as _last_event_date",
            "    from spine",
            *[f"    {j}" for j in joins],
            "",
            ")",
            "",
            "select",
            "    {{ fs_target_date() }} as target_date,",
            *[
                f"    cast({e} as {CONTRACT_TYPES[self.spec.entity_types[e]]}) as {e},"
                for e in self.ents
            ],
            "",
        ]
        L += [self._field_sections(out_items, indent="    ")]
        L += [
            "",
            "    cast(_first_event_date as date) as _first_event_date,",
            "    cast(_last_event_date as date) as _last_event_date,",
            "    cast({{ dbt.current_timestamp() }} as timestamp) as _generated_at,",
            f"    cast('{self.hash}' as {CONTRACT_TYPES['varchar']}) as _spec_version",
            "from joined",
        ]
        return "\n".join(L) + "\n"

    # ======================================================================
    # 7. schema.yml -- every generated column documented
    # ======================================================================
    def render_schema_yml(self) -> tuple[str, str]:
        """(marts YAML, intermediate YAML): each lives beside the models it describes."""
        import yaml as _yaml

        def col(
            name: str,
            desc: str,
            tests: list | None = None,
            dtype: str | None = None,
            not_null: bool = False,
        ) -> dict:
            d: dict = {"name": name, "description": desc}
            if dtype:
                d["data_type"] = CONTRACT_TYPES[dtype]
            if not_null:
                d["constraints"] = [{"type": "not_null"}]
            if tests:
                d["data_tests"] = tests
            return d

        def unique(*cols: str) -> list[dict]:
            return [
                {"fs_unique_combination": {"arguments": {"combination_of_columns": list(cols)}}}
            ]

        group = group_name(self.spec.created_by)
        private = {"access": "private", "group": group}
        state_version = col(
            "_state_version",
            f"Fingerprint of what the stored state means ({self.spec.state_version}). "
            "Every row must carry the current one; a generated test fails the build if not.",
        )

        # --- the published mart: the contract consumers rely on -------------
        mart_cols = [
            col(
                "target_date",
                "As-of date these features describe. Every value is computed from what was "
                "knowable when this date ended on the business clock "
                f"({self.s.timezone}), so joining labels on it cannot leak the future.",
                dtype="date",
                not_null=True,
            )
        ]
        for e in self.ents:
            mart_cols.append(
                col(e, f"Entity key ({e}).", dtype=self.spec.entity_types[e], not_null=True)
            )
        for item in self.plan.outputs():
            mart_cols.append(col(item.name, item.description, dtype=item.dtype))
        mart_cols += [
            col(
                "_first_event_date",
                "Earliest event_date ever seen for this entity.",
                dtype="date",
            ),
            col(
                "_last_event_date",
                "Latest event_date seen at or before target_date. Use it to tell a genuine "
                "zero from a dormant entity.",
                dtype="date",
            ),
            col("_generated_at", "Wall-clock time this row was materialised.", dtype="timestamp"),
            col(
                "_spec_version",
                f"Fingerprint of the spec that produced this row ({self.hash}). A change "
                "here explains a change in feature semantics.",
                dtype="varchar",
            ),
        ]

        desc = self.spec.description or f"Daily feature mart for {self.fn}."
        models: list[dict] = [
            {
                "name": self.m_mart,
                "description": (
                    f"{desc}\n\n"
                    f"{self.plan.feature_count} features per entity per day, expanded from "
                    f"{len(self.spec.fields)} atomic fields x "
                    f"{len(self.spec.categories)} condition categories x "
                    f"{len(self.spec.windows)} time windows.\n"
                    f"Owner: {self.spec.created_by}. Generated from "
                    f"{self.spec.spec_path.as_posix() if self.spec.spec_path else '?'}."
                ),
                "config": {
                    "access": "public",
                    "group": group,
                    "contract": {"enforced": True},
                },
                "data_tests": unique("target_date", *self.ents),
                "columns": mart_cols,
            },
            {
                "name": self.m_events,
                "description": (
                    f"Every row version of {self.spec.relation.source_name}."
                    f"{self.spec.relation.table_name} through this spec's projection, with "
                    f"timestamps on the business clock ({self.s.timezone}). Not bound to an "
                    "as-of date: the partial layer decides what each run may see."
                ),
                "config": private,
                "columns": [
                    col(
                        "event_date",
                        f"Date of the event on the business clock ({self.s.timezone}).",
                        ["not_null"],
                    ),
                    col(LOADED_AT, "When this row version became visible, in UTC."),
                    col(SUPERSEDED_AT, "When it was replaced or deleted, in UTC; NULL if never."),
                ],
            },
            {
                "name": self.m_partials,
                "description": (
                    "Reusable per-entity, per-event_date partial aggregate state. Composable "
                    "by construction: every published window is a fold over these rows."
                ),
                "config": private,
                "data_tests": unique(*self.ents, "event_date"),
                "columns": [
                    col(
                        "event_date",
                        "Calendar date of the events summarised by this row.",
                        ["not_null"],
                    ),
                    *[col(e, f"Entity key ({e}).", ["not_null"]) for e in self.ents],
                    col(
                        "_n_rows",
                        "Source rows folded into this row. 0 marks a tombstone: an entity-day "
                        "whose every row was superseded, holding each aggregation's identity.",
                        ["not_null"],
                    ),
                    col(
                        "_computed_for", "As-of date of the run that wrote this row.", ["not_null"]
                    ),
                    col(
                        "_known_through",
                        "UTC instant that run's knowledge ended. The next run looks for "
                        "changes loaded or superseded from here on.",
                        ["not_null"],
                    ),
                    state_version,
                ],
            },
        ]
        if self.spec.bounded_windows:
            models.append(
                {
                    "name": self.m_rollup,
                    "description": "Bounded-window features per entity, folded from partials.",
                    "config": private,
                    "data_tests": unique(*self.ents),
                }
            )
        if self.spec.has_all_time:
            models.append(
                {
                    "name": self.m_state,
                    "description": (
                        "Sealed all_time accumulator, one row per entity. Advances only over "
                        "event_dates past their provisional window; an entity whose sealed "
                        "history changes afterwards is re-folded from its stored days."
                    ),
                    "config": private,
                    "data_tests": unique(*self.ents),
                    "columns": [
                        *[col(e, f"Entity key ({e}).", ["not_null"]) for e in self.ents],
                        col(
                            "_state_as_of_date",
                            "Watermark: every event_date at or before this has been folded in. "
                            "Drives the self-healing, idempotent catch-up range.",
                            ["not_null"],
                        ),
                        col(
                            "_folded_through",
                            "UTC instant the knowledge of the run that wrote this row ended. A "
                            "partial rewritten after the newest of these, on a sealed day, "
                            "restates its entity.",
                            ["not_null"],
                        ),
                        col(
                            "_n_rows",
                            "Source rows folded in. 0 when every event was since superseded; "
                            "such an entity is not published.",
                            ["not_null"],
                        ),
                        state_version,
                    ],
                }
            )
        if self.has_recent:
            models.append(
                {
                    "name": self.m_recent,
                    "description": "all_time state for the days after the watermark.",
                    "config": private,
                    "data_tests": unique(*self.ents),
                }
            )

        mart_doc: dict = {"version": 2, "models": models[:1]}
        intermediate_doc: dict = {"version": 2, "models": models[1:]}
        if self.spec.exposures:
            mart_doc["exposures"] = [
                {
                    "name": e.name,
                    "type": e.type,
                    **({"maturity": e.maturity} if e.maturity else {}),
                    **({"url": e.url} if e.url else {}),
                    "description": e.description or f"Consumer of {self.m_mart}.",
                    "owner": {
                        k: v for k, v in (("name", e.owner_name), ("email", e.owner_email)) if v
                    },
                    "depends_on": [f"ref('{self.m_mart}')"],
                }
                for e in self.spec.exposures
            ]

        header = (
            "# ============================================================================\n"
            "# GENERATED FILE - DO NOT EDIT BY HAND\n"
            f"#   feature   : {self.fn}\n"
            f"#   spec hash : {self.spec.spec_hash}\n"
            "#   Edit the spec and run `make generate`.\n"
            "# ============================================================================\n"
        )
        return tuple(  # type: ignore[return-value]
            header + _yaml.safe_dump(d, sort_keys=False, width=100, allow_unicode=True)
            for d in (mart_doc, intermediate_doc)
        )

    # ======================================================================
    # 8. generated invariant tests
    # ======================================================================
    # These are worth more than the sum of their parts: they do not check that
    # the SQL runs, they check that the algebra held on real data. A broken
    # merge, a mis-signed window bound or a leaked future event all surface
    # here as an arithmetic contradiction rather than as a silently wrong
    # number that ships into a model.

    def _test_header(self, purpose: str) -> str:
        """Banner plus group: a test that reads private state must belong to its owner."""
        group = group_name(self.spec.created_by)
        return _banner(self.spec, purpose) + f"\n{{{{ config(group='{group}') }}}}\n"

    def _violation_test(self, name: str, purpose: str, checks: list[tuple[str, str]]) -> str:
        if not checks:
            terms = "        '' as violations"
        else:
            terms = (
                "\n".join(
                    f"        {'' if i == 0 else '|| '}"
                    f"coalesce(case when {cond} then '{label};' end, '')"
                    for i, (cond, label) in enumerate(checks)
                )
                + "\n        || '' as violations"
            )
        return (
            self._test_header(f"test / {purpose}")
            + textwrap.dedent(f"""
            -- {len(checks)} invariant(s), evaluated in a SINGLE scan of the as-of partition.
            -- Each violated invariant names itself in the `violations` column, so a failure
            -- points at the exact feature rather than at the table.

            with checked as (

                select
                    target_date,
                    {self._ent_cols()},
            """).rstrip("\n")
            + "\n"
            + terms
            + textwrap.dedent(f"""
                from {{{{ ref('{self.m_mart}') }}}}
                where target_date = {{{{ fs_target_date() }}}}

            )

            select
                target_date,
                {self._ent_cols()},
                violations
            from checked
            where violations <> ''
            """).rstrip()
            + "\n"
        )

    def _ordered_windows(self) -> list[TimeWindow]:
        return sorted(self.spec.windows, key=lambda w: (1, 0) if w.is_all_time else (0, w.days))

    def _feature_index(self) -> dict[tuple[str, str, str, str], StoredFeature | ComputedFeature]:
        """(field, agg, combo_label, window) -> published feature."""
        items = [*self.plan.public_stored, *self.plan.computed]
        return {(i.field, i.agg_key, i.combo.label, i.window.name): i for i in items}

    def render_test_non_negative(self) -> str:
        checks = [
            (f"{item.name} < 0", f"{item.name}<0")
            for item in self.plan.outputs()
            if item.aggregation.non_negative
        ]
        return self._violation_test("non_negative", "values are never negative", checks)

    def render_test_window_monotonicity(self) -> str:
        # A wider window covers a superset of a narrower one's rows, so every
        # ordered aggregation moves in its declared direction as windows widen.
        windows = self._ordered_windows()
        idx = self._feature_index()
        checks = []
        seen: set[tuple[str, str, str]] = set()
        for field, agg, label, _win in idx:
            key = (field, agg, label)
            if key in seen:
                continue
            seen.add(key)
            for a, b in zip(windows, windows[1:], strict=False):
                na, nb = idx.get((field, agg, label, a.name)), idx.get((field, agg, label, b.name))
                if not na or not nb or not na.aggregation.rows_order:
                    continue
                op = ">" if na.aggregation.rows_order > 0 else "<"
                checks.append((f"{na.name} {op} {nb.name}", f"{na.name}!{op}{b.name}"))
                # Covering more rows cannot lose a value that fewer rows had.
                if na.aggregation.nullable:
                    checks.append(
                        (
                            f"{na.name} is not null and {nb.name} is null",
                            f"{nb.name}_null_but_{na.name}_set",
                        )
                    )
        return self._violation_test("window_monotonicity", "nested windows stay ordered", checks)

    def render_test_internal_coherence(self) -> str:
        idx = self._feature_index()
        checks = []
        # A condition combo selects a subset of the rows the marginal sees, so
        # the marginal is ordered after every combo built from it, exactly as a
        # wider window is ordered after a narrower one.
        for (field, agg, label, win), item in idx.items():
            order = item.aggregation.rows_order
            marginal = idx.get((field, agg, "", win))
            if label == "" or not order or not marginal:
                continue
            op = "<" if order > 0 else ">"
            checks.append((f"{marginal.name} {op} {item.name}", f"{marginal.name}{op}{item.name}"))
            if item.aggregation.nullable:
                checks.append(
                    (
                        f"{item.name} is not null and {marginal.name} is null",
                        f"{marginal.name}_null_but_{item.name}_set",
                    )
                )

        # An aggregation bounded by another over the same rows never exceeds
        # it: min <= max, and an exact distinct count <= the row count.
        for (field, _agg, label, win), item in idx.items():
            bound_key = item.aggregation.bounded_by
            bound = idx.get((field, bound_key, label, win)) if bound_key else None
            if bound:
                checks.append((f"{item.name} > {bound.name}", f"{item.name}>{bound.name}"))

        return self._violation_test(
            "internal_coherence",
            "marginals dominate, min <= max, distinct <= count",
            checks,
        )

    def render_test_state_watermark(self) -> str:
        late = self.s.late_arrival_days
        return (
            self._test_header("test / all_time state is usable for this as-of date")
            + textwrap.dedent(f"""
            -- The accumulator is a single forward-only fold, so its watermark is global
            -- while a mart partition is per-date. This asserts the one relationship
            -- between them that all_time correctness depends on.
            --
            -- The accumulator stores `_state_as_of_date = W` having folded in exactly
            -- the event_dates <= W. So for an as-of date T, whose all_time must see
            -- event_dates <= T and nothing later:
            --
            --     W <= T
            --
            -- Recomputing a PAST mart partition is safe and required -- that is what the
            -- revision window does -- and it satisfies this automatically: an in-order
            -- run leaves W = T - {late}, so every date in the revision window
            -- (T-1 .. T-{late}) is at or after W. The unsealed tail, anchored to W rather
            -- than to the as-of date, supplies the remainder exactly.
            --
            -- What this catches is a watermark that has moved AHEAD of the date being
            -- served, which happens when as-of dates are run out of order. Then the
            -- sealed fold already contains events this partition must not see, the
            -- excess is inside the fold rather than beside it, and no tail can subtract
            -- it: every all_time feature for this date would leak the future. So the
            -- build fails here instead of publishing numbers that look plausible and
            -- score well offline.
            --
            -- Remedy -- rebuild the accumulator at the date being served:
            --   {reset_accumulator_command(self.m_state, "<date>")}

            with watermark as (

                select coalesce(max(_state_as_of_date), cast('1900-01-01' as date)) as wm
                from {{{{ ref('{self.m_state}') }}}}

            )

            select
                wm as sealed_through,
                {{{{ fs_target_date() }}}} as as_of_date,
                {{{{ fs_datediff_day(fs_target_date(), 'wm') }}}} as days_ahead,
                'sealed all_time state contains events this as-of date must not see'
                    as problem
            from watermark
            where wm > {{{{ fs_target_date() }}}}
            """).rstrip()
            + "\n"
        )

    def render_test_state_version(self) -> str:
        layers = [("daily_partials", self.m_partials)]
        if self.spec.has_all_time:
            layers.append(("alltime_state", self.m_state))
        selects = "\n\n            union all\n\n            ".join(
            textwrap.dedent(f"""\
                select
                    '{layer}' as layer,
                    _state_version as found,
                    count(*) as n_rows
                from {{{{ ref('{model}') }}}}
                where _state_version is distinct from '{self.spec.state_version}'
                group by _state_version""").replace("\n", "\n            ")
            for layer, model in layers
        )
        return (
            self._test_header("test / stored state was built by this spec")
            + textwrap.dedent(f"""
            -- Stored state is only reusable while it means what the spec says. Change a
            -- predicate, a source expression or the time zone and every row folded
            -- before the change still holds the OLD meaning, with nothing in its
            -- columns to show it: the partial layer only rewrites the days that
            -- changed, and the accumulator never revisits sealed history unprompted.
            -- So every stored row carries the fingerprint of the meaning it was built
            -- under ({self.spec.state_version}), and this fails the build while any row
            -- carries another.
            --
            -- Remedy: rebuild the stored history under the new meaning,
            --   make -C showcase dbt-backfill TARGET_DATE=<date> BACKFILL_FROM=<start>
            -- or, when the published history must stay as it was, ship the change
            -- as a new spec version alongside this one.

            {selects}
            """).rstrip()
            + "\n"
        )

    # ======================================================================
    # 9. write everything
    # ======================================================================
    def render_all(self, root: Path) -> list[RenderedFile]:
        models = root / "models"
        tests = root / "tests"
        # Each spec's intermediate models get a folder of their own, with the
        # YAML that describes them beside them; the mart joins the shared marts/.
        intermediate = models / "intermediate" / self.fn
        mart_yml, intermediate_yml = self.render_schema_yml()
        out = [
            RenderedFile(intermediate / f"{self.m_events}.sql", self.render_events()),
            RenderedFile(intermediate / f"{self.m_partials}.sql", self.render_partials()),
        ]
        if self.spec.bounded_windows:
            out.append(RenderedFile(intermediate / f"{self.m_rollup}.sql", self.render_rollup()))
        if self.spec.has_all_time:
            out.append(
                RenderedFile(intermediate / f"{self.m_state}.sql", self.render_alltime_state())
            )
        if self.has_recent:
            out.append(
                RenderedFile(intermediate / f"{self.m_recent}.sql", self.render_alltime_recent())
            )
        out += [
            RenderedFile(intermediate / f"_int_{self.fn}__models.yml", intermediate_yml),
            RenderedFile(models / "marts" / f"{self.m_mart}.sql", self.render_mart()),
            RenderedFile(models / "marts" / f"_{self.fn}__models.yml", mart_yml),
            RenderedFile(
                tests / f"assert_{self.fn}_non_negative.sql", self.render_test_non_negative()
            ),
            RenderedFile(
                tests / f"assert_{self.fn}_window_monotonicity.sql",
                self.render_test_window_monotonicity(),
            ),
            RenderedFile(
                tests / f"assert_{self.fn}_internal_coherence.sql",
                self.render_test_internal_coherence(),
            ),
            RenderedFile(
                tests / f"assert_{self.fn}_state_version.sql",
                self.render_test_state_version(),
            ),
        ]
        if self.spec.has_all_time:
            out.append(
                RenderedFile(
                    tests / f"assert_{self.fn}_state_watermark.sql",
                    self.render_test_state_watermark(),
                )
            )
        return out
