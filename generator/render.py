"""Render a FeaturePlan into committed dbt models.

The generated SQL is meant to be read in a pull request. That drives three
choices: real column names rather than loops, one section comment per atomic
field, and dialect differences pushed into dispatch macros so the model bodies
stay plain SQL.

Model graph produced per spec:

    stg_<name>                      view         source, as-of date bound
      -> int_<name>__daily_partials incremental  REUSABLE per-entity/per-day state
           -> int_<name>__window_rollup   view   bounded windows
           -> int_<name>__alltime_state   incr   sealed accumulator
           -> int_<name>__alltime_recent  view   unsealed tail
                -> <name>                incr    the published wide mart
"""

from __future__ import annotations

import textwrap
from dataclasses import dataclass
from pathlib import Path

from generator.expand import ComputedFeature, FeaturePlan, StoredFeature
from generator.expr import Expr
from generator.spec import TARGET_DATE_RE, FeatureSpec, TimeWindow, substitute_relations

GENERATOR = "featuremart"


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
        return f"stg_{self.fn}"

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
    # 1. staging
    # ======================================================================
    def render_staging(self) -> str:
        ps = self.spec.parsed_source
        assert ps is not None

        def subst(sql: str) -> str:
            sql = substitute_relations(sql, self.spec.relations)
            return TARGET_DATE_RE.sub("{{ fs_target_date_str() }}", sql)

        # An expression whose value moves with the as-of date is removed here,
        # not merely ignored downstream. Letting one reach the partial layer
        # would silently invalidate every stored partial the following day.
        derived = {f.name for f in self.spec.fields if f.is_derived}
        dropped, keep = [], []
        for alias, expr in ps.items:
            if alias in derived or TARGET_DATE_RE.search(expr):
                dropped.append(alias)
            else:
                keep.append((alias, expr))

        ts = self.spec.timestamp_col
        L: list[str] = [_banner(self.spec, "staging").rstrip("\n"), ""]
        L += [
            "-- Binds the source query to a single as-of date and projects only the",
            "-- columns the feature layer may legally see.",
            "",
            f"{{{{ config(materialized='view', tags=['feature_store', '{self.fn}']) }}}}",
            "",
            "select",
        ]
        for alias, expr in keep:
            e = subst(expr)
            L.append(f"    {alias}," if e == alias else f"    {e} as {alias},")
        L.append(f"    cast({ts} as date) as event_date,")

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
            "    {{ fs_target_date() }} as _compiled_for_date",
            subst(ps.from_clause),
        ]
        return "\n".join(L) + "\n"

    # ======================================================================
    # 2. daily partials -- the reusable state
    # ======================================================================
    def render_partials(self) -> str:
        items = [
            (f"{p.field} / {p.agg_key}", f"{p.partial_sql.render()} as {p.name}")
            for p in self.plan.partials
        ]
        late = self.s.late_arrival_days
        return (
            _banner(self.spec, "intermediate / daily partial aggregates")
            + textwrap.dedent(f"""
            -- One row per entity per event_date holding COMPOSABLE state, not finished
            -- features. Every window and all_time is a fold over these rows, so a day is
            -- read from the source exactly once no matter how many windows consume it.
            --
            -- Each run rewrites the last {late} day(s) as well as today, so events that
            -- arrive late still land in the day they belong to. That is why the all_time
            -- accumulator seals only up to target_date - {late}.

            {{{{ config(
                materialized='incremental',
                incremental_strategy=fs_partition_replace_strategy(),
                unique_key={self.ents + ["event_date"]!r},
                partition_by=fs_partition_config(['event_date']),
                cluster_by=fs_cluster_config(['event_date']),
                on_schema_change='sync_all_columns',
                tags=['feature_store', '{self.fn}']
            ) }}}}

            with

            {{% if is_incremental() %}}
            rewrite_window as (

                -- The rewrite window is anchored to the last as-of date ACTUALLY
                -- processed, not to this run's. If runs were missed, the days that were
                -- still inside their late-arrival window back then are still incomplete
                -- now, and a window measured from today would step straight over them --
                -- losing those events permanently, with nothing to signal it. Taking the
                -- earlier of the two anchors makes the partial layer self-heal across a
                -- gap, exactly as the all_time accumulator does.
                select coalesce(max(_computed_for), cast('1900-01-01' as date)) as last_run
                from {{{{ this }}}}

            ),

            -- MONOTONICITY GUARD.
            -- Each run sees the source as of ITS OWN target_date, so recomputing an
            -- event_date under an earlier as-of date would see fewer rows than a later
            -- run already stored and silently drop events. Runs are not guaranteed to
            -- arrive in order -- backfills, manual replays and retried tasks all break
            -- that assumption -- so an event_date already computed under a LATER as-of
            -- date is left alone. Stored partials can therefore only ever gain
            -- information, never lose it.
            already_fresher as (

                select distinct event_date
                from {{{{ this }}}}
                where _computed_for > {self._offset(0)}

            ),
            {{% endif %}}

            events as (

                select s.*
                from {{{{ ref('{self.m_stg}') }}}} s
                {{% if is_incremental() %}}
                cross join rewrite_window w
                {{% endif %}}
                where s.event_date <= {self._offset(0)}
                {{% if var('fs_backfill_from', none) %}}
                  -- Initial load / replay: widen the window to the requested start date.
                  and s.event_date >= cast('{{{{ var('fs_backfill_from') }}}}' as date)
                {{% elif is_incremental() %}}
                  and {{{{ fs_datediff_day('s.event_date',
                                        fs_least2('w.last_run', fs_target_date())) }}}} <= {late}
                  and s.event_date not in (select event_date from already_fresher)
                {{% else %}}
                  and s.event_date >= {self._offset(late)}
                {{% endif %}}

            )

            select
                {self._ent_cols()},
                event_date,

            """).rstrip("\n")
            + "\n"
            + self._field_sections(items)
            + f"\n\n    {self._offset(0)} as _computed_for,\n"
            + f"    '{self.hash}' as _spec_version\n"
            + f"from events\ngroup by {self._ent_cols()}, event_date\n"
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

            {{{{ config(materialized='view', tags=['feature_store', '{self.fn}']) }}}}

            with bounds as (

                select
            """).rstrip("\n")
            + "\n"
            + "\n".join(bound_lines)
            + textwrap.dedent(f"""

            ),

            partials as (

                select p.*, b.*
                from {{{{ ref('{self.m_partials}') }}}} p
                cross join bounds b
                where p.event_date >= b.lo_{self.spec.bounded_windows[-1].name}
                  and p.event_date <= b.d_hi

            )

            select
                {self._ent_cols()},

            """).rstrip("\n")
            + "\n"
            + self._field_sections(items)
            + "\n\n    max(event_date) as _rollup_last_event_date\n"
            + f"from partials\ngroup by {self._ent_cols()}\n"
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

        return (
            _banner(self.spec, "intermediate / all_time sealed state")
            + textwrap.dedent(f"""
            -- The running all_time accumulator: one row per entity, holding the same
            -- composable state as a daily partial but folded over all sealed history.
            --
            -- SEALING. Only event_dates at or before target_date - {late} are folded in,
            -- because more recent days may still receive late arrivals and get rewritten.
            -- The mart completes all_time by merging this with the unsealed tail, using
            -- the same merge function, so the published number is never stale.
            --
            -- SELF-HEALING AND IDEMPOTENT. The consumed range is
            -- (stored watermark, seal date], not "yesterday". A retried run consumes an
            -- empty range and changes nothing; a run that follows a missed day picks the
            -- gap up automatically. Neither case needs an operator.
            --
            -- Only entities with activity in the range are written. An entity with no
            -- events in the range has, by construction, nothing to fold, so leaving its
            -- row untouched is correct and keeps the write volume proportional to
            -- activity rather than to population.
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
                on_schema_change='sync_all_columns',
                tags=['feature_store', '{self.fn}']
            ) }}}}

            with watermark as (

                {{% if is_incremental() %}}
                select coalesce(max(_state_as_of_date), cast('1900-01-01' as date)) as wm
                from {{{{ this }}}}
                {{% else %}}
                select cast('1900-01-01' as date) as wm
                {{% endif %}}

            ),

            new_days as (

                select
                    {self._ent_cols("p")},

            """).rstrip("\n")
            + "\n"
            + self._field_sections(agg_items, indent="        ")
            + textwrap.dedent(f"""

                    min(p.event_date) as _min_event_date,
                    max(p.event_date) as _max_event_date
                from {{{{ ref('{self.m_partials}') }}}} p
                cross join watermark w
                where p.event_date > w.wm
                  and p.event_date <= {self._offset(late)}
                group by {self._ent_cols("p")}

            ),

            prev as (

                {{% if is_incremental() %}}
                select * from {{{{ this }}}}
                {{% else %}}
                -- First build: same shape, no rows, so the merge below is the only
                -- projection in the model and cannot diverge between branches.
                select * from new_days where 1 = 0
                {{% endif %}}

            )

            select
                {self._ent_cols("n")},

            """).rstrip("\n")
            + "\n"
            + self._field_sections(merge_items)
            + textwrap.dedent(f"""

                {{{{ fs_least2('prev._min_event_date', 'n._min_event_date') }}}} as _min_event_date,
                {{{{ fs_greatest2('prev._max_event_date', 'n._max_event_date') }}}} as _max_event_date,
                {self._offset(late)} as _state_as_of_date,
                '{self.hash}' as _spec_version
            from new_days n
            left join prev on {self._ent_join("n", "prev")}
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

                min(p.event_date) as _min_event_date,
                max(p.event_date) as _max_event_date
            from {{{{ ref('{self.m_partials}') }}}} p
            cross join watermark w
            where p.event_date > w.wm
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
        if has_bounded:
            ctes.append(("rollup", self.m_rollup))
            spine_parts.append(f"select {self._ent_cols()} from rollup")
            joins.append(f"left join rollup on {self._ent_join('spine', 'rollup')}")
        if has_at:
            ctes.append(("at_state", self.m_state))
            spine_parts.append(f"select {self._ent_cols()} from at_state")
            joins.append(f"left join at_state on {self._ent_join('spine', 'at_state')}")
        if self.has_recent:
            ctes.append(("at_recent", self.m_recent))
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
                expr = Expr.sql(f"rollup.{f.name}")
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
            line = (
                f"{item.expr.render()} as {item.name}"
                if isinstance(item, ComputedFeature)
                else item.name
            )
            out_items.append((group, line))

        L: list[str] = [_banner(self.spec, "mart / published feature table").rstrip("\n"), ""]
        L += [
            f"-- One row per entity per as-of date, {self.plan.feature_count} feature columns wide.",
            "--",
            "-- POINT-IN-TIME SAFETY. Nothing here can see past target_date: the partial",
            "-- layer refuses events after it and every window is bounded above by it, so",
            "-- joining this table to labels on target_date is leak-free by construction.",
            "",
            "{{ config(",
            f"    materialized='{self.s.materialized_mart}',",
            "    incremental_strategy=fs_partition_replace_strategy(),",
            "    unique_key=['target_date'],",
            "    partition_by=fs_partition_config(['target_date']),",
            "    cluster_by=fs_cluster_config(['target_date']),",
            "    on_schema_change='sync_all_columns',",
            f"    tags=['feature_store', '{self.fn}', 'mart']",
            ") }}",
            "",
            "with",
            "",
        ]
        for alias, model in ctes:
            L += [f"{alias} as (", "", f"    select * from {{{{ ref('{model}') }}}}", "", "),", ""]
        L += ["spine as ("]
        L += ["", *[f"    {n}" for n in spine_note], *[f"    {b}" for b in spine_body], ""]
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
            f"    {self._ent_cols()},",
            "",
        ]
        L += [self._field_sections(out_items, indent="    ")]
        L += [
            "",
            "    _first_event_date,",
            "    _last_event_date,",
            "    {{ dbt.current_timestamp() }} as _generated_at,",
            f"    '{self.hash}' as _spec_version",
            "from joined",
        ]
        return "\n".join(L) + "\n"

    # ======================================================================
    # 7. schema.yml -- every generated column documented
    # ======================================================================
    def render_schema_yml(self) -> str:
        import yaml as _yaml

        def col(name: str, desc: str, tests: list | None = None) -> dict:
            d: dict = {"name": name, "description": desc}
            if tests:
                d["data_tests"] = tests
            return d

        mart_cols = [
            col(
                "target_date",
                "As-of date these features describe. Every value is computed from events "
                "at or before this date, so joining labels on it cannot leak the future.",
                ["not_null"],
            )
        ]
        for e in self.ents:
            mart_cols.append(col(e, f"Entity key ({e}).", ["not_null"]))
        for item in self.plan.outputs():
            mart_cols.append(col(item.name, item.description))
        mart_cols += [
            col("_first_event_date", "Earliest event_date ever seen for this entity."),
            col(
                "_last_event_date",
                "Latest event_date seen at or before target_date. Use it to tell a genuine "
                "zero from a dormant entity.",
            ),
            col("_generated_at", "Wall-clock time this row was materialised."),
            col(
                "_spec_version",
                f"Fingerprint of the spec that produced this row ({self.hash}). A change "
                "here explains a change in feature semantics.",
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
                "data_tests": [
                    {
                        "fs_unique_combination": {
                            "arguments": {"combination_of_columns": ["target_date", *self.ents]}
                        }
                    }
                ],
                "columns": mart_cols,
            },
            {
                "name": self.m_partials,
                "description": (
                    "Reusable per-entity, per-event_date partial aggregate state. Composable "
                    "by construction: every published window is a fold over these rows."
                ),
                "data_tests": [
                    {
                        "fs_unique_combination": {
                            "arguments": {"combination_of_columns": [*self.ents, "event_date"]}
                        }
                    }
                ],
                "columns": [
                    col(
                        "event_date",
                        "Calendar date of the events summarised by this row.",
                        ["not_null"],
                    ),
                    *[col(e, f"Entity key ({e}).", ["not_null"]) for e in self.ents],
                ],
            },
        ]
        if self.spec.has_all_time:
            models.append(
                {
                    "name": self.m_state,
                    "description": (
                        "Sealed all_time accumulator, one row per entity. Advances only over "
                        "event_dates old enough to be immune to late arrivals."
                    ),
                    "data_tests": [
                        {
                            "fs_unique_combination": {
                                "arguments": {"combination_of_columns": list(self.ents)}
                            }
                        }
                    ],
                    "columns": [
                        *[col(e, f"Entity key ({e}).", ["not_null"]) for e in self.ents],
                        col(
                            "_state_as_of_date",
                            "Watermark: every event_date at or before this has been folded in. "
                            "Drives the self-healing, idempotent catch-up range.",
                            ["not_null"],
                        ),
                    ],
                }
            )

        header = (
            "# ============================================================================\n"
            "# GENERATED FILE - DO NOT EDIT BY HAND\n"
            f"#   feature   : {self.fn}\n"
            f"#   spec hash : {self.spec.spec_hash}\n"
            "#   Edit the spec and run `make generate`.\n"
            "# ============================================================================\n"
        )
        return header + _yaml.safe_dump(
            {"version": 2, "models": models}, sort_keys=False, width=100, allow_unicode=True
        )

    # ======================================================================
    # 8. generated invariant tests
    # ======================================================================
    # These are worth more than the sum of their parts: they do not check that
    # the SQL runs, they check that the algebra held on real data. A broken
    # merge, a mis-signed window bound or a leaked future event all surface
    # here as an arithmetic contradiction rather than as a silently wrong
    # number that ships into a model.

    def _violation_test(self, name: str, purpose: str, checks: list[tuple[str, str]]) -> str:
        if not checks:
            terms = "        '' as violations"
        else:
            terms = (
                " ||\n".join(
                    f"        coalesce(case when {cond} then '{label};' end, '')"
                    for cond, label in checks
                )
                + " || '' as violations"
            )
        return (
            _banner(self.spec, f"test / {purpose}")
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

            select target_date, {self._ent_cols()}, violations
            from checked
            where violations <> ''
            """).rstrip()
            + "\n"
        )

    def _ordered_windows(self) -> list[TimeWindow]:
        return sorted(self.spec.windows, key=lambda w: (1, 0) if w.is_all_time else (0, w.days))

    def _feature_index(self) -> dict[tuple[str, str, str, str], str]:
        """(field, agg, combo_label, window) -> feature name."""
        idx: dict[tuple[str, str, str, str], str] = {}
        for f in self.plan.stored:
            if not f.internal:
                idx[(f.field, f.agg_key, f.combo.label, f.window.name)] = f.name
        for c in self.plan.computed:
            idx[(c.field, c.agg_key, c.combo.label, c.window.name)] = c.name
        return idx

    def render_test_non_negative(self) -> str:
        checks = []
        for item in self.plan.outputs():
            agg = item.agg_key
            if agg in ("count", "count_distinct"):
                checks.append((f"{item.name} < 0", f"{item.name}<0"))
        return self._violation_test("non_negative", "counts are never negative", checks)

    def render_test_window_monotonicity(self) -> str:
        windows = self._ordered_windows()
        idx = self._feature_index()
        approx_fields = {f.name for f in self.spec.fields if f.distinct_method == "approx"}
        # Direction of the inequality as a window widens. A wider window is a
        # superset of a narrower one, so counts can only grow, minima can only
        # fall and maxima can only rise. days_since inherits min/max direction
        # because it is an affine, decreasing function of event time.
        grows = {"count", "count_distinct", "sum", "max"}
        checks = []
        seen: set[tuple[str, str, str]] = set()
        for field, agg, label, _win in idx:
            key = (field, agg, label)
            if key in seen:
                continue
            seen.add(key)
            if agg == "count_distinct" and field in approx_fields:
                continue  # sketch error can legitimately invert by a few counts
            for a, b in zip(windows, windows[1:], strict=False):
                na, nb = idx.get((field, agg, label, a.name)), idx.get((field, agg, label, b.name))
                if not na or not nb:
                    continue
                op = ">" if agg in grows else "<"
                checks.append((f"{na} {op} {nb}", f"{na}!{op}{b.name}"))
                if agg in ("min", "max"):
                    checks.append((f"{na} is not null and {nb} is null", f"{nb}_null_but_{na}_set"))
        return self._violation_test("window_monotonicity", "nested windows stay ordered", checks)

    def render_test_internal_coherence(self) -> str:
        idx = self._feature_index()
        checks = []
        # A condition combo selects a subset of the rows the marginal sees, so
        # a marginal count dominates every combo built from it.
        for (field, agg, label, win), name in idx.items():
            if agg not in ("count", "count_distinct") or label == "":
                continue
            marginal = idx.get((field, agg, "", win))
            if marginal:
                checks.append((f"{marginal} < {name}", f"{marginal}<{name}"))
        # min can never exceed max over the same rows.
        for (field, agg, label, win), name in idx.items():
            if agg != "min":
                continue
            mx = idx.get((field, "max", label, win))
            if mx:
                checks.append((f"{name} > {mx}", f"{name}>{mx}"))

        # A distinct count can never exceed the row count it is drawn from.
        # Worth stating explicitly: it is the invariant that distinguishes a
        # genuine sketch error from a NULL or a duplicate leaking into the
        # retained key set, and it holds for the approximate path too because
        # a KMV sketch below k is exact.
        approx = {f.name for f in self.spec.fields if f.distinct_method == "approx"}
        for (field, agg, label, win), name in idx.items():
            if agg != "count_distinct" or field in approx:
                continue
            total = idx.get((field, "count", label, win))
            if total:
                checks.append((f"{name} > {total}", f"{name}>{total}"))

        return self._violation_test(
            "internal_coherence",
            "marginals dominate, min <= max, distinct <= count",
            checks,
        )

    def render_test_state_watermark(self) -> str:
        late = self.s.late_arrival_days
        return (
            _banner(self.spec, "test / all_time state is usable for this as-of date")
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
            --   dbt run --full-refresh --select {self.m_state} --vars 'target_date: <date>'

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

    # ======================================================================
    # 9. write everything
    # ======================================================================
    def render_all(self, root: Path) -> list[RenderedFile]:
        models = root / "models"
        tests = root / "tests"
        out = [
            RenderedFile(models / "staging" / f"{self.m_stg}.sql", self.render_staging()),
            RenderedFile(
                models / "intermediate" / f"{self.m_partials}.sql", self.render_partials()
            ),
        ]
        if self.spec.bounded_windows:
            out.append(
                RenderedFile(models / "intermediate" / f"{self.m_rollup}.sql", self.render_rollup())
            )
        if self.spec.has_all_time:
            out.append(
                RenderedFile(
                    models / "intermediate" / f"{self.m_state}.sql", self.render_alltime_state()
                )
            )
        if self.has_recent:
            out.append(
                RenderedFile(
                    models / "intermediate" / f"{self.m_recent}.sql", self.render_alltime_recent()
                )
            )
        out += [
            RenderedFile(models / "marts" / f"{self.m_mart}.sql", self.render_mart()),
            RenderedFile(models / "marts" / f"_{self.fn}__models.yml", self.render_schema_yml()),
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
        ]
        if self.spec.has_all_time:
            out.append(
                RenderedFile(
                    tests / f"assert_{self.fn}_state_watermark.sql",
                    self.render_test_state_watermark(),
                )
            )
        return out
