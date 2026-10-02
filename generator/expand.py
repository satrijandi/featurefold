"""Expand a validated spec into a concrete plan of partials and feature columns.

This is where the combinatorics happen. For each atomic field, the applied
condition categories are crossed with each other, then with the aggregations,
then with the time windows. Because every category carries a `default: TRUE`
member, the marginal (uncut) features fall out of the same cross-product rather
than needing a special case.

The plan distinguishes two kinds of output column:

  StoredFeature    folds up through the monoid from stored daily partials.
  ComputedFeature  is reconstructed in the mart from other columns, because its
                   aggregation is a Composite (see generator/aggregates.py).

A partial that exists only to feed a ComputedFeature is marked internal: it is
carried through the intermediate models but never published to the mart.
"""

from __future__ import annotations

import itertools
from dataclasses import dataclass
from dataclasses import field as dc_field

from generator.aggregates import Aggregation, Composite, Monoid
from generator.expr import Expr
from generator.spec import AtomicField, FeatureSpec, SpecError, TimeWindow

MAX_IDENTIFIER_LEN = 255
WARN_IDENTIFIER_LEN = 128


def _join(sep: str, *parts: str) -> str:
    return sep.join(p for p in parts if p)


@dataclass(frozen=True)
class Combo:
    """One point in the cross-product of a field's applied condition categories."""

    members: tuple[tuple[str, str], ...]  # (category_name, member_name)
    label: str  # default members dropped; "" for the marginal
    predicate: str  # "TRUE" for the marginal

    @property
    def is_marginal(self) -> bool:
        return self.label == ""

    def describe(self) -> str:
        if self.is_marginal:
            return "all rows"
        return ", ".join(f"{cat}={mem}" for cat, mem in self.members)


@dataclass
class PartialColumn:
    """One column of the reusable per-entity, per-event_date partial state."""

    name: str
    dtype: str
    agg_key: str
    field: str
    combo: Combo
    value_sql: str
    aggregate: Monoid
    internal: bool = True

    @property
    def partial_sql(self) -> Expr:
        return self.aggregate.partial_expr(self.value_sql, self.combo.predicate)


@dataclass
class StoredFeature:
    name: str
    partial: PartialColumn
    window: TimeWindow
    dtype: str
    description: str
    internal: bool = True

    @property
    def agg_key(self) -> str:
        return self.partial.agg_key

    @property
    def field(self) -> str:
        return self.partial.field

    @property
    def combo(self) -> Combo:
        return self.partial.combo

    @property
    def aggregation(self) -> Aggregation:
        return self.partial.aggregate


@dataclass
class ComputedFeature:
    name: str
    window: TimeWindow
    dtype: str
    expr: Expr
    inputs: tuple[str, ...]
    description: str
    aggregation: Composite
    field: str
    combo: Combo

    @property
    def agg_key(self) -> str:
        return self.aggregation.key


@dataclass
class FeaturePlan:
    spec: FeatureSpec
    partials: list[PartialColumn] = dc_field(default_factory=list)
    stored: list[StoredFeature] = dc_field(default_factory=list)
    computed: list[ComputedFeature] = dc_field(default_factory=list)
    output_order: list[str] = dc_field(default_factory=list)

    @property
    def public_stored(self) -> list[StoredFeature]:
        return [s for s in self.stored if not s.internal]

    @property
    def feature_count(self) -> int:
        return len(self.output_order)

    def outputs(self) -> list[StoredFeature | ComputedFeature]:
        """Public feature columns, in the order the spec implies."""
        by_name: dict[str, StoredFeature | ComputedFeature] = {}
        for s in self.stored:
            if not s.internal:
                by_name[s.name] = s
        for c in self.computed:
            by_name[c.name] = c
        return [by_name[n] for n in self.output_order]


def build_combos(spec: FeatureSpec, field: AtomicField, sep: str) -> list[Combo]:
    """Cross-product of the field's applied categories, declaration order preserved."""
    if not field.apply_cond_cat:
        return [Combo(members=(), label="", predicate="TRUE")]

    member_lists = [spec.categories[c].members for c in field.apply_cond_cat]
    combos: list[Combo] = []
    for picked in itertools.product(*member_lists):
        members = tuple((cat, m.name) for cat, m in zip(field.apply_cond_cat, picked, strict=True))
        non_default = [m for m in picked if not m.is_default]
        label = _join(sep, *[m.name for m in non_default])
        predicate = "TRUE" if not non_default else " and ".join(f"({m.sql})" for m in non_default)
        combos.append(Combo(members=members, label=label, predicate=predicate))
    return combos


class _Namer:
    """Allocates identifiers and refuses to let two features share a name."""

    def __init__(self, sep: str) -> None:
        self.sep = sep
        self._seen: dict[str, str] = {}
        self.warnings: list[str] = []

    def claim(self, name: str, origin: str) -> str:
        if name in self._seen:
            raise SpecError(
                f"generated name collision on {name!r}:\n"
                f"    already produced by: {self._seen[name]}\n"
                f"    also produced by:    {origin}\n"
                "Rename a condition member or an atomic field so the expansion stays unique."
            )
        if len(name) > MAX_IDENTIFIER_LEN:
            raise SpecError(
                f"generated column {name!r} is {len(name)} characters, over the "
                f"{MAX_IDENTIFIER_LEN}-character limit enforced by Snowflake. Shorten the "
                "condition member names it is built from."
            )
        if len(name) > WARN_IDENTIFIER_LEN:
            self.warnings.append(
                f"column {name!r} is {len(name)} characters; consider shorter member names"
            )
        self._seen[name] = origin
        return name


def _describe(agg: str, field: str, combo: Combo, window: TimeWindow, convention: str) -> str:
    if window.is_all_time:
        span = "over all history up to and including the as-of date"
    elif convention == "inclusive":
        span = f"over the {window.days} days ending on the as-of date (inclusive)"
    else:
        span = f"over the {window.days} days ending the day before the as-of date"
    return f"{agg} of {field} for {combo.describe()} {span}"


def build_plan(spec: FeatureSpec) -> FeaturePlan:
    sep = spec.settings.separator
    convention = spec.settings.window_convention
    plan = FeaturePlan(spec=spec)
    namer = _Namer(sep)

    partials: dict[str, PartialColumn] = {}
    stored: dict[str, StoredFeature] = {}

    def ensure_partial(
        field_name: str,
        agg: Monoid,
        combo: Combo,
        dtype_source: str | None,
        public: bool,
    ) -> PartialColumn:
        name = _join(sep, "p", agg.key, field_name, combo.label)
        existing = partials.get(name)
        if existing is not None:
            if public:
                existing.internal = False
            return existing
        namer.claim(name, f"partial for {agg.key}({field_name}) [{combo.describe()}]")
        col = PartialColumn(
            name=name,
            dtype=agg.partial_dtype(dtype_source),
            agg_key=agg.key,
            field=field_name,
            combo=combo,
            value_sql=field_name,
            aggregate=agg,
            internal=not public,
        )
        partials[name] = col
        plan.partials.append(col)
        return col

    def ensure_stored(
        partial: PartialColumn,
        window: TimeWindow,
        dtype_source: str | None,
        public: bool,
    ) -> StoredFeature:
        name = _join(sep, partial.agg_key, partial.field, partial.combo.label, window.name)
        existing = stored.get(name)
        if existing is not None:
            if public:
                existing.internal = False
            return existing
        namer.claim(
            name, f"{partial.agg_key}({partial.field}) [{partial.combo.describe()}] {window.name}"
        )
        feat = StoredFeature(
            name=name,
            partial=partial,
            window=window,
            dtype=partial.aggregate.feature_dtype(dtype_source),
            description=_describe(
                partial.agg_key, partial.field, partial.combo, window, convention
            ),
            internal=not public,
        )
        stored[name] = feat
        plan.stored.append(feat)
        return feat

    by_name = {f.name: f for f in spec.fields}

    for field in spec.fields:
        combos = build_combos(spec, field, sep)

        # A composite is folded from monoids over the field it is rebuilt from:
        # the field itself for avg, the underlying timestamp for days_since.
        if field.is_derived:
            source = field.derived.from_field
            base_field = by_name.get(source)
            source_dtype = base_field.field_type if base_field else "timestamp"
        else:
            source, source_dtype = field.name, field.field_type

        for agg in field.aggregations(spec.settings.kmv_k):
            for combo in combos:
                if isinstance(agg, Monoid):
                    partial = ensure_partial(field.name, agg, combo, field.field_type, public=True)
                    for window in spec.windows:
                        feat = ensure_stored(partial, window, field.field_type, public=True)
                        plan.output_order.append(feat.name)
                    continue

                parts = [
                    ensure_partial(source, part, combo, source_dtype, public=False)
                    for part in agg.parts
                ]
                for window in spec.windows:
                    inputs = tuple(
                        ensure_stored(p, window, source_dtype, public=False).name for p in parts
                    )
                    name = namer.claim(
                        _join(sep, agg.key, field.name, combo.label, window.name),
                        f"{agg.key}({field.name}) {agg.kind} [{combo.describe()}] {window.name}",
                    )
                    plan.computed.append(
                        ComputedFeature(
                            name=name,
                            window=window,
                            dtype=agg.feature_dtype(field.field_type),
                            expr=agg.publish_expr(*(Expr.sql(i) for i in inputs)),
                            inputs=inputs,
                            description=_describe(agg.key, field.name, combo, window, convention)
                            + agg.describe_suffix(inputs),
                            aggregation=agg,
                            field=field.name,
                            combo=combo,
                        )
                    )
                    plan.output_order.append(name)

    plan.warnings = namer.warnings  # type: ignore[attr-defined]
    return plan
