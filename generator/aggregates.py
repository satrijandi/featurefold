"""The aggregations a spec can name, and everything the compiler knows about them.

Every stored aggregation is a commutative monoid over a partial state:

    partial   : source rows   -> state   (one state per entity, per event_date)
    state_agg : many states   -> state   (roll a range of days up)
    merge     : state, state  -> state   (fold sealed history into a fresh tail)
    finalize  : state         -> value   (what the mart publishes)

Two consequences are worth stating, because they are why the design holds
together:

1. A bounded window is exactly `finalize(state_agg(days in window))`, and
   all_time is exactly `finalize(merge(sealed_state, state_agg(unsealed
   tail)))`. There is no second code path, so `l30d` and `all_time` cannot
   drift apart -- they are the same fold over different ranges.

2. An aggregation that is not a monoid on its own (`avg`, `days_since`) is a
   Composite: it names the monoids it is rebuilt from and how to combine their
   published values, and the plan carries those monoids as internal columns.

Each aggregation also declares the facts that spec validation, the generated
invariant tests and the registry need: which field types it accepts, which way
it moves when it covers more rows, what bounds it, and whether it can be NULL.
Adding an aggregation is therefore a change to this module only.
"""

from __future__ import annotations

from abc import ABC, abstractmethod

from generator.expr import Expr, compose, macro

# Logical field types an arithmetic aggregation accepts.
NUMERIC_TYPES = frozenset({"double", "bigint"})


def guard(value_sql: str, predicate: str) -> Expr:
    """Restrict a value to the rows a condition combo selects.

    The all-defaults combo carries the predicate TRUE, so it emits the bare
    column and the marginal (uncut) features stay readable in the generated SQL.
    """
    if predicate.strip().upper() == "TRUE":
        return Expr.sql(value_sql)
    return Expr.sql(f"case when {predicate} then {value_sql} end")


class Aggregation(ABC):
    """One entry of a spec's `agg:` list."""

    key: str
    #: Which way the published value moves when it covers a superset of rows: a
    #: wider window, or the marginal against a condition combo. +1 never falls,
    #: -1 never rises, 0 promises nothing. The generated invariant tests check
    #: exactly this, so it must be a guarantee, not a tendency.
    rows_order: int = 0
    #: True when no input can make the published value negative.
    non_negative: bool = False
    #: Key of an aggregation over the same field and rows that can never be
    #: smaller than this one (min <= max, distinct <= count).
    bounded_by: str | None = None
    #: True when the field must declare a type, because the feature's column
    #: type is the field's.
    requires_field_type: bool = False
    #: True when the aggregation is arithmetic over the value.
    numeric_only: bool = False

    @property
    @abstractmethod
    def nullable(self) -> bool:
        """True when an entity with no matching rows publishes NULL."""

    @abstractmethod
    def feature_dtype(self, field_dtype: str | None) -> str: ...

    def accepts(self, field_dtype: str | None) -> str | None:
        """Why a field of `field_dtype` cannot use this aggregation, or None."""
        if self.requires_field_type and not field_dtype:
            return "needs a declared type"
        if self.numeric_only and field_dtype not in NUMERIC_TYPES:
            return "needs a numeric field"
        return None


class Monoid(Aggregation):
    """An aggregation stored as composable per-day state."""

    #: True when an entity with no matching rows should publish 0 rather than
    #: NULL. Counts are zero-filled; extrema are not, because "never happened"
    #: and "happened at time zero" are different facts.
    zero_filled: bool = False

    @property
    def nullable(self) -> bool:
        return not self.zero_filled

    @abstractmethod
    def partial_expr(self, value_sql: str, predicate: str) -> Expr: ...

    @abstractmethod
    def state_agg_expr(self, col: Expr) -> Expr: ...

    @abstractmethod
    def merge_expr(self, a: Expr, b: Expr) -> Expr: ...

    def finalize_expr(self, state: Expr) -> Expr:
        return state

    @abstractmethod
    def partial_dtype(self, field_dtype: str | None) -> str: ...

    def window_expr(self, col: str, window_predicate: str | None) -> Expr:
        """Bounded-window value: finalize the fold over states inside the window."""
        scoped = (
            Expr.sql(col)
            if window_predicate is None
            else Expr.sql(f"case when {window_predicate} then {col} end")
        )
        return self.finalize_expr(self.state_agg_expr(scoped))


class Composite(Aggregation):
    """An aggregation rebuilt in the mart from published monoid values."""

    #: Recorded in the registry as `computed_kind`.
    kind: str

    @property
    def nullable(self) -> bool:
        return True

    @property
    @abstractmethod
    def parts(self) -> tuple[Monoid, ...]:
        """The monoids carried as internal columns, in the order publish_expr takes them."""

    @abstractmethod
    def publish_expr(self, *parts: Expr) -> Expr:
        """The published value, given each part's finished window value."""

    def describe_suffix(self, inputs: tuple[str, ...]) -> str:
        return ""


# --------------------------------------------------------------------------- #
# Monoids
# --------------------------------------------------------------------------- #


class Count(Monoid):
    key = "count"
    zero_filled = True
    rows_order = 1
    non_negative = True

    def partial_expr(self, value_sql: str, predicate: str) -> Expr:
        return compose("count({0})", guard(value_sql, predicate))

    def state_agg_expr(self, col: Expr) -> Expr:
        return compose("sum({0})", col)

    def merge_expr(self, a: Expr, b: Expr) -> Expr:
        return compose("(coalesce({0}, 0) + coalesce({1}, 0))", a, b)

    def finalize_expr(self, state: Expr) -> Expr:
        return compose("coalesce({0}, 0)", state)

    def partial_dtype(self, field_dtype: str | None) -> str:
        return "bigint"

    def feature_dtype(self, field_dtype: str | None) -> str:
        return "bigint"


class Sum(Monoid):
    """Zero-filled, but unordered: a signed field can make a wider window smaller."""

    key = "sum"
    zero_filled = True
    requires_field_type = True
    numeric_only = True

    def partial_expr(self, value_sql: str, predicate: str) -> Expr:
        return compose("sum({0})", guard(value_sql, predicate))

    def state_agg_expr(self, col: Expr) -> Expr:
        return compose("sum({0})", col)

    def merge_expr(self, a: Expr, b: Expr) -> Expr:
        return compose("(coalesce({0}, 0) + coalesce({1}, 0))", a, b)

    def finalize_expr(self, state: Expr) -> Expr:
        return compose("coalesce({0}, 0)", state)

    def partial_dtype(self, field_dtype: str | None) -> str:
        return field_dtype or "double"

    def feature_dtype(self, field_dtype: str | None) -> str:
        return field_dtype or "double"


class _Extremum(Monoid):
    sql_fn: str
    merge_macro: str
    requires_field_type = True

    def partial_expr(self, value_sql: str, predicate: str) -> Expr:
        return compose(f"{self.sql_fn}({{0}})", guard(value_sql, predicate))

    def state_agg_expr(self, col: Expr) -> Expr:
        return compose(f"{self.sql_fn}({{0}})", col)

    def merge_expr(self, a: Expr, b: Expr) -> Expr:
        # LEAST/GREATEST disagree on NULL across engines, and here NULL means
        # "no data yet" rather than "unknown", so it must not propagate.
        return macro(self.merge_macro, a, b)

    def partial_dtype(self, field_dtype: str | None) -> str:
        return field_dtype or "double"

    def feature_dtype(self, field_dtype: str | None) -> str:
        return field_dtype or "double"


class Min(_Extremum):
    key = "min"
    sql_fn = "min"
    merge_macro = "fs_least2"
    rows_order = -1
    bounded_by = "max"


class Max(_Extremum):
    key = "max"
    sql_fn = "max"
    merge_macro = "fs_greatest2"
    rows_order = 1


class CountDistinctExact(Monoid):
    """Exact distinct via a retained key set.

    State is the actual set of distinct values, cast to varchar so the element
    type is identical on every engine. Sound for fields whose per-entity
    cardinality is small (device models, merchant categories);
    `distinct_method: approx` covers the rest. A generated dbt test watches the
    realised set sizes, so switching a field to approx is an evidence-driven
    decision rather than a guess.
    """

    key = "count_distinct"
    zero_filled = True
    rows_order = 1
    non_negative = True
    # The invariant that distinguishes a genuine sketch error from a NULL or a
    # duplicate leaking into the retained key set.
    bounded_by = "count"

    def partial_expr(self, value_sql: str, predicate: str) -> Expr:
        return macro("fs_collect_set", compose("cast({0} as varchar)", guard(value_sql, predicate)))

    def state_agg_expr(self, col: Expr) -> Expr:
        return macro("fs_array_union_agg", col)

    def merge_expr(self, a: Expr, b: Expr) -> Expr:
        return macro("fs_array_union2", a, b)

    def finalize_expr(self, state: Expr) -> Expr:
        return macro("fs_array_size", state)

    def partial_dtype(self, field_dtype: str | None) -> str:
        return "array<varchar>"

    def feature_dtype(self, field_dtype: str | None) -> str:
        return "bigint"


class CountDistinctApprox(Monoid):
    """Approximate distinct via a mergeable KMV sketch. See macros/fs_kmv.sql.

    `k` is emitted explicitly at every call site rather than read from a dbt
    var, so two specs in the same project can choose different accuracy/size
    trade-offs and neither can be silently changed by a run-time flag.

    The estimate is ordered but not bounded. A superset of rows has a k-th
    smallest hash no larger than its subset's, so `(k-1) / m_k` cannot fall as
    rows are added, and below k the sketch is exact. It can, however, exceed
    the true row count above k, so it carries no `bounded_by`.
    """

    key = "count_distinct"
    zero_filled = True
    rows_order = 1
    non_negative = True

    def __init__(self, k: int = 256) -> None:
        self.k = k

    def partial_expr(self, value_sql: str, predicate: str) -> Expr:
        return macro("fs_kmv_build", guard(value_sql, predicate), str(self.k))

    def state_agg_expr(self, col: Expr) -> Expr:
        return macro("fs_kmv_union_agg", col, str(self.k))

    def merge_expr(self, a: Expr, b: Expr) -> Expr:
        return macro("fs_kmv_merge2", a, b, str(self.k))

    def finalize_expr(self, state: Expr) -> Expr:
        return macro("fs_kmv_estimate", state, str(self.k))

    def partial_dtype(self, field_dtype: str | None) -> str:
        return "array<double>"

    def feature_dtype(self, field_dtype: str | None) -> str:
        return "bigint"


# --------------------------------------------------------------------------- #
# Composites
# --------------------------------------------------------------------------- #


class Avg(Composite):
    """A ratio of two monoids, divided at publish time.

    Unordered and unbounded on purpose: the mean of a wider window can sit on
    either side of a narrower one's, and floating-point division can put an
    average of identical values a hair above their maximum.
    """

    key = "avg"
    kind = "avg"
    requires_field_type = True
    numeric_only = True

    @property
    def parts(self) -> tuple[Monoid, ...]:
        return (Sum(), Count())

    def publish_expr(self, *parts: Expr) -> Expr:
        total, n = parts
        return compose("case when {1} = 0 then null else {0} / cast({1} as double) end", total, n)

    def feature_dtype(self, field_dtype: str | None) -> str:
        return "double"


class DaysSince(Composite):
    """Whole days from an event timestamp to the as-of date, for `derived: days_since`.

    days_since is monotonically decreasing in event time, so its minimum over a
    window is the distance to the LATEST event and its maximum is the distance
    to the EARLIEST. Aggregating the underlying timestamp and flipping here
    keeps the partial layer independent of the as-of date, which is the whole
    reason partials can be reused across runs.

    The flip also fixes the ordering: min(days_since) moves like min, max like
    max. And no event after the as-of date ever reaches a fold, so the value is
    never negative -- a negative one means the future leaked in.
    """

    kind = "days_since"
    KEYS = ("min", "max")
    non_negative = True

    def __init__(self, key: str) -> None:
        if key not in self.KEYS:
            raise KeyError(f"days_since supports {list(self.KEYS)}, not {key!r}")
        self.key = key
        self._timestamp_fold: Monoid = Max() if key == "min" else Min()
        self.rows_order = -1 if key == "min" else 1
        self.bounded_by = "max" if key == "min" else None

    @property
    def parts(self) -> tuple[Monoid, ...]:
        return (self._timestamp_fold,)

    def publish_expr(self, *parts: Expr) -> Expr:
        (timestamp,) = parts
        return macro("fs_datediff_day", timestamp, Expr.jinja("fs_target_date()"))

    def feature_dtype(self, field_dtype: str | None) -> str:
        return "bigint"

    def describe_suffix(self, inputs: tuple[str, ...]) -> str:
        return f" (whole calendar days from {inputs[0]} to the as-of date)"


# --------------------------------------------------------------------------- #
# Resolution
# --------------------------------------------------------------------------- #

_REGISTRY: dict[str, type[Aggregation]] = {
    "count": Count,
    "sum": Sum,
    "min": Min,
    "max": Max,
    "avg": Avg,
}

#: Every key a spec may name under `agg:`.
AGG_KEYS = frozenset({*_REGISTRY, "count_distinct"})


def get_aggregate(
    agg: str,
    distinct_method: str = "exact",
    kmv_k: int = 256,
    *,
    days_since: bool = False,
) -> Aggregation:
    """The aggregation a field's `agg:` entry names.

    `days_since` is set for a field declared (or detected) as
    `derived: days_since`, whose aggregations are rebuilt from a timestamp.
    """
    if days_since:
        return DaysSince(agg)
    if agg == "count_distinct":
        return CountDistinctApprox(k=kmv_k) if distinct_method == "approx" else CountDistinctExact()
    try:
        return _REGISTRY[agg]()
    except KeyError:
        raise KeyError(f"no aggregate implementation for {agg!r}") from None
