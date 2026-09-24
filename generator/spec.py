"""Parse and validate a feature spec YAML into a typed, checked model.

Everything that can be wrong with a spec should be caught here, loudly, before
any SQL is generated. A feature store that silently produces plausible but
time-leaking numbers is far worse than one that refuses to compile.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from dataclasses import field as dc_field
from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError

# Aggregations whose partial state is a plain scalar carried straight through.
SCALAR_AGGS = {"count", "sum", "min", "max"}
DISTINCT_AGGS = {"count_distinct"}
SUPPORTED_AGGS = SCALAR_AGGS | DISTINCT_AGGS | {"avg"}

# Aggregations that need to know the field's data type to emit a column type.
TYPED_AGGS = {"min", "max", "sum", "avg"}

FIELD_TYPE_MAP = {
    "numeric": "double",
    "number": "double",
    "double": "double",
    "float": "double",
    "int": "bigint",
    "integer": "bigint",
    "bigint": "bigint",
    "timestamp": "timestamp",
    "datetime": "timestamp",
    "date": "date",
    "string": "varchar",
    "varchar": "varchar",
    "boolean": "boolean",
}

WINDOW_RE = re.compile(r"^l(\d+)d$", re.IGNORECASE)
IDENT_RE = re.compile(r"^[a-z_][a-z0-9_]*$")
TARGET_DATE_RE = re.compile(r"\{\{\s*target_date\s*\}\}")

# Recognised shapes for "days between this event and the as-of date".
# Matching one of these lets a target_date-dependent column stay reusable,
# because the value is reconstructed at scoring time from a stored timestamp.
DAYS_SINCE_PATTERNS = [
    re.compile(
        r"""datediff \s* \( \s* day \s* , \s*
            (?P<from>.+?) \s* , \s*
            '? \{\{ \s* target_date \s* \}\} '? \s* (?: :: \s* date | \s )? \s* \)""",
        re.IGNORECASE | re.VERBOSE | re.DOTALL,
    ),
    re.compile(
        r"""date_diff \s* \( \s* 'day' \s* , \s*
            (?P<from>.+?) \s* , \s*
            '? \{\{ \s* target_date \s* \}\} '? \s* (?: :: \s* date | \s )? \s* \)""",
        re.IGNORECASE | re.VERBOSE | re.DOTALL,
    ),
]


class SpecError(ValueError):
    """Raised for any invalid feature spec. Message is aimed at the spec author."""


# --------------------------------------------------------------------------- #
# Typed model
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class ConditionMember:
    name: str
    sql: str

    @property
    def is_default(self) -> bool:
        return self.sql.strip().upper() == "TRUE"


@dataclass(frozen=True)
class ConditionCategory:
    name: str
    members: tuple[ConditionMember, ...]


@dataclass(frozen=True)
class SourceRelation:
    """Maps a hard-coded relation in the `source` SQL onto a dbt source().

    Without this the source SQL names a physical table that only exists in one
    environment. With it, the same generated model reads a seeded fixture
    locally and the real Unity Catalog / Snowflake table in production, which
    is what makes the local stack a genuine rehearsal rather than a mock.
    """

    literal: str
    source_name: str
    table_name: str

    @property
    def dbt_ref(self) -> str:
        return f"{{{{ source('{self.source_name}', '{self.table_name}') }}}}"


def _relation_re(literals: list[str]) -> re.Pattern[str]:
    """Match any of `literals` as a whole relation name, never as part of a longer one.

    Without the boundaries `bronze.db.events` would match inside
    `bronze.db.events_blocklist` and splice the wrong source() into it. Longest
    first, so of two names where one is a prefix of the other, each position
    resolves to the name actually written there.
    """
    alts = "|".join(re.escape(lit) for lit in sorted(literals, key=len, reverse=True))
    return re.compile(rf"(?<![\w.])(?:{alts})(?![\w])")


def substitute_relations(sql: str, relations: dict[str, SourceRelation]) -> str:
    """Replace every mapped relation name in `sql` with its dbt source()."""
    if not relations:
        return sql
    return _relation_re(list(relations)).sub(lambda m: relations[m.group(0)].dbt_ref, sql)


@dataclass(frozen=True)
class Derivation:
    """A column reconstructed at scoring time rather than stored in partials."""

    kind: str  # currently only "days_since"
    from_field: str  # source alias holding the underlying timestamp
    auto_detected: bool = False


@dataclass
class AtomicField:
    name: str
    apply_cond_cat: tuple[str, ...]
    aggs: tuple[str, ...]
    field_type: str | None = None  # logical type, already normalised
    distinct_method: str = "exact"  # exact | approx
    derived: Derivation | None = None
    description: str = ""

    @property
    def is_derived(self) -> bool:
        return self.derived is not None


@dataclass(frozen=True)
class TimeWindow:
    name: str
    days: int | None  # None for all_time

    @property
    def is_all_time(self) -> bool:
        return self.days is None


class Settings(BaseModel):
    # Strict, so a quoted number or a misspelt key is an error rather than a
    # coercion or a silently applied default.
    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)

    # l7d at target_date T means event_date in [T-6, T] under "inclusive",
    # or [T-7, T-1] under "trailing" (yesterday-and-back, no same-day leakage).
    window_convention: Literal["inclusive", "trailing"] = "inclusive"
    # Which entities get a row on each daily snapshot.
    entity_spine: Literal["all_time", "active_window"] = "all_time"
    # Days of already-seen event_dates each run recomputes, to absorb late
    # arrivals. all_time state seals only up to T - late_arrival_days.
    late_arrival_days: int = Field(default=3, ge=0)
    # k for the KMV sketch backing distinct_method: approx. Below 16 the
    # estimate is too noisy to be usable.
    kmv_k: int = Field(default=256, ge=16)
    # Joins the parts of every generated column name, so it must keep them
    # valid unquoted identifiers.
    separator: str = Field(default="_", pattern=r"^[a-z0-9_]+$")
    source_is_append_only: bool = True
    materialized_mart: Literal["incremental", "table"] = "incremental"


# --------------------------------------------------------------------------- #
# Document shape
#
# These models check only the shape of a spec: which keys exist and what type
# each value has. What the values mean -- whether a field is produced by the
# source, whether a predicate depends on target_date -- is checked by the
# parsers and validate_spec below, which work from the raw mapping.
# --------------------------------------------------------------------------- #

_STRICT = ConfigDict(extra="forbid", strict=True)


class _DerivedDoc(BaseModel):
    model_config = _STRICT
    kind: str
    from_: str = Field(alias="from")


class _AtomicFieldDoc(BaseModel):
    model_config = _STRICT
    apply_cond_cat: list[str] | None = None
    agg: list[str]
    field_type: str | None = None
    distinct_method: str | None = None
    derived: _DerivedDoc | None = None
    description: str | None = None


class _RelationDoc(BaseModel):
    model_config = _STRICT
    source_name: str
    table_name: str


class _SpecDoc(BaseModel):
    model_config = _STRICT
    feature_name: str
    feature_type: str | None = None
    created_by: str | None = None
    description: str | None = None
    source: str
    entities: list[str]
    timestamp_col: str
    condition_cat: list[dict[str, list[dict[str, str]]]] | None = None
    time_cat: list[str]
    atomic_field: list[dict[str, _AtomicFieldDoc]]
    settings: Settings | None = None
    relations: dict[str, _RelationDoc] | None = None


def _shape_error(path: Path, exc: ValidationError) -> SpecError:
    """Restate pydantic's errors in the dotted key paths a spec author writes."""
    lines = []
    for err in exc.errors():
        where = ".".join(str(p) for p in err["loc"] if not isinstance(p, int)) or "<top level>"
        if err["type"] == "extra_forbidden":
            lines.append(
                f"  {where}: unknown key. Check the spelling against "
                "features/examples/every_optional_key.yml."
            )
        elif err["type"] == "missing":
            lines.append(f"  {where}: required key is missing")
        else:
            lines.append(f"  {where}: {err['msg']} (got {err['input']!r})")
    return SpecError(f"{path}: invalid spec\n" + "\n".join(lines))


@dataclass
class FeatureSpec:
    feature_name: str
    feature_type: str
    created_by: str
    description: str
    source_sql: str
    entities: tuple[str, ...]
    timestamp_col: str
    categories: dict[str, ConditionCategory]
    windows: tuple[TimeWindow, ...]
    fields: list[AtomicField]
    settings: Settings
    source_columns: dict[str, str] = dc_field(default_factory=dict)
    parsed_source: ParsedSource | None = None
    relations: dict[str, SourceRelation] = dc_field(default_factory=dict)
    spec_path: Path | None = None
    raw: dict[str, Any] = dc_field(default_factory=dict)

    @property
    def has_all_time(self) -> bool:
        return any(w.is_all_time for w in self.windows)

    @property
    def bounded_windows(self) -> list[TimeWindow]:
        return [w for w in self.windows if not w.is_all_time]

    @property
    def max_window_days(self) -> int:
        return max((w.days for w in self.bounded_windows), default=0)

    @property
    def spec_hash(self) -> str:
        """Stable fingerprint of the spec, stamped onto every generated row."""
        payload = yaml.safe_dump(self.raw, sort_keys=True, default_flow_style=False)
        return hashlib.sha256(payload.encode()).hexdigest()[:12]


# --------------------------------------------------------------------------- #
# Lightweight SQL introspection
# --------------------------------------------------------------------------- #


def _strip_sql_comments(sql: str) -> str:
    sql = re.sub(r"--[^\n]*", "", sql)
    return re.sub(r"/\*.*?\*/", "", sql, flags=re.DOTALL)


def _split_top_level(text: str, sep: str = ",") -> list[str]:
    """Split on `sep` at paren depth 0, ignoring separators inside quotes."""
    parts, buf, depth = [], [], 0
    quote: str | None = None
    i = 0
    while i < len(text):
        ch = text[i]
        if quote:
            buf.append(ch)
            if ch == quote:
                # Doubled quote is an escaped literal quote, not a terminator.
                if i + 1 < len(text) and text[i + 1] == quote:
                    buf.append(text[i + 1])
                    i += 1
                else:
                    quote = None
        elif ch in "'\"":
            quote = ch
            buf.append(ch)
        elif ch in "([":
            depth += 1
            buf.append(ch)
        elif ch in ")]":
            depth -= 1
            buf.append(ch)
        elif ch == sep and depth == 0:
            parts.append("".join(buf))
            buf = []
        else:
            buf.append(ch)
        i += 1
    parts.append("".join(buf))
    return [p.strip() for p in parts if p.strip()]


def _find_top_level_keyword(sql: str, keyword: str) -> int:
    """Index of `keyword` at paren depth 0 and outside quotes, else -1."""
    depth, quote, i = 0, None, 0
    kw = keyword.upper()
    up = sql.upper()
    while i < len(sql):
        ch = sql[i]
        if quote:
            if ch == quote:
                quote = None
        elif ch in "'\"":
            quote = ch
        elif ch in "([":
            depth += 1
        elif ch in ")]":
            depth -= 1
        elif depth == 0 and up.startswith(kw, i):
            before_ok = i == 0 or not (sql[i - 1].isalnum() or sql[i - 1] == "_")
            after = i + len(kw)
            after_ok = after >= len(sql) or not (sql[after].isalnum() or sql[after] == "_")
            if before_ok and after_ok:
                return i
        i += 1
    return -1


@dataclass
class ParsedSource:
    """The `source` block split into a projection and everything after FROM.

    Splitting matters for more than tidiness. The projection is rebuilt rather
    than pasted, which lets the staging model drop as-of-date-dependent
    expressions structurally instead of merely documenting that they are
    ignored. In the example spec that removes the one genuinely
    dialect-specific expression (a DATEDIFF) from the emitted SQL entirely.
    """

    items: list[tuple[str, str]]  # (alias, expression), source order
    from_clause: str  # "FROM ... WHERE ..." verbatim

    @property
    def columns(self) -> dict[str, str]:
        return {alias: expr for alias, expr in self.items}


def parse_source(source_sql: str) -> ParsedSource:
    """Split a single top-level SELECT into its projection and remainder."""
    sql = _strip_sql_comments(source_sql).strip()
    sel = _find_top_level_keyword(sql, "SELECT")
    if sel < 0:
        raise SpecError("source must be a SELECT statement; no top-level SELECT found")
    frm = _find_top_level_keyword(sql[sel:], "FROM")
    if frm < 0:
        raise SpecError("source must contain a top-level FROM clause")
    projection = sql[sel + len("SELECT") : sel + frm]
    from_clause = sql[sel + frm :].strip()

    if re.match(r"(?is)^\s*distinct\b", projection):
        raise SpecError(
            "source uses SELECT DISTINCT. The partial layer already aggregates per "
            "entity and day, so a DISTINCT here changes counts in a way the spec "
            "cannot express. Deduplicate upstream or model it as a condition instead."
        )
    if "*" in re.sub(r"'[^']*'", "", projection):
        raise SpecError(
            "source projects `*`. Every column must be named so the generator can "
            "type it, detect as-of-date dependence, and document it in the registry."
        )

    items: list[tuple[str, str]] = []
    for item in _split_top_level(projection):
        m = re.search(r"(?is)^(?P<expr>.+?)\s+AS\s+(?P<alias>[A-Za-z_][\w]*)\s*$", item)
        if m:
            alias, expr = m.group("alias"), m.group("expr").strip()
        elif re.fullmatch(r"[A-Za-z_][\w]*", item):
            alias, expr = item, item
        elif re.fullmatch(r"[A-Za-z_][\w]*\.([A-Za-z_][\w]*)", item):
            alias, expr = item.split(".")[-1], item
        else:
            raise SpecError(
                f"source projects an expression with no alias: {item!r}. Add `AS <name>` "
                "so it can be referenced as an atomic field or a condition operand."
            )
        items.append((alias.lower(), " ".join(expr.split())))
    return ParsedSource(items=items, from_clause=from_clause)


def detect_days_since(expr: str) -> str | None:
    """If `expr` is a days-between-event-and-target_date shape, return the source operand."""
    for pattern in DAYS_SINCE_PATTERNS:
        m = pattern.search(expr)
        if m:
            operand = m.group("from").strip()
            if operand.lower().endswith("::date"):
                operand = operand[: -len("::date")].strip()
            return operand
    return None


# --------------------------------------------------------------------------- #
# Loading
# --------------------------------------------------------------------------- #


def _one_key_mapping(item: Any, where: str) -> tuple[str, Any]:
    if not isinstance(item, dict) or len(item) != 1:
        raise SpecError(f"{where}: expected a single-key mapping like '- name: ...', got {item!r}")
    return next(iter(item.items()))


def _parse_categories(raw: Any) -> dict[str, ConditionCategory]:
    if not isinstance(raw, list):
        raise SpecError("condition_cat must be a list of single-key mappings")
    cats: dict[str, ConditionCategory] = {}
    for entry in raw:
        cat_name, members_raw = _one_key_mapping(entry, "condition_cat")
        if not IDENT_RE.match(str(cat_name)):
            raise SpecError(f"condition_cat name {cat_name!r} must be a lower_snake identifier")
        if not isinstance(members_raw, list):
            raise SpecError(f"condition_cat.{cat_name} must be a list of single-key mappings")
        members: list[ConditionMember] = []
        seen: set[str] = set()
        for m in members_raw:
            m_name, m_sql = _one_key_mapping(m, f"condition_cat.{cat_name}")
            m_name = str(m_name)
            if not IDENT_RE.match(m_name):
                raise SpecError(
                    f"condition_cat.{cat_name}.{m_name}: member name must be lower_snake"
                )
            if m_name in seen:
                raise SpecError(f"condition_cat.{cat_name}: duplicate member {m_name!r}")
            seen.add(m_name)
            if not isinstance(m_sql, str) or not m_sql.strip():
                raise SpecError(
                    f"condition_cat.{cat_name}.{m_name}: predicate must be a SQL string"
                )
            members.append(ConditionMember(name=m_name, sql=m_sql.strip()))
        if cat_name in cats:
            raise SpecError(f"condition_cat: duplicate category {cat_name!r}")
        cats[cat_name] = ConditionCategory(name=cat_name, members=tuple(members))
    return cats


def _parse_windows(raw: Any) -> tuple[TimeWindow, ...]:
    if not isinstance(raw, list) or not raw:
        raise SpecError("time_cat must be a non-empty list, e.g. ['l7d', 'l30d', 'all_time']")
    windows, seen = [], set()
    for name in raw:
        name = str(name).strip()
        if name in seen:
            raise SpecError(f"time_cat: duplicate window {name!r}")
        seen.add(name)
        if name.lower() == "all_time":
            windows.append(TimeWindow(name="all_time", days=None))
            continue
        m = WINDOW_RE.match(name)
        if not m:
            raise SpecError(
                f"time_cat entry {name!r} is not supported. Use 'l<N>d' (e.g. l7d) or 'all_time'."
            )
        days = int(m.group(1))
        if days < 1:
            raise SpecError(f"time_cat entry {name!r}: window must cover at least 1 day")
        windows.append(TimeWindow(name=name.lower(), days=days))
    return tuple(windows)


def _parse_fields(raw: Any, categories: dict[str, ConditionCategory]) -> list[AtomicField]:
    if not isinstance(raw, list) or not raw:
        raise SpecError("atomic_field must be a non-empty list of single-key mappings")
    fields: list[AtomicField] = []
    seen: set[str] = set()
    for entry in raw:
        name, cfg = _one_key_mapping(entry, "atomic_field")
        name = str(name)
        if name in seen:
            raise SpecError(f"atomic_field: duplicate field {name!r}")
        seen.add(name)
        if not isinstance(cfg, dict):
            raise SpecError(f"atomic_field.{name} must be a mapping")

        applied = cfg.get("apply_cond_cat") or []
        if not isinstance(applied, list):
            raise SpecError(f"atomic_field.{name}.apply_cond_cat must be a list")
        for cat in applied:
            if cat not in categories:
                raise SpecError(
                    f"atomic_field.{name}.apply_cond_cat references unknown category {cat!r}. "
                    f"Known categories: {sorted(categories)}"
                )
        if len(set(applied)) != len(applied):
            raise SpecError(f"atomic_field.{name}.apply_cond_cat contains duplicates")

        aggs = cfg.get("agg") or []
        if not isinstance(aggs, list) or not aggs:
            raise SpecError(f"atomic_field.{name}.agg must be a non-empty list")
        for agg in aggs:
            if agg not in SUPPORTED_AGGS:
                raise SpecError(
                    f"atomic_field.{name}: unsupported agg {agg!r}. "
                    f"Supported: {sorted(SUPPORTED_AGGS)}"
                )
        if len(set(aggs)) != len(aggs):
            raise SpecError(f"atomic_field.{name}.agg contains duplicates")

        raw_type = cfg.get("field_type")
        field_type = None
        if raw_type is not None:
            key = str(raw_type).strip().lower()
            if key not in FIELD_TYPE_MAP:
                raise SpecError(
                    f"atomic_field.{name}.field_type {raw_type!r} is not recognised. "
                    f"Known: {sorted(set(FIELD_TYPE_MAP))}"
                )
            field_type = FIELD_TYPE_MAP[key]

        distinct_method = str(cfg.get("distinct_method", "exact")).strip().lower()
        if distinct_method not in ("exact", "approx"):
            raise SpecError(
                f"atomic_field.{name}.distinct_method must be 'exact' or 'approx', "
                f"got {distinct_method!r}"
            )

        derived = None
        d_cfg = cfg.get("derived")
        if d_cfg is not None:
            if not isinstance(d_cfg, dict) or "kind" not in d_cfg:
                raise SpecError(f"atomic_field.{name}.derived must be a mapping with a 'kind'")
            if d_cfg["kind"] != "days_since":
                raise SpecError(
                    f"atomic_field.{name}.derived.kind {d_cfg['kind']!r} is not supported. "
                    "Only 'days_since' exists today."
                )
            if "from" not in d_cfg:
                raise SpecError(f"atomic_field.{name}.derived requires a 'from' source column")
            derived = Derivation(kind="days_since", from_field=str(d_cfg["from"]))

        fields.append(
            AtomicField(
                name=name,
                apply_cond_cat=tuple(applied),
                aggs=tuple(aggs),
                field_type=field_type,
                distinct_method=distinct_method,
                derived=derived,
                description=str(cfg.get("description", "") or ""),
            )
        )
    return fields


def load_spec(path: str | Path) -> FeatureSpec:
    path = Path(path)
    raw = yaml.safe_load(path.read_text())
    if not isinstance(raw, dict):
        raise SpecError(f"{path}: top level of a feature spec must be a mapping")

    try:
        doc = _SpecDoc.model_validate(raw)
    except ValidationError as exc:
        raise _shape_error(path, exc) from None

    feature_name = str(raw["feature_name"]).strip()
    if not IDENT_RE.match(feature_name):
        raise SpecError(f"feature_name {feature_name!r} must be a lower_snake identifier")

    feature_type = str(raw.get("feature_type", "daily")).strip().lower()
    if feature_type != "daily":
        raise SpecError(
            f"feature_type {feature_type!r} is not implemented. Only 'daily' batch is supported."
        )

    entities = raw["entities"]
    if not isinstance(entities, list) or not entities:
        raise SpecError("entities must be a non-empty list")

    categories = _parse_categories(raw.get("condition_cat", []))
    windows = _parse_windows(raw["time_cat"])
    fields = _parse_fields(raw["atomic_field"], categories)

    settings = doc.settings or Settings()
    relations = {
        literal: SourceRelation(
            literal=literal, source_name=cfg.source_name, table_name=cfg.table_name
        )
        for literal, cfg in (doc.relations or {}).items()
    }

    spec = FeatureSpec(
        feature_name=feature_name,
        feature_type=feature_type,
        created_by=str(raw.get("created_by", "unknown")),
        description=str(raw.get("description", "") or "").strip(),
        source_sql=str(raw["source"]).strip(),
        entities=tuple(str(e) for e in entities),
        timestamp_col=str(raw["timestamp_col"]),
        categories=categories,
        windows=windows,
        fields=fields,
        settings=settings,
        relations=relations,
        spec_path=path,
        raw=raw,
    )
    spec.parsed_source = parse_source(spec.source_sql)
    spec.source_columns = spec.parsed_source.columns
    validate_spec(spec)
    return spec


# --------------------------------------------------------------------------- #
# Validation
# --------------------------------------------------------------------------- #


def _resolve_operand_to_alias(operand: str, source_columns: dict[str, str]) -> str | None:
    """Map a SQL operand back to a source alias, so derivations stay declarative."""
    key = operand.strip().lower()
    if key in source_columns:
        return key
    normalised = " ".join(operand.split()).lower()
    for alias, expr in source_columns.items():
        if " ".join(expr.split()).lower() == normalised:
            return alias
    return None


def validate_spec(spec: FeatureSpec) -> None:
    src = spec.source_columns
    known = sorted(src)

    for ent in spec.entities:
        if ent.lower() not in src:
            raise SpecError(
                f"entity {ent!r} is not produced by the source SELECT list. Available: {known}"
            )
    if spec.timestamp_col.lower() not in src:
        raise SpecError(
            f"timestamp_col {spec.timestamp_col!r} is not produced by the source SELECT list. "
            f"Available: {known}"
        )

    ts_expr = src[spec.timestamp_col.lower()]
    if TARGET_DATE_RE.search(ts_expr):
        raise SpecError(
            f"timestamp_col {spec.timestamp_col!r} depends on target_date. The event timestamp "
            "defines the partition an event belongs to and must be independent of the as-of date."
        )

    # A condition that moves with the as-of date would make yesterday's stored
    # partials wrong today, silently. Partial reuse depends on this holding.
    for cat in spec.categories.values():
        for member in cat.members:
            if TARGET_DATE_RE.search(member.sql):
                raise SpecError(
                    f"condition_cat.{cat.name}.{member.name} references target_date. Condition "
                    "predicates must be time-invariant, otherwise stored daily partials would "
                    "have to be recomputed for every as-of date and lose all reuse."
                )

    for f in spec.fields:
        if f.name.lower() not in src:
            raise SpecError(
                f"atomic_field {f.name!r} is not produced by the source SELECT list. "
                f"Available: {known}"
            )
        expr = src[f.name.lower()]

        # --- target_date dependence ------------------------------------------
        if TARGET_DATE_RE.search(expr) and f.derived is None:
            operand = detect_days_since(expr)
            if operand is None:
                raise SpecError(
                    f"atomic_field {f.name!r} is computed from target_date:\n"
                    f"    {expr}\n"
                    "Its value changes with every as-of date, so it cannot be stored in the "
                    "reusable daily partial layer. Declare how to rebuild it at scoring time:\n"
                    f"    - {f.name}:\n"
                    "        derived: {kind: days_since, from: <timestamp_column>}\n"
                    "or rewrite the source so the column is time-invariant."
                )
            alias = _resolve_operand_to_alias(operand, src)
            if alias is None:
                raise SpecError(
                    f"atomic_field {f.name!r} looks like days-since-target_date over {operand!r}, "
                    f"but {operand!r} is not a column of the source SELECT list. Available: {known}"
                )
            f.derived = Derivation(kind="days_since", from_field=alias, auto_detected=True)

        if f.derived is not None:
            base = f.derived.from_field.lower()
            if base not in src:
                raise SpecError(
                    f"atomic_field {f.name!r}: derived.from {f.derived.from_field!r} is not a "
                    f"source column. Available: {known}"
                )
            if TARGET_DATE_RE.search(src[base]):
                raise SpecError(
                    f"atomic_field {f.name!r}: derived.from {base!r} itself depends on "
                    "target_date, so it cannot anchor a derivation."
                )
            unsupported = set(f.aggs) - {"min", "max"}
            if unsupported:
                raise SpecError(
                    f"atomic_field {f.name!r} is a days_since derivation, which is monotonic in "
                    f"event time and therefore only supports 'min' and 'max'. Got: "
                    f"{sorted(unsupported)}. Aggregate the underlying timestamp "
                    f"({f.derived.from_field}) instead."
                )
            f.field_type = f.field_type or "bigint"

        # --- typing -----------------------------------------------------------
        typed = [a for a in f.aggs if a in TYPED_AGGS]
        if typed and not f.field_type:
            raise SpecError(
                f"atomic_field {f.name!r} uses {typed} which need a declared type. "
                "Add e.g. field_type: timestamp | numeric | string."
            )
        if f.field_type in ("timestamp", "date", "varchar", "boolean"):
            numeric_only = [a for a in f.aggs if a in ("sum", "avg")]
            if numeric_only:
                raise SpecError(
                    f"atomic_field {f.name!r} has field_type {f.field_type!r} but requests "
                    f"{numeric_only}, which need a numeric field."
                )

        if f.distinct_method == "approx" and "count_distinct" not in f.aggs:
            raise SpecError(
                f"atomic_field {f.name!r} sets distinct_method but has no count_distinct agg."
            )

        # --- label collisions --------------------------------------------------
        seen_members: dict[str, str] = {}
        for cat_name in f.apply_cond_cat:
            for member in spec.categories[cat_name].members:
                if member.is_default:
                    continue
                if member.name in seen_members:
                    raise SpecError(
                        f"atomic_field {f.name!r} applies categories "
                        f"{seen_members[member.name]!r} and {cat_name!r}, which both define a "
                        f"member named {member.name!r}. Generated feature names would collide. "
                        "Rename one of them."
                    )
                seen_members[member.name] = cat_name

    for literal in spec.relations:
        if not _relation_re([literal]).search(spec.source_sql):
            raise SpecError(
                f"relations declares {literal!r} but that text does not appear in the source SQL. "
                "The mapping substitutes whole relation names, so it must match one exactly."
            )

    unmapped = re.findall(r"(?is)\bfrom\s+([a-z_][\w]*(?:\.[a-z_][\w]*)+)", spec.source_sql)
    unmapped += re.findall(r"(?is)\bjoin\s+([a-z_][\w]*(?:\.[a-z_][\w]*)+)", spec.source_sql)
    for rel in unmapped:
        if rel not in spec.relations:
            raise SpecError(
                f"source reads {rel!r} directly. Hard-coded relations only resolve in one "
                "environment, so the model could never be run or tested anywhere else. Map it:\n"
                f"    relations:\n"
                f"      {rel}:\n"
                f"        source_name: <dbt source>\n"
                f"        table_name: <table>"
            )

    if spec.has_all_time and not spec.settings.source_is_append_only:
        raise SpecError(
            "time_cat includes 'all_time' but settings.source_is_append_only is false. "
            "Incrementally accumulated all_time state is only sound over an append-only source; "
            "a mutable source needs a full recompute strategy that is not implemented."
        )
