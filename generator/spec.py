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
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

from generator.aggregates import AGG_KEYS, Aggregation, DaysSince, get_aggregate

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
COLUMN_RE = re.compile(r"^[A-Za-z_][\w]*$")
TARGET_DATE_RE = re.compile(r"\{\{\s*target_date\s*\}\}")
JOIN_RE = re.compile(r"(?i)\bjoin\b")

# Names the generated staging model gives a relation's knowledge-time columns,
# so every layer above it reads one spelling whatever the source calls them.
LOADED_AT = "_fs_loaded_at"
SUPERSEDED_AT = "_fs_superseded_at"


def _check_timezone(name: str) -> str:
    """An IANA zone name, checked against the tz database rather than trusted."""
    try:
        ZoneInfo(name)
    except (ZoneInfoNotFoundError, ValueError):
        raise ValueError(
            f"{name!r} is not an IANA time zone name (e.g. 'UTC', 'Asia/Jakarta')"
        ) from None
    return name


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

    It also carries the facts about the table that point-in-time correctness
    rests on, because they are properties of the table rather than of any one
    spec reading it:

      loaded_at      when each row (or row version) became visible in the
                     warehouse. Required: it is how a run tells what it may
                     know about, and how it finds the days that changed since
                     the last run.
      superseded_at  when a row version was replaced or deleted upstream (SCD2
                     or soft delete). Absent for an insert-only table.
      timezone       the zone the table's naive timestamps are recorded in.
      key            the columns identifying a row across its versions, when
                     known. It lets the staging model test the history itself:
                     one row per key and version, and no two versions of a key
                     current at once -- which would count that row twice.
    """

    literal: str
    source_name: str
    table_name: str
    loaded_at: str
    superseded_at: str | None = None
    timezone: str = "UTC"
    key: tuple[str, ...] = ()

    @property
    def staging_model(self) -> str:
        """The one model that reads this source. Every spec reads it through this."""
        return f"stg_{self.source_name}__{self.table_name}"

    @property
    def dbt_ref(self) -> str:
        return f"{{{{ ref('{self.staging_model}') }}}}"

    @property
    def dbt_source(self) -> str:
        return f"{{{{ source('{self.source_name}', '{self.table_name}') }}}}"

    def contract(self) -> dict[str, str | list[str] | None]:
        """Everything two specs reading this table must agree on."""
        return {
            "loaded_at": self.loaded_at,
            "superseded_at": self.superseded_at,
            "timezone": self.timezone,
            "key": list(self.key),
        }


def _relation_re(literals: list[str]) -> re.Pattern[str]:
    """Match any of `literals` as a whole relation name, never as part of a longer one.

    Without the boundaries `bronze.db.events` would match inside
    `bronze.db.events_blocklist` and splice the wrong source() into it. Longest
    first, so of two names where one is a prefix of the other, each position
    resolves to the name actually written there.
    """
    alts = "|".join(re.escape(lit) for lit in sorted(literals, key=len, reverse=True))
    return re.compile(rf"(?<![\w.])(?:{alts})(?![\w])", re.IGNORECASE)


def substitute_relations(sql: str, relations: dict[str, SourceRelation]) -> str:
    """Replace every mapped relation name in `sql` with a ref() to its staging model.

    Matched without regard to case, like the warehouse resolves an unquoted name.
    """
    if not relations:
        return sql
    by_name = {lit.lower(): rel for lit, rel in relations.items()}
    return _relation_re(list(relations)).sub(lambda m: by_name[m.group(0).lower()].dbt_ref, sql)


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

    def aggregations(self, kmv_k: int) -> list[Aggregation]:
        """What each `agg:` entry means for this field, in declaration order."""
        return [
            get_aggregate(a, self.distinct_method, kmv_k, days_since=self.is_derived)
            for a in self.aggs
        ]


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
    materialized_mart: Literal["incremental", "table"] = "incremental"
    # The business time zone. An event's date, the hour a condition sees and
    # the end of an as-of day are all read on this clock, never on whatever
    # the warehouse session happens to be set to.
    timezone: str = "UTC"

    @field_validator("timezone")
    @classmethod
    def _iana_zone(cls, v: str) -> str:
        return _check_timezone(v)


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
    source_name: str = Field(pattern=r"^[a-z_][a-z0-9_]*$")
    table_name: str = Field(pattern=r"^[a-z_][a-z0-9_]*$")
    loaded_at: str = Field(pattern=COLUMN_RE.pattern)
    superseded_at: str | None = Field(default=None, pattern=COLUMN_RE.pattern)
    timezone: str = "UTC"
    key: list[str] | None = None

    @field_validator("key")
    @classmethod
    def _columns(cls, v: list[str] | None) -> list[str] | None:
        if v is not None and (not v or not all(COLUMN_RE.match(c) for c in v)):
            raise ValueError("must be a non-empty list of column names")
        return v

    @field_validator("timezone")
    @classmethod
    def _iana_zone(cls, v: str) -> str:
        return _check_timezone(v)


class _OwnerDoc(BaseModel):
    model_config = _STRICT
    name: str | None = None
    email: str | None = None


class _ExposureDoc(BaseModel):
    model_config = _STRICT
    name: str = Field(pattern=r"^[a-z_][a-z0-9_]*$")
    type: Literal["ml", "application", "notebook", "analysis", "dashboard"]
    owner: _OwnerDoc
    description: str | None = None
    url: str | None = None
    maturity: Literal["low", "medium", "high"] | None = None


class _SpecDoc(BaseModel):
    model_config = _STRICT
    feature_name: str
    feature_type: str | None = None
    created_by: str | None = None
    description: str | None = None
    source: str
    entities: list[str | dict[str, str]]
    timestamp_col: str
    condition_cat: list[dict[str, list[dict[str, str]]]] | None = None
    time_cat: list[str]
    atomic_field: list[dict[str, _AtomicFieldDoc]]
    settings: Settings | None = None
    relations: dict[str, _RelationDoc]
    exposures: list[_ExposureDoc] | None = None


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


@dataclass(frozen=True)
class Exposure:
    """A downstream consumer of the published mart, recorded as a dbt exposure."""

    name: str
    type: str
    owner_name: str | None
    owner_email: str | None
    description: str = ""
    url: str | None = None
    maturity: str | None = None


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
    #: Logical type of each entity key, published in the mart's contract.
    entity_types: dict[str, str] = dc_field(default_factory=dict)
    source_columns: dict[str, str] = dc_field(default_factory=dict)
    parsed_source: ParsedSource | None = None
    relations: dict[str, SourceRelation] = dc_field(default_factory=dict)
    exposures: tuple[Exposure, ...] = ()
    spec_path: Path | None = None
    raw: dict[str, Any] = dc_field(default_factory=dict)

    @property
    def relation(self) -> SourceRelation:
        """The one table this spec reads. validate_spec guarantees there is exactly one."""
        (rel,) = self.relations.values()
        return rel

    @property
    def timestamp_aliases(self) -> list[str]:
        """Source aliases holding timestamps, in projection order.

        These are the columns that are moved onto the business clock before
        anything reads them: the event timestamp itself, any atomic field typed
        as a timestamp, and the anchor of every days_since derivation.
        """
        names = {self.timestamp_col.lower()}
        for f in self.fields:
            if f.field_type == "timestamp" and not f.is_derived:
                names.add(f.name.lower())
            if f.derived is not None:
                names.add(f.derived.from_field.lower())
        return [alias for alias in self.source_columns if alias in names]

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
        """Stable fingerprint of the spec, stamped onto every published row."""
        return _fingerprint(self.raw)

    @property
    def state_version(self) -> str:
        """Fingerprint of everything that decides what the STORED state means.

        Stamped on every partial and accumulator row. Unlike spec_hash it
        ignores what cannot change a stored value (descriptions, windows, the
        spine, the seal delay), so editing a description does not demand a
        rebuild, while changing a predicate, a source expression or the time
        zone does: rows folded under the old meaning would otherwise sit beside
        rows folded under the new one, with nothing to tell them apart.
        """
        settings = self.settings
        return _fingerprint(
            {
                "source": self.source_sql,
                "entities": self.raw.get("entities"),
                "timestamp_col": self.timestamp_col,
                "condition_cat": self.raw.get("condition_cat"),
                "atomic_field": [
                    {
                        "name": f.name,
                        "apply_cond_cat": list(f.apply_cond_cat),
                        "agg": list(f.aggs),
                        "field_type": f.field_type,
                        "distinct_method": f.distinct_method,
                        "derived": f.derived.from_field if f.derived else None,
                    }
                    for f in self.fields
                ],
                "relations": {k: r.contract() for k, r in self.relations.items()},
                "kmv_k": settings.kmv_k,
                "separator": settings.separator,
                "timezone": settings.timezone,
            }
        )


def _fingerprint(payload: Any) -> str:
    text = yaml.safe_dump(payload, sort_keys=True, default_flow_style=False)
    return hashlib.sha256(text.encode()).hexdigest()[:12]


# --------------------------------------------------------------------------- #
# Lightweight SQL introspection
# --------------------------------------------------------------------------- #


def _strip_sql_comments(sql: str) -> str:
    sql = re.sub(r"--[^\n]*", "", sql)
    return re.sub(r"/\*.*?\*/", "", sql, flags=re.DOTALL)


def _strip_sql_literals(sql: str) -> str:
    """Blank out string literals, so a keyword inside one is never read as SQL."""
    return re.sub(r"'(?:[^']|'')*'", "''", sql)


def lowercase_sql(sql: str) -> str:
    """Lowercase SQL outside string literals, quoted identifiers, comments and Jinja.

    Authored fragments are pasted into generated models whose house style is
    lower case. Unquoted identifiers and keywords are case-insensitive on every
    supported warehouse, so this changes how the SQL reads and never what it
    means; anything whose case could matter -- a literal, a quoted name, a
    Jinja expression -- is copied through untouched.
    """
    out: list[str] = []
    i, n = 0, len(sql)
    while i < n:
        if sql.startswith(("{{", "{%", "{#"), i):
            close = {"{{": "}}", "{%": "%}", "{#": "#}"}[sql[i : i + 2]]
            end = sql.find(close, i + 2)
            end = n if end < 0 else end + 2
        elif sql.startswith("--", i):
            end = sql.find("\n", i)
            end = n if end < 0 else end
        elif sql.startswith("/*", i):
            end = sql.find("*/", i + 2)
            end = n if end < 0 else end + 2
        elif sql[i] in "'\"`":
            quote, end = sql[i], i + 1
            while end < n:
                if sql[end] == quote:
                    if end + 1 < n and sql[end + 1] == quote:
                        end += 2  # doubled quote: an escaped literal quote
                        continue
                    break
                end += 1
            end = min(end + 1, n)
        else:
            out.append(sql[i].lower())
            i += 1
            continue
        out.append(sql[i:end])
        i = end
    return "".join(out)


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
        items.append((alias.lower(), " ".join(lowercase_sql(expr).split())))
    return ParsedSource(items=items, from_clause=lowercase_sql(from_clause))


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


def _parse_entities(raw: list[str | dict[str, str]]) -> dict[str, str]:
    """Entity key name -> logical type, from `["id"]` or `[{id: bigint}]` items.

    An untyped key is a string. That is a default rather than a guess: the mart
    enforces its contract, so a key that is really numeric fails the build
    loudly on its first run instead of being published under the wrong type.
    """
    if not raw:
        raise SpecError("entities must be a non-empty list")
    out: dict[str, str] = {}
    for item in raw:
        if isinstance(item, str):
            name, type_ = item, "string"
        else:
            name, type_ = _one_key_mapping(item, "entities")
        key = str(type_).strip().lower()
        if key not in FIELD_TYPE_MAP:
            raise SpecError(
                f"entities.{name}: type {type_!r} is not recognised. "
                f"Known: {sorted(set(FIELD_TYPE_MAP))}"
            )
        if name in out:
            raise SpecError(f"entities: duplicate key {name!r}")
        out[str(name)] = FIELD_TYPE_MAP[key]
    return out


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
            members.append(ConditionMember(name=m_name, sql=lowercase_sql(m_sql.strip())))
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
            if agg not in AGG_KEYS:
                raise SpecError(
                    f"atomic_field.{name}: unsupported agg {agg!r}. Supported: {sorted(AGG_KEYS)}"
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

    entity_types = _parse_entities(doc.entities)

    categories = _parse_categories(raw.get("condition_cat", []))
    windows = _parse_windows(raw["time_cat"])
    fields = _parse_fields(raw["atomic_field"], categories)

    settings = doc.settings or Settings()
    relations = {
        literal: SourceRelation(
            literal=literal,
            source_name=cfg.source_name,
            table_name=cfg.table_name,
            loaded_at=cfg.loaded_at,
            superseded_at=cfg.superseded_at,
            timezone=cfg.timezone,
            key=tuple(cfg.key or ()),
        )
        for literal, cfg in doc.relations.items()
    }
    exposures = tuple(
        Exposure(
            name=e.name,
            type=e.type,
            owner_name=e.owner.name,
            owner_email=e.owner.email,
            description=(e.description or "").strip(),
            url=e.url,
            maturity=e.maturity,
        )
        for e in doc.exposures or []
    )
    for e in exposures:
        if not (e.owner_name or e.owner_email):
            raise SpecError(f"exposures.{e.name}.owner needs a name or an email")
    if len({e.name for e in exposures}) != len(exposures):
        raise SpecError("exposures: two exposures share a name")

    spec = FeatureSpec(
        feature_name=feature_name,
        feature_type=feature_type,
        created_by=str(raw.get("created_by", "unknown")),
        description=str(raw.get("description", "") or "").strip(),
        source_sql=str(raw["source"]).strip(),
        entities=tuple(entity_types),
        entity_types=entity_types,
        timestamp_col=str(raw["timestamp_col"]),
        categories=categories,
        windows=windows,
        fields=fields,
        settings=settings,
        relations=relations,
        exposures=exposures,
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

    for alias in src:
        if alias == "event_date" or alias.startswith("_fs_"):
            raise SpecError(
                f"source projects {alias!r}, a name the generated models reserve for "
                "themselves. Alias it to something else."
            )

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
            unsupported = set(f.aggs) - set(DaysSince.KEYS)
            if unsupported:
                raise SpecError(
                    f"atomic_field {f.name!r} is a days_since derivation, which is monotonic in "
                    f"event time and therefore only supports 'min' and 'max'. Got: "
                    f"{sorted(unsupported)}. Aggregate the underlying timestamp "
                    f"({f.derived.from_field}) instead."
                )
            f.field_type = f.field_type or "bigint"

        # --- typing -----------------------------------------------------------
        aggregations = f.aggregations(spec.settings.kmv_k)
        untyped = [a.key for a in aggregations if a.requires_field_type and not f.field_type]
        if untyped:
            raise SpecError(
                f"atomic_field {f.name!r} uses {untyped} which need a declared type. "
                "Add e.g. field_type: timestamp | numeric | string."
            )
        non_numeric = [a.key for a in aggregations if a.accepts(f.field_type)]
        if non_numeric:
            raise SpecError(
                f"atomic_field {f.name!r} has field_type {f.field_type!r} but requests "
                f"{non_numeric}, which need a numeric field."
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
    mapped = {lit.lower() for lit in spec.relations}
    for rel in unmapped:
        if rel.lower() not in mapped:
            raise SpecError(
                f"source reads {rel!r} directly. Hard-coded relations only resolve in one "
                "environment, so the model could never be run or tested anywhere else. Map it:\n"
                f"    relations:\n"
                f"      {rel}:\n"
                f"        source_name: <dbt source>\n"
                f"        table_name: <table>\n"
                f"        loaded_at: <column recording when each row became visible>"
            )

    # Point-in-time reads are generated from the relation's knowledge-time
    # columns. A hand-written as-of filter would be a second, competing
    # definition of what a run may know, and one the generator cannot use to
    # find the days that changed since the last run.
    if TARGET_DATE_RE.search(_strip_sql_comments(spec.parsed_source.from_clause)):  # type: ignore[union-attr]
        raise SpecError(
            "source filters on {{ target_date }}. The generator writes the as-of filter "
            "itself, from the relation's knowledge-time columns, so it can also detect late "
            "arrivals, corrections and deletions. Remove the filter and declare them:\n"
            "    relations:\n"
            "      <relation>:\n"
            "        loaded_at: <column: when a row version became visible>\n"
            "        superseded_at: <column: when it was replaced or deleted, if ever>"
        )

    # A joined table would be read as it is NOW, not as it was on each as-of
    # date, so every feature built from it would quietly see the future.
    if JOIN_RE.search(_strip_sql_literals(_strip_sql_comments(spec.source_sql))):
        raise SpecError(
            "source joins another relation. A join here is not point-in-time: the joined "
            "table would be read as it stands today for every as-of date. Materialise the "
            "join upstream into one relation that carries its own loaded_at, and read that."
        )
    if len(spec.relations) != 1:
        raise SpecError(
            f"source must read exactly one mapped relation, found {len(spec.relations)}. "
            "Each row's knowledge time comes from that relation's loaded_at."
        )
