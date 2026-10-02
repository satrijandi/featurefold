"""The feature registry: written by the compiler, read by every deployment.

The registry is the machine-readable contract between this repo and everything
downstream: notebooks discovering what exists, training pipelines resolving a
feature list, orchestration deciding what to build, and reviewers seeing exactly
what a pull request added or removed. It is committed, so `git log` on it is a
feature-level changelog.

Both directions live here so the key layout has one home. `build_registry` turns
a plan into the JSON payload; `Registry` reads one back and answers the questions
a deployment actually asks of it -- which models to run, how far back a run may
revise, how to reset the accumulator -- so no caller walks dict paths.

Reading needs nothing but the standard library: a deployment can depend on the
contract without installing the compiler's dependencies.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

from generator.revision import refreshable_dates, reset_accumulator_command

if TYPE_CHECKING:
    from generator.expand import FeaturePlan


class RegistryError(ValueError):
    """A registry file that does not match the layout this reader knows."""


# --------------------------------------------------------------------------- #
# Writing
# --------------------------------------------------------------------------- #


def build_registry(plan: FeaturePlan, generated_at: str | None = None) -> dict:
    # Imported here, not at module level, so reading a registry never pulls in
    # the compiler.
    from generator.expand import ComputedFeature
    from generator.render import Renderer, group_name

    spec = plan.spec
    r = Renderer(plan)

    features = []
    for item in plan.outputs():
        is_computed = isinstance(item, ComputedFeature)
        field = next((f for f in spec.fields if f.name == item.field), None)
        features.append(
            {
                "name": item.name,
                "dtype": item.dtype,
                "atomic_field": item.field,
                "agg": item.agg_key,
                "window": item.window.name,
                "window_days": item.window.days,
                "conditions": {cat: mem for cat, mem in item.combo.members},
                "is_marginal": item.combo.is_marginal,
                "materialisation": "computed" if is_computed else "stored",
                "computed_kind": item.aggregation.kind if is_computed else None,
                "inputs": list(item.inputs) if is_computed else [item.partial.name],
                "distinct_method": (
                    field.distinct_method if field and item.agg_key == "count_distinct" else None
                ),
                "nullable": item.aggregation.nullable,
                "description": item.description,
            }
        )

    models = {
        "staging": r.m_stg,
        "events": r.m_events,
        "daily_partials": r.m_partials,
        "window_rollup": r.m_rollup if spec.bounded_windows else None,
        "alltime_state": r.m_state if spec.has_all_time else None,
        "alltime_recent": r.m_recent if r.has_recent else None,
        "mart": r.m_mart,
    }

    return {
        "feature_name": spec.feature_name,
        "spec_version": spec.spec_hash,
        "state_version": spec.state_version,
        "feature_type": spec.feature_type,
        "owner": spec.created_by,
        "group": group_name(spec.created_by),
        "description": spec.description,
        "generated_at": generated_at or datetime.now(UTC).isoformat(timespec="seconds"),
        "generator_version": __import__("generator").__version__,
        "entities": list(spec.entities),
        "entity_types": dict(spec.entity_types),
        "timestamp_col": spec.timestamp_col,
        "sources": [
            {
                "literal": rel.literal,
                "dbt_source": rel.source_name,
                "dbt_table": rel.table_name,
                "loaded_at": rel.loaded_at,
                "superseded_at": rel.superseded_at,
                "timezone": rel.timezone,
            }
            for rel in spec.relations.values()
        ],
        "exposures": [e.name for e in spec.exposures],
        "settings": spec.settings.model_dump(),
        "models": models,
        "expansion": {
            "atomic_fields": len(spec.fields),
            "condition_categories": {
                c.name: [m.name for m in c.members] for c in spec.categories.values()
            },
            "time_windows": [w.name for w in spec.windows],
            "partial_columns": len(plan.partials),
            "stored_features": len([s for s in plan.stored if not s.internal]),
            "computed_features": len(plan.computed),
            "total_features": plan.feature_count,
        },
        "features": features,
    }


def write_registry(plan: FeaturePlan, out_dir: Path, generated_at: str | None = None) -> str:
    payload = build_registry(plan, generated_at=generated_at)
    return json.dumps(payload, indent=2, sort_keys=False) + "\n"


# --------------------------------------------------------------------------- #
# Reading
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class RegisteredModels:
    """The dbt models one spec compiles into. A layer the spec does not need is None."""

    staging: str
    events: str
    daily_partials: str
    window_rollup: str | None
    alltime_state: str | None
    alltime_recent: str | None
    mart: str


@dataclass(frozen=True)
class RegisteredSource:
    """The table a spec reads, and the facts its point-in-time reads rest on."""

    dbt_source: str
    dbt_table: str
    loaded_at: str
    superseded_at: str | None
    #: The zone the table's naive timestamps are recorded in.
    timezone: str


@dataclass(frozen=True)
class RegisteredFeature:
    name: str
    agg: str
    window: str
    #: None for all_time.
    window_days: int | None
    is_marginal: bool
    nullable: bool


@dataclass(frozen=True)
class Registry:
    feature_name: str
    spec_version: str
    state_version: str
    description: str
    entities: tuple[str, ...]
    late_arrival_days: int
    entity_spine: str
    window_convention: str
    kmv_k: int
    #: The business clock: event dates and as-of days are read in this zone.
    timezone: str
    source: RegisteredSource
    models: RegisteredModels
    features: tuple[RegisteredFeature, ...]

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> Registry:
        try:
            settings = payload["settings"]
            return cls(
                feature_name=payload["feature_name"],
                spec_version=payload["spec_version"],
                state_version=payload["state_version"],
                description=payload.get("description") or "",
                entities=tuple(payload["entities"]),
                late_arrival_days=int(settings["late_arrival_days"]),
                entity_spine=settings["entity_spine"],
                window_convention=settings["window_convention"],
                kmv_k=int(settings["kmv_k"]),
                timezone=settings["timezone"],
                source=RegisteredSource(
                    **{k: v for k, v in payload["sources"][0].items() if k != "literal"}
                ),
                models=RegisteredModels(**payload["models"]),
                features=tuple(
                    RegisteredFeature(
                        name=f["name"],
                        agg=f["agg"],
                        window=f["window"],
                        window_days=f["window_days"],
                        is_marginal=f["is_marginal"],
                        nullable=f["nullable"],
                    )
                    for f in payload["features"]
                ),
            )
        except (KeyError, TypeError) as exc:
            name = payload.get("feature_name", "<unnamed>") if isinstance(payload, dict) else "?"
            raise RegistryError(
                f"registry for {name} does not match this reader ({exc!r}). "
                "Regenerate it with `make generate`."
            ) from None

    @classmethod
    def load(cls, path: str | Path) -> Registry:
        path = Path(path)
        if not path.exists():
            raise RegistryError(f"no registry at {path}; run `make generate` first")
        return cls.from_dict(json.loads(path.read_text()))

    @classmethod
    def load_dir(cls, registry_dir: str | Path) -> list[Registry]:
        """Every registry in a directory, by feature name. A missing directory is empty."""
        return [cls.load(p) for p in sorted(Path(registry_dir).glob("*.json"))]

    @property
    def widest_window_days(self) -> int:
        """Days spanned by the widest bounded window, or 0 if every window is all_time."""
        return max((f.window_days for f in self.features if f.window_days), default=0)

    def refreshable_dates(
        self, target: date, watermark: date | None, first_served: date | None = None
    ) -> list[date]:
        """Partitions before `target` a run may still rebuild. See generator.revision."""
        return refreshable_dates(target, self.late_arrival_days, watermark, first_served)

    def reset_accumulator_command(self, as_of: str) -> str | None:
        """The dbt command that rebuilds the all_time accumulator at `as_of`, if there is one."""
        state = self.models.alltime_state
        return reset_accumulator_command(state, as_of) if state else None
