"""Emit the feature registry.

The registry is the machine-readable contract between this repo and everything
downstream: notebooks discovering what exists, training pipelines resolving a
feature list, serving code checking types, and reviewers seeing exactly what a
pull request added or removed. It is committed, so `git log` on it is a
feature-level changelog.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from generator.expand import ComputedFeature, FeaturePlan
from generator.render import Renderer


def build_registry(plan: FeaturePlan, generated_at: str | None = None) -> dict:
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
                "computed_kind": item.kind if is_computed else None,
                "inputs": list(item.inputs) if is_computed else [item.partial.name],
                "distinct_method": (
                    field.distinct_method if field and item.agg_key == "count_distinct" else None
                ),
                "nullable": item.agg_key in ("min", "max", "avg"),
                "description": item.description,
            }
        )

    models = {
        "staging": r.m_stg,
        "daily_partials": r.m_partials,
        "window_rollup": r.m_rollup if spec.bounded_windows else None,
        "alltime_state": r.m_state if spec.has_all_time else None,
        "alltime_recent": r.m_recent if r.has_recent else None,
        "mart": r.m_mart,
    }

    return {
        "feature_name": spec.feature_name,
        "spec_version": spec.spec_hash,
        "feature_type": spec.feature_type,
        "owner": spec.created_by,
        "description": spec.description,
        "generated_at": generated_at or datetime.now(UTC).isoformat(timespec="seconds"),
        "generator_version": __import__("generator").__version__,
        "entities": list(spec.entities),
        "timestamp_col": spec.timestamp_col,
        "sources": [
            {
                "literal": rel.literal,
                "dbt_source": rel.source_name,
                "dbt_table": rel.table_name,
            }
            for rel in spec.relations.values()
        ],
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
