"""Project-level artefacts that span every feature spec.

Sources and their staging models live here rather than on the renderer because
two specs can legitimately read the same physical table, and dbt requires
exactly one definition per source. Emitting one file per spec produced a
duplicate-source error the moment a second spec was added -- the kind of
failure that only appears once the system has more than one user.

Each source table gets exactly one staging model, `stg_<source>__<table>`, and
it is the only model that reads the source. Every spec reads the table through
it, so lineage has one edge per table rather than one per consumer, and the
knowledge-time columns the point-in-time reads depend on are normalised once:
renamed to fixed names and moved onto UTC.

Groups are project-wide for the same reason: several specs can share an owner.
"""

from __future__ import annotations

import textwrap
from dataclasses import dataclass
from pathlib import Path

import yaml

from generator.render import group_name
from generator.spec import LOADED_AT, SUPERSEDED_AT, FeatureSpec, SourceRelation, SpecError

HEADER = (
    "# ============================================================================\n"
    "# GENERATED FILE - DO NOT EDIT BY HAND\n"
    "#   Merged from every feature spec that reads this source.\n"
    "#   Edit a spec and run `make generate`.\n"
    "# ============================================================================\n"
)

SQL_HEADER = """\
-- ============================================================================
-- GENERATED FILE - DO NOT EDIT BY HAND
--
--   layer      : staging
--   source     : {source}.{table}
--   read by    : {consumers}
--   generator  : featurefold
--
-- Generated from the `relations:` block of the specs above. Edit those and
-- run `make generate`.
-- ============================================================================
"""


@dataclass
class SourceTable:
    """One physical table, with every spec that reads it."""

    relation: SourceRelation
    consumers: list[str]


def collect_sources(specs: list[FeatureSpec]) -> dict[str, dict[str, SourceTable]]:
    """source_name -> table_name -> the table, checking that every spec agrees on it.

    The knowledge-time columns and the time zone are facts about the table, and
    the one staging model encodes them for every reader. Two specs declaring
    them differently is a contradiction to resolve, not a choice to make.
    """
    tree: dict[str, dict[str, SourceTable]] = {}
    for spec in specs:
        rel = spec.relation
        tables = tree.setdefault(rel.source_name, {})
        known = tables.get(rel.table_name)
        if known is None:
            tables[rel.table_name] = SourceTable(rel, [spec.feature_name])
            continue
        if known.relation.contract() != rel.contract():
            raise SpecError(
                f"{rel.source_name}.{rel.table_name} is declared differently by "
                f"{known.consumers[0]} and {spec.feature_name}:\n"
                f"    {known.relation.contract()}\n"
                f"    {rel.contract()}\n"
                "These describe the table itself, so every spec reading it must agree."
            )
        known.consumers.append(spec.feature_name)
    return tree


def render_staging_model(table: SourceTable) -> str:
    rel = table.relation

    def to_utc(column: str) -> str:
        return f"{{{{ fs_convert_tz('src.{column}', '{rel.timezone}', 'UTC') }}}}"

    superseded = to_utc(rel.superseded_at) if rel.superseded_at else "cast(null as timestamp)"
    kind = (
        "versioned: a correction or deletion closes a row version"
        if rel.superseded_at
        else "insert-only: a row version, once loaded, is never replaced"
    )
    return SQL_HEADER.format(
        source=rel.source_name,
        table=rel.table_name,
        consumers=", ".join(sorted(table.consumers)),
    ) + textwrap.dedent(f"""
        -- The only model that reads this source, 1:1 with it. It passes every
        -- column through untouched and adds the two knowledge-time columns every
        -- point-in-time read depends on, under fixed names and in UTC:
        --
        --   {LOADED_AT}       when the row version became visible ({rel.loaded_at})
        --   {SUPERSEDED_AT}   when it was replaced or deleted, NULL if never
        --
        -- The table is {kind}.
        -- Its naive timestamps are recorded in {rel.timezone}.

        {{{{ config(materialized='view', tags=['feature_store']) }}}}

        select
            -- Every column, on purpose: specs name the ones they read, and a
            -- 1:1 staging model has no business choosing among them.
            src.*,  -- noqa: AM04
            {to_utc(rel.loaded_at)} as {LOADED_AT},
            {superseded} as {SUPERSEDED_AT}
        from {rel.dbt_source} as src
        """)


def render_sources_yml(source_name: str, tables: dict[str, SourceTable]) -> str:
    rendered = []
    for table_name in sorted(tables):
        table = tables[table_name]
        rel = table.relation
        columns = [
            {
                "name": rel.loaded_at,
                "description": f"When each row version became visible ({rel.timezone}).",
                "data_tests": ["not_null"],
            }
        ]
        if rel.superseded_at:
            columns.append(
                {
                    "name": rel.superseded_at,
                    "description": (
                        f"When a row version was replaced or deleted ({rel.timezone}). "
                        "Open versions carry NULL or a far-future sentinel."
                    ),
                }
            )
        rendered.append(
            {
                "name": table_name,
                "description": (
                    (
                        "Versioned source: a correction closes the old row version and opens "
                        "a new one, a deletion closes it without a successor. Both are read: "
                        "the feature store finds the days they change and rewrites them."
                        if rel.superseded_at
                        else "Insert-only source: a row version, once loaded, never changes."
                    )
                    + f"\nRead only by {rel.staging_model}, for: "
                    + ", ".join(sorted(table.consumers))
                ),
                "loaded_at_field": rel.loaded_at,
                "freshness": {
                    "warn_after": {"count": 12, "period": "hour"},
                    "error_after": {"count": 36, "period": "hour"},
                },
                "columns": columns,
            }
        )
    doc = {
        "version": 2,
        "sources": [
            {
                "name": source_name,
                "description": f"Upstream source feeding {len(rendered)} table(s).",
                "schema": "{{ env_var('FS_BRONZE_SCHEMA', '" + source_name + "') }}",
                "tables": rendered,
            }
        ],
    }
    return HEADER + yaml.safe_dump(doc, sort_keys=False, width=100)


def render_staging_yml(source_name: str, tables: dict[str, SourceTable]) -> str:
    models = []
    for table_name in sorted(tables):
        rel = tables[table_name].relation
        tests: list[dict] = [
            {"fs_ordered_interval": {"arguments": {"start": LOADED_AT, "end": SUPERSEDED_AT}}}
        ]
        if rel.key:
            # One row per key and version: the staging model's primary key.
            cols = [*rel.key, LOADED_AT]
            tests.append({"fs_unique_combination": {"arguments": {"combination_of_columns": cols}}})
            if rel.superseded_at:
                tests.append(
                    {
                        "fs_no_overlapping_versions": {
                            "arguments": {
                                "key": list(rel.key),
                                "start": LOADED_AT,
                                "end": SUPERSEDED_AT,
                            }
                        }
                    }
                )
        models.append(
            {
                "name": rel.staging_model,
                "description": (
                    f"Every row version of {source_name}.{table_name}, with its knowledge-time "
                    "columns normalised to UTC under fixed names."
                ),
                "data_tests": tests,
                "columns": [
                    {
                        "name": LOADED_AT,
                        "description": "When this row version became visible, in UTC.",
                        "data_tests": ["not_null"],
                    },
                    {
                        "name": SUPERSEDED_AT,
                        "description": "When it was replaced or deleted, in UTC; NULL if never.",
                    },
                ],
            }
        )
    return HEADER + yaml.safe_dump({"version": 2, "models": models}, sort_keys=False, width=100)


def render_groups_yml(specs: list[FeatureSpec]) -> str:
    owners = sorted({spec.created_by for spec in specs})
    groups = [{"name": group_name(o), "owner": {"name": o}} for o in owners]
    header = HEADER.replace(
        "Merged from every feature spec that reads this source.",
        "One group per spec owner (created_by).",
    )
    return header + yaml.safe_dump({"version": 2, "groups": groups}, sort_keys=False, width=100)


def render_project(specs: list[FeatureSpec], dbt_root: Path) -> list[tuple[Path, str]]:
    """Every project-wide file, as (path, content)."""
    models = dbt_root / "models"
    out: list[tuple[Path, str]] = [
        (models / "_feature_store__groups.yml", render_groups_yml(specs))
    ]
    for source_name, tables in sorted(collect_sources(specs).items()):
        folder = models / "staging" / source_name
        out.append(
            (folder / f"_{source_name}__sources.yml", render_sources_yml(source_name, tables))
        )
        out.append(
            (folder / f"_{source_name}__models.yml", render_staging_yml(source_name, tables))
        )
        for table_name in sorted(tables):
            table = tables[table_name]
            out.append(
                (folder / f"{table.relation.staging_model}.sql", render_staging_model(table))
            )
    return out
