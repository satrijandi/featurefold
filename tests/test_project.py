"""The project-wide files: one staging model per source table, sources, groups.

Several specs can read one table and share one owner, so these are merged
across specs, and the merge refuses to paper over two specs that describe the
same table differently.
"""

from pathlib import Path

import pytest
import yaml

from generator.project import collect_sources, render_project
from generator.spec import SpecError, load_spec

SPECS = sorted(Path("features").glob("*.yml"))


@pytest.fixture(scope="module")
def files() -> dict[str, str]:
    specs = [load_spec(p) for p in SPECS]
    return {p.as_posix(): c for p, c in render_project(specs, Path("transform"))}


def test_each_source_table_gets_exactly_one_staging_model(files):
    staging = [p for p in files if p.endswith(".sql")]
    assert staging == [
        "transform/models/staging/bronze_events/stg_bronze_events__customer_login.sql"
    ]
    sql = files[staging[0]]
    assert "from {{ source('bronze_events', 'customer_login') }} as src" in sql
    assert (
        "read by    : fact_agg_features_login_device_v1, fact_agg_features_login_history_v2" in sql
    )


def test_staging_normalises_knowledge_time_to_utc(files):
    sql = files["transform/models/staging/bronze_events/stg_bronze_events__customer_login.sql"]
    assert "{{ fs_convert_tz('src._scd_valid_from', 'UTC', 'UTC') }} as _fs_loaded_at" in sql
    assert "{{ fs_convert_tz('src._scd_valid_to', 'UTC', 'UTC') }} as _fs_superseded_at" in sql


def test_an_insert_only_source_has_no_superseded_column(write_spec):
    files = {p.name: c for p, c in render_project([load_spec(write_spec())], Path("t"))}
    assert "cast(null as timestamp) as _fs_superseded_at" in files["stg_bronze__events.sql"]


def test_sources_declare_freshness_and_test_knowledge_time(files):
    doc = yaml.safe_load(
        files["transform/models/staging/bronze_events/_bronze_events__sources.yml"]
    )
    (table,) = doc["sources"][0]["tables"]
    assert table["loaded_at_field"] == "_scd_valid_from"
    assert {"name": "_scd_valid_from"}.items() <= table["columns"][0].items()
    assert table["columns"][0]["data_tests"] == ["not_null"]

    models = yaml.safe_load(
        files["transform/models/staging/bronze_events/_bronze_events__models.yml"]
    )
    (stg,) = models["models"]
    assert stg["data_tests"] == [
        {
            "fs_ordered_interval": {
                "arguments": {"start": "_fs_loaded_at", "end": "_fs_superseded_at"},
            }
        },
        # The row key across versions makes the history itself testable.
        {
            "fs_unique_combination": {
                "arguments": {"combination_of_columns": ["event_id", "_fs_loaded_at"]},
            }
        },
        {
            "fs_no_overlapping_versions": {
                "arguments": {
                    "key": ["event_id"],
                    "start": "_fs_loaded_at",
                    "end": "_fs_superseded_at",
                },
            }
        },
    ]


def test_owners_become_groups(files):
    doc = yaml.safe_load(files["transform/models/_feature_store__groups.yml"])
    assert doc["groups"] == [{"name": "data_platform", "owner": {"name": "data-platform"}}]


def test_specs_that_disagree_about_a_table_are_rejected(write_spec, base_spec, tmp_path):
    a = load_spec(write_spec(base_spec))
    other = tmp_path / "other.yml"
    other.write_text(
        base_spec.replace("test_feature", "other_feature").replace(
            "loaded_at: _loaded_at", "loaded_at: _ingested_at"
        )
    )
    with pytest.raises(SpecError, match="declared differently"):
        collect_sources([a, load_spec(other)])
