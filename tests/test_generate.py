"""Generation is deterministic, complete, and drift-detectable.

The GitOps contract is that a spec and its generated models cannot disagree.
That only holds if generation is a pure function of the spec, so determinism is
tested directly rather than assumed.
"""

import json

import pytest
from click.testing import CliRunner

from generator.cli import cli
from generator.expand import build_plan
from generator.registry import build_registry
from generator.render import Renderer
from generator.spec import load_spec

REAL = "features/fact_agg_features_login_history_v2.yml"


@pytest.fixture(scope="module")
def plan():
    return build_plan(load_spec(REAL))


def test_generation_is_deterministic(plan, tmp_path):
    a = {f.path.name: f.content for f in Renderer(plan).render_all(tmp_path)}
    b = {f.path.name: f.content for f in Renderer(build_plan(load_spec(REAL))).render_all(tmp_path)}
    assert a == b


def test_every_layer_is_emitted(plan, tmp_path):
    names = {f.path.name for f in Renderer(plan).render_all(tmp_path)}
    fn = plan.spec.feature_name
    assert names == {
        f"stg_{fn}.sql",
        f"int_{fn}__daily_partials.sql",
        f"int_{fn}__window_rollup.sql",
        f"int_{fn}__alltime_state.sql",
        f"int_{fn}__alltime_recent.sql",
        f"{fn}.sql",
        f"_{fn}__models.yml",
        f"assert_{fn}_non_negative.sql",
        f"assert_{fn}_window_monotonicity.sql",
        f"assert_{fn}_internal_coherence.sql",
        f"assert_{fn}_state_watermark.sql",
    }


def test_generated_sql_has_no_unresolved_placeholders(plan, tmp_path):
    for f in Renderer(plan).render_all(tmp_path):
        if f.path.suffix == ".sql":
            assert "{{ target_date }}" not in f.content, f.path.name
            assert "{0}" not in f.content and "{1}" not in f.content, f.path.name


def test_relations_sharing_a_prefix_resolve_to_their_own_sources(write_spec, base_spec):
    """Substituting `bronze.db.events` must not rewrite part of `bronze.db.events_blocklist`."""
    src = base_spec.replace(
        "  WHERE customer_id IS NOT NULL",
        "  WHERE customer_id NOT IN (SELECT id FROM bronze.db.events_blocklist)",
    ) + ("  bronze.db.events_blocklist:\n    source_name: bronze\n    table_name: blocklist\n")
    staging = Renderer(build_plan(load_spec(write_spec(src)))).render_staging()
    assert "FROM {{ source('bronze', 'events') }}\n" in staging
    assert "FROM {{ source('bronze', 'blocklist') }})" in staging
    assert "bronze.db." not in staging


def test_every_generated_file_is_marked_generated(plan, tmp_path):
    for f in Renderer(plan).render_all(tmp_path):
        assert "GENERATED FILE - DO NOT EDIT" in f.content


def test_mart_publishes_exactly_the_planned_features(plan, tmp_path):
    mart = next(
        f
        for f in Renderer(plan).render_all(tmp_path)
        if f.path.name == f"{plan.spec.feature_name}.sql"
    )
    body = mart.content
    for name in plan.output_order:
        assert f" {name}" in body or f"as {name}" in body


def test_no_layer_can_see_past_the_as_of_date(plan, tmp_path):
    """Point-in-time safety must be structural, not a convention."""
    files = {f.path.name: f.content for f in Renderer(plan).render_all(tmp_path)}
    fn = plan.spec.feature_name
    partials = files[f"int_{fn}__daily_partials.sql"]
    assert "event_date <= {{ fs_date_offset_lit(0) }}" in partials

    tail = files[f"int_{fn}__alltime_recent.sql"]
    assert "event_date <= {{ fs_date_offset_lit(0) }}" in tail


def test_partial_layer_never_recomputes_under_an_earlier_view(plan, tmp_path):
    """A backwards replay must not drop events a later run already captured."""
    files = {f.path.name: f.content for f in Renderer(plan).render_all(tmp_path)}
    partials = files[f"int_{plan.spec.feature_name}__daily_partials.sql"]
    assert "already_fresher" in partials
    assert "_computed_for > {{ fs_date_offset_lit(0) }}" in partials
    assert "event_date not in (select event_date from already_fresher)" in partials


def test_rewrite_window_is_anchored_to_the_last_processed_run(plan, tmp_path):
    """A missed day leaves earlier days incomplete; the window must reach back to them."""
    files = {f.path.name: f.content for f in Renderer(plan).render_all(tmp_path)}
    partials = files[f"int_{plan.spec.feature_name}__daily_partials.sql"]
    assert "rewrite_window" in partials
    assert "max(_computed_for)" in partials
    assert "fs_least2('w.last_run'" in partials


def test_unsealed_tail_starts_at_the_actual_watermark(plan, tmp_path):
    """Anchoring the tail to target_date instead double-counts on a backwards run."""
    files = {f.path.name: f.content for f in Renderer(plan).render_all(tmp_path)}
    fn = plan.spec.feature_name
    tail = files[f"int_{fn}__alltime_recent.sql"]
    assert "max(_state_as_of_date)" in tail
    assert "event_date > w.wm" in tail
    # The guard for the case the tail cannot repair.
    assert f"assert_{fn}_state_watermark.sql" in {k for k in files}


def test_registry_describes_every_published_feature(plan):
    reg = build_registry(plan, generated_at="fixed")
    assert reg["expansion"]["total_features"] == 480
    assert len(reg["features"]) == 480
    assert reg["spec_version"] == plan.spec.spec_hash

    f = next(f for f in reg["features"] if f["name"] == "count_event_id_is_ios_is_late_night_l7d")
    assert f["agg"] == "count"
    assert f["window"] == "l7d"
    assert f["window_days"] == 7
    assert f["conditions"] == {
        "login_status": "default",
        "login_os": "is_ios",
        "behavioural_time": "is_late_night",
    }
    assert f["materialisation"] == "stored"

    d = next(f for f in reg["features"] if f["name"] == "min_days_since_login_all_time")
    assert d["materialisation"] == "computed"
    assert d["computed_kind"] == "days_since"
    assert d["inputs"] == ["max_event_timestamp_all_time"]

    json.dumps(reg)  # must stay serialisable


def test_check_mode_passes_on_committed_output():
    res = CliRunner().invoke(cli, ["generate", "--check"])
    assert res.exit_code == 0, res.output
    assert "up to date" in res.output


def test_check_mode_detects_a_hand_edited_model(tmp_path):
    """Editing generated SQL by hand must fail CI rather than silently persist."""
    from pathlib import Path

    target = Path("transform/models/marts/fact_agg_features_login_history_v2.sql")
    original = target.read_text()
    try:
        target.write_text(original + "\n-- someone edited this by hand\n")
        res = CliRunner().invoke(cli, ["generate", "--check"])
        assert res.exit_code == 1
        assert "out of date" in res.output
    finally:
        target.write_text(original)


def test_distinct_never_exceeds_count_when_both_are_declared(tmp_path):
    """The invariant that catches a NULL or duplicate leaking into a key set."""
    spec_yaml = """
feature_name: "t"
feature_type: "daily"
created_by: "t"
source: |
  SELECT customer_id AS safe_id, device_id, event_timestamp, os_name
  FROM bronze.db.events WHERE customer_id IS NOT NULL
entities: ["safe_id"]
timestamp_col: "event_timestamp"
condition_cat:
  - login_os:
    - default: "TRUE"
    - is_ios: "UPPER(os_name) = 'IOS'"
time_cat: ["l7d"]
atomic_field:
  - device_id:
      apply_cond_cat: [login_os]
      agg: ['count', 'count_distinct']
relations:
  bronze.db.events: {source_name: bronze, table_name: events}
"""
    path = tmp_path / "s.yml"
    path.write_text(spec_yaml)
    p = build_plan(load_spec(path))
    files = {f.path.name: f.content for f in Renderer(p).render_all(tmp_path)}
    coherence = files["assert_t_internal_coherence.sql"]
    assert "count_distinct_device_id_l7d > count_device_id_l7d" in coherence
    assert "count_distinct_device_id_is_ios_l7d > count_device_id_is_ios_l7d" in coherence


def test_approximate_distinct_is_exempt_from_exact_invariants(tmp_path):
    """A sketch may legitimately over- or under-count above k."""
    spec_yaml = """
feature_name: "t"
feature_type: "daily"
created_by: "t"
source: |
  SELECT customer_id AS safe_id, event_id, event_timestamp, os_name
  FROM bronze.db.events WHERE customer_id IS NOT NULL
entities: ["safe_id"]
timestamp_col: "event_timestamp"
condition_cat:
  - login_os:
    - default: "TRUE"
    - is_ios: "UPPER(os_name) = 'IOS'"
time_cat: ["l7d", "l30d"]
atomic_field:
  - event_id:
      apply_cond_cat: [login_os]
      agg: ['count', 'count_distinct']
      distinct_method: approx
relations:
  bronze.db.events: {source_name: bronze, table_name: events}
"""
    path = tmp_path / "s.yml"
    path.write_text(spec_yaml)
    p = build_plan(load_spec(path))
    files = {f.path.name: f.content for f in Renderer(p).render_all(tmp_path)}
    coherence = files["assert_t_internal_coherence.sql"]
    mono = files["assert_t_window_monotonicity.sql"]
    assert "count_distinct_event_id_l7d > count_event_id_l7d" not in coherence
    assert "count_distinct_event_id_l7d > count_distinct_event_id_l30d" not in mono
    # The exact count on the same field is still checked.
    assert "count_event_id_l7d > count_event_id_l30d" in mono
