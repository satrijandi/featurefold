"""Generation is deterministic, complete, and drift-detectable.

The GitOps contract is that a spec and its generated models cannot disagree.
That only holds if generation is a pure function of the spec, so determinism is
tested directly rather than assumed.
"""

import json

import pytest
import yaml
from click.testing import CliRunner

from generator.cli import cli
from generator.expand import build_plan
from generator.registry import build_registry
from generator.render import Renderer
from generator.spec import SourceRelation, load_spec, substitute_relations

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
        f"int_{fn}__events.sql",
        f"int_{fn}__daily_partials.sql",
        f"int_{fn}__window_rollup.sql",
        f"int_{fn}__alltime_state.sql",
        f"int_{fn}__alltime_recent.sql",
        f"{fn}.sql",
        f"_{fn}__models.yml",
        f"_int_{fn}__models.yml",
        f"assert_{fn}_non_negative.sql",
        f"assert_{fn}_window_monotonicity.sql",
        f"assert_{fn}_internal_coherence.sql",
        f"assert_{fn}_state_watermark.sql",
        f"assert_{fn}_state_version.sql",
    }


def test_generated_sql_has_no_unresolved_placeholders(plan, tmp_path):
    for f in Renderer(plan).render_all(tmp_path):
        if f.path.suffix == ".sql":
            assert "{{ target_date }}" not in f.content, f.path.name
            assert "{0}" not in f.content and "{1}" not in f.content, f.path.name


def test_relations_sharing_a_prefix_resolve_to_their_own_staging_models():
    """Substituting `bronze.db.events` must not rewrite part of `bronze.db.events_blocklist`."""
    rels = {
        lit: SourceRelation(literal=lit, source_name="bronze", table_name=table, loaded_at="t")
        for lit, table in (("bronze.db.events", "events"), ("bronze.db.events_blocklist", "bl"))
    }
    sql = "FROM bronze.db.events WHERE id NOT IN (SELECT id FROM bronze.db.events_blocklist)"
    out = substitute_relations(sql, rels)
    assert "FROM {{ ref('stg_bronze__events') }} WHERE" in out
    assert "FROM {{ ref('stg_bronze__bl') }})" in out


def test_every_spec_reads_its_source_through_the_shared_staging_model(plan, tmp_path):
    files = {f.path.name: f.content for f in Renderer(plan).render_all(tmp_path)}
    events = files[f"int_{plan.spec.feature_name}__events.sql"]
    assert "{{ ref('stg_bronze_events__customer_login') }}" in events
    assert "source(" not in "".join(files.values())


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
    assert "event_date not in (select already_fresher.event_date from already_fresher)" in partials


def test_partials_rewrite_exactly_the_days_that_changed(plan, tmp_path):
    """Late arrivals, corrections and deletions each announce the day they change."""
    files = {f.path.name: f.content for f in Renderer(plan).render_all(tmp_path)}
    partials = files[f"int_{plan.spec.feature_name}__daily_partials.sql"]
    # Anchored to what the last run knew, so a gap is covered whole.
    assert "max(_known_through)" in partials
    assert "or e.event_date > r.as_of" in partials
    assert "e._fs_loaded_at >= r.known_through" in partials
    assert "e._fs_superseded_at >= r.known_through" in partials
    # Reads only the versions current when the as-of day ended.
    assert "e._fs_superseded_at is null or e._fs_superseded_at >=" in partials
    assert "fs_knowledge_cutoff('Asia/Jakarta')" in partials
    # A day is replaced whole, and an emptied entity-day becomes a tombstone.
    assert "unique_key=['event_date']" in partials
    assert "select distinct\n        safe_id,\n        event_date\n    from visible" in partials
    assert "keys.safe_id = visible.safe_id" in partials
    assert "count(_fs_loaded_at) as _n_rows" in partials
    # No fixed look-back window that a very late change could fall behind.
    assert "fs_least2('w.last_run'" not in partials


def test_accumulator_refolds_entities_whose_sealed_history_changed(plan, tmp_path):
    files = {f.path.name: f.content for f in Renderer(plan).render_all(tmp_path)}
    state = files[f"int_{plan.spec.feature_name}__alltime_state.sql"]
    assert "p._known_through > s.folded_through" in state
    assert "and (p.event_date > s.wm or r.safe_id is not null)" in state
    # A restated entity takes nothing from its previous state.
    assert "left join prev on n.safe_id = prev.safe_id and n._restated = 0" in state
    assert "as _folded_through" in state


def test_timestamps_are_read_on_the_business_clock(plan, tmp_path):
    files = {f.path.name: f.content for f in Renderer(plan).render_all(tmp_path)}
    events = files[f"int_{plan.spec.feature_name}__events.sql"]
    clock = """{{ fs_convert_tz("event_timestamp", 'UTC', 'Asia/Jakarta') }}"""
    assert f"{clock} as event_timestamp" in events
    assert f"cast({clock} as date) as event_date" in events


def test_incremental_models_fail_on_schema_change(plan, tmp_path):
    """Syncing would publish a new column as NULL across all history, read as absence."""
    for f in Renderer(plan).render_all(tmp_path):
        if "materialized='incremental'" in f.content:
            assert "on_schema_change='fail'" in f.content, f.path.name
        assert "sync_all_columns" not in f.content, f.path.name


def test_mart_contract_is_enforced_and_typed(plan, tmp_path):
    files = {f.path.name: f.content for f in Renderer(plan).render_all(tmp_path)}
    fn = plan.spec.feature_name
    doc = yaml.safe_load(files[f"_{fn}__models.yml"])
    mart = next(m for m in doc["models"] if m["name"] == fn)
    assert mart["config"] == {
        "access": "public",
        "group": "data_platform",
        "contract": {"enforced": True},
    }
    assert all("data_type" in c for c in mart["columns"])
    by_name = {c["name"]: c for c in mart["columns"]}
    assert by_name["target_date"]["constraints"] == [{"type": "not_null"}]
    assert by_name["safe_id"]["data_type"] == "string"
    assert by_name["count_event_id_l7d"]["data_type"] == "bigint"
    assert by_name["max_event_timestamp_all_time"]["data_type"] == "timestamp"
    # Every published value is cast to the declared type.
    sql = files[f"{fn}.sql"]
    assert "cast(count_event_id_l7d as bigint) as count_event_id_l7d" in sql
    # Everything below the mart is private to the owner, and tested for its key.
    below = yaml.safe_load(files[f"_int_{fn}__models.yml"])["models"]
    assert {m["name"] for m in below} == {
        f"int_{fn}__events",
        f"int_{fn}__daily_partials",
        f"int_{fn}__window_rollup",
        f"int_{fn}__alltime_state",
        f"int_{fn}__alltime_recent",
    }
    for m in below:
        assert m["config"] == {"access": "private", "group": "data_platform"}, m["name"]
        if m["name"] != f"int_{fn}__events":
            assert "fs_unique_combination" in m["data_tests"][0], m["name"]


def test_each_layer_is_documented_beside_its_models(plan, tmp_path):
    paths = {f.path.name: f.path.relative_to(tmp_path) for f in Renderer(plan).render_all(tmp_path)}
    fn = plan.spec.feature_name
    assert paths[f"_{fn}__models.yml"].parent.as_posix() == "models/marts"
    assert paths[f"_int_{fn}__models.yml"].parent.as_posix() == f"models/intermediate/{fn}"
    assert paths[f"int_{fn}__daily_partials.sql"].parent.as_posix() == f"models/intermediate/{fn}"


def test_exposures_hang_off_the_mart(plan, tmp_path):
    files = {f.path.name: f.content for f in Renderer(plan).render_all(tmp_path)}
    fn = plan.spec.feature_name
    doc = yaml.safe_load(files[f"_{fn}__models.yml"])
    names = {e["name"] for e in doc["exposures"]}
    assert names == {"login_offline_store", "login_feature_validation"}
    assert all(e["depends_on"] == [f"ref('{fn}')"] for e in doc["exposures"])


def test_tests_reading_private_state_belong_to_its_group(plan, tmp_path):
    for f in Renderer(plan).render_all(tmp_path):
        if f.path.name.startswith("assert_"):
            assert "{{ config(group='data_platform') }}" in f.content, f.path.name


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
  bronze.db.events: {source_name: bronze, table_name: events, loaded_at: _loaded_at}
"""
    path = tmp_path / "s.yml"
    path.write_text(spec_yaml)
    p = build_plan(load_spec(path))
    files = {f.path.name: f.content for f in Renderer(p).render_all(tmp_path)}
    coherence = files["assert_t_internal_coherence.sql"]
    assert "count_distinct_device_id_l7d > count_device_id_l7d" in coherence
    assert "count_distinct_device_id_is_ios_l7d > count_device_id_is_ios_l7d" in coherence


def test_approximate_distinct_is_ordered_but_not_bounded_by_count(tmp_path):
    """A sketch can overshoot the true count above k, but never shrinks as rows are added."""
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
  bronze.db.events: {source_name: bronze, table_name: events, loaded_at: _loaded_at}
"""
    path = tmp_path / "s.yml"
    path.write_text(spec_yaml)
    p = build_plan(load_spec(path))
    files = {f.path.name: f.content for f in Renderer(p).render_all(tmp_path)}
    coherence = files["assert_t_internal_coherence.sql"]
    mono = files["assert_t_window_monotonicity.sql"]
    assert "count_distinct_event_id_l7d > count_event_id_l7d" not in coherence
    assert "count_distinct_event_id_l7d > count_distinct_event_id_l30d" in mono
    assert "count_event_id_l7d > count_event_id_l30d" in mono


AVG_SPEC = """
feature_name: "t"
feature_type: "daily"
created_by: "t"
source: |
  SELECT customer_id AS safe_id, amount, event_timestamp, os_name
  FROM bronze.db.events WHERE customer_id IS NOT NULL
entities: ["safe_id"]
timestamp_col: "event_timestamp"
condition_cat:
  - login_os:
    - default: "TRUE"
    - is_ios: "UPPER(os_name) = 'IOS'"
time_cat: ["l7d", "all_time"]
atomic_field:
  - amount:
      apply_cond_cat: [login_os]
      field_type: numeric
      agg: ['sum', 'min', 'max', 'avg']
relations:
  bronze.db.events: {source_name: bronze, table_name: events, loaded_at: _loaded_at}
"""


def _rendered(tmp_path, spec_yaml):
    path = tmp_path / "s.yml"
    path.write_text(spec_yaml)
    p = build_plan(load_spec(path))
    return {f.path.name: f.content for f in Renderer(p).render_all(tmp_path)}


def test_averages_and_signed_sums_are_not_asserted_to_grow_with_the_window(tmp_path):
    """A 7-day mean below the all-time mean is ordinary data, not a broken fold.

    This used to emit `avg_amount_l7d < avg_amount_all_time` as a violation,
    which failed the build for every entity whose recent mean dipped.
    """
    files = _rendered(tmp_path, AVG_SPEC)
    mono = files["assert_t_window_monotonicity.sql"]
    coherence = files["assert_t_internal_coherence.sql"]
    assert "avg_amount" not in mono
    assert "avg_amount" not in coherence
    # A signed field can make a wider sum smaller.
    assert "sum_amount" not in mono
    assert "sum_amount_l7d" not in files["assert_t_non_negative.sql"]
    # The extrema over the same field are still ordered both ways.
    assert "min_amount_l7d < min_amount_all_time" in mono
    assert "max_amount_l7d > max_amount_all_time" in mono
    assert "min_amount_l7d > min_amount_is_ios_l7d" in coherence
    assert "max_amount_l7d < max_amount_is_ios_l7d" in coherence
    assert "min_amount_is_ios_l7d > max_amount_is_ios_l7d" in coherence


def test_days_since_can_never_be_negative(plan, tmp_path):
    """A negative distance to the as-of date means a future event reached the fold."""
    files = {f.path.name: f.content for f in Renderer(plan).render_all(tmp_path)}
    non_negative = files[f"assert_{plan.spec.feature_name}_non_negative.sql"]
    assert "min_days_since_login_l7d < 0" in non_negative
    assert "max_days_since_login_all_time < 0" in non_negative
