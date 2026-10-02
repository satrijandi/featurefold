"""Cross-product expansion, naming, and the derived-field machinery."""

import pytest

from generator.expand import build_plan
from generator.spec import SpecError, load_spec

REAL = "features/fact_agg_features_login_history_v2.yml"


@pytest.fixture(scope="module")
def real_plan():
    return build_plan(load_spec(REAL))


def test_expansion_is_the_full_cross_product(real_plan):
    # event_id      3 status x 4 os x 5 time x 1 agg x 4 windows = 240
    # device_id     3 x 4 x 1 agg x 4                            =  48
    # event_ts      3 x 4 x 2 aggs x 4                           =  96
    # days_since    3 x 4 x 2 aggs x 4                           =  96
    assert real_plan.feature_count == 480


def test_default_members_produce_the_marginals(real_plan):
    """`default: TRUE` is what makes uncut features fall out of the same product."""
    names = set(real_plan.output_order)
    assert "count_event_id_l7d" in names
    assert "count_event_id_is_ios_l7d" in names
    assert "count_event_id_is_login_success_is_ios_is_late_night_l7d" in names


def test_naming_follows_agg_field_conditions_window(real_plan):
    for n in real_plan.output_order:
        assert not n.startswith("_") and n == n.lower()
    assert "count_event_id_is_login_failed_is_android_is_office_hours_all_time" in set(
        real_plan.output_order
    )


def test_partial_layer_is_far_smaller_than_the_published_surface(real_plan):
    """96 stored columns per entity-day back 480 published features."""
    assert len(real_plan.partials) == 96
    assert real_plan.feature_count == 480


def test_days_since_is_computed_not_stored(real_plan):
    computed = {c.name for c in real_plan.computed}
    assert len(computed) == 96
    assert all("days_since_login" in n for n in computed)
    stored = {s.name for s in real_plan.stored}
    assert not any("days_since" in n for n in stored)


def test_days_since_min_is_derived_from_the_max_timestamp(real_plan):
    """min(days since) is the distance to the LATEST event, so it flips the aggregation."""
    c = next(c for c in real_plan.computed if c.name == "min_days_since_login_is_ios_l7d")
    assert c.inputs == ("max_event_timestamp_is_ios_l7d",)
    assert "fs_datediff_day" in c.expr.render()

    c = next(c for c in real_plan.computed if c.name == "max_days_since_login_is_ios_l7d")
    assert c.inputs == ("min_event_timestamp_is_ios_l7d",)


def test_every_output_name_is_unique(real_plan):
    assert len(real_plan.output_order) == len(set(real_plan.output_order))


def test_marginal_combo_carries_a_true_predicate(real_plan):
    marginal = [p for p in real_plan.partials if p.combo.is_marginal]
    assert marginal and all(p.combo.predicate == "TRUE" for p in marginal)


def test_derived_field_pulls_in_internal_partials_when_the_base_is_not_requested(tmp_path):
    """days_since over combos the timestamp does not publish must still resolve."""
    spec_yaml = """
feature_name: "t"
feature_type: "daily"
created_by: "t"
source: |
  SELECT
    customer_id AS safe_id,
    event_timestamp,
    DATEDIFF(DAY, event_timestamp, '{{ target_date }}'::DATE) AS days_since_login,
    os_name
  FROM bronze.db.events
  WHERE customer_id IS NOT NULL
entities: ["safe_id"]
timestamp_col: "event_timestamp"
condition_cat:
  - login_os:
    - default: "TRUE"
    - is_ios: "UPPER(os_name) = 'IOS'"
time_cat: ["l7d"]
atomic_field:
  - days_since_login:
      apply_cond_cat: [login_os]
      agg: ['min']
      field_type: numeric
relations:
  bronze.db.events: {source_name: bronze, table_name: events, loaded_at: _loaded_at}
"""
    p = tmp_path / "s.yml"
    p.write_text(spec_yaml)
    plan = build_plan(load_spec(p))

    # Only days_since is published, but the max(event_timestamp) partials it is
    # rebuilt from must exist, carried internally.
    assert plan.output_order == ["min_days_since_login_l7d", "min_days_since_login_is_ios_l7d"]
    assert {p.name for p in plan.partials} == {
        "p_max_event_timestamp",
        "p_max_event_timestamp_is_ios",
    }
    assert all(p.internal for p in plan.partials)
    assert all(s.internal for s in plan.stored)


def test_overlong_generated_names_are_rejected(tmp_path):
    long_member = "is_" + "x" * 240
    spec_yaml = f"""
feature_name: "t"
feature_type: "daily"
created_by: "t"
source: |
  SELECT customer_id AS safe_id, event_id, event_timestamp, os_name
  FROM bronze.db.events WHERE customer_id IS NOT NULL
entities: ["safe_id"]
timestamp_col: "event_timestamp"
condition_cat:
  - c:
    - default: "TRUE"
    - {long_member}: "1=1"
time_cat: ["l7d"]
atomic_field:
  - event_id:
      apply_cond_cat: [c]
      agg: ['count']
relations:
  bronze.db.events: {{source_name: bronze, table_name: events, loaded_at: _loaded_at}}
"""
    p = tmp_path / "s.yml"
    p.write_text(spec_yaml)
    with pytest.raises(SpecError, match="over the 255-character limit"):
        build_plan(load_spec(p))
