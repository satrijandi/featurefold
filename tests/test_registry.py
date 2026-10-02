"""The registry reads back exactly what the compiler wrote.

Deployments only ever see the registry through `Registry`, so these tests cross
the same interface they do: build from a plan, serialise, read back, ask.
"""

import json
import subprocess
import sys
from datetime import date
from pathlib import Path

import pytest

from generator.expand import build_plan
from generator.registry import Registry, RegistryError, build_registry, write_registry
from generator.render import Renderer
from generator.spec import load_spec

SPECS = sorted(Path("features").glob("*.yml"))


def _round_trip(spec_path) -> tuple[Registry, Renderer]:
    plan = build_plan(load_spec(spec_path))
    payload = json.loads(write_registry(plan, Path("unused"), generated_at="t"))
    return Registry.from_dict(payload), Renderer(plan)


@pytest.mark.parametrize("spec_path", SPECS, ids=lambda p: p.stem)
def test_committed_registry_reads_back_as_its_spec_describes(spec_path):
    reg = Registry.load(Path("registry") / f"{spec_path.stem}.json")
    spec = load_spec(spec_path)
    assert reg.feature_name == spec.feature_name
    assert reg.spec_version == spec.spec_hash
    assert reg.state_version == spec.state_version
    assert reg.timezone == spec.settings.timezone
    assert reg.source.loaded_at == spec.relation.loaded_at
    assert reg.source.superseded_at == spec.relation.superseded_at
    assert reg.source.timezone == spec.relation.timezone
    assert reg.late_arrival_days == spec.settings.late_arrival_days
    assert reg.entity_spine == spec.settings.entity_spine
    assert reg.entities == spec.entities
    assert reg.widest_window_days == spec.max_window_days


def test_model_names_are_the_ones_rendered(write_spec):
    reg, r = _round_trip(write_spec())
    assert reg.models.staging == r.m_stg
    assert reg.models.events == r.m_events
    assert reg.models.daily_partials == r.m_partials
    assert reg.models.alltime_state == r.m_state
    assert reg.models.mart == r.m_mart


def test_a_spec_without_all_time_has_no_accumulator(write_spec, base_spec):
    reg, _ = _round_trip(write_spec(base_spec.replace('["l7d", "all_time"]', '["l7d"]')))
    assert reg.models.alltime_state is None
    assert reg.models.alltime_recent is None
    assert reg.reset_accumulator_command("2026-09-01") is None


def test_the_reset_remedy_matches_the_one_the_build_prints(write_spec, tmp_path):
    """The guard test and every deployment tool give the same instruction."""
    reg, r = _round_trip(write_spec())
    watermark_test = r.render_test_state_watermark()
    assert reg.reset_accumulator_command("<date>") in watermark_test


def test_refreshable_dates_take_their_width_from_the_spec(write_spec, base_spec):
    body = base_spec + "settings:\n  late_arrival_days: 2\n"
    reg, _ = _round_trip(write_spec(body))
    assert reg.refreshable_dates(date(2026, 9, 3), None) == [date(2026, 9, 2), date(2026, 9, 1)]
    assert reg.refreshable_dates(date(2026, 9, 3), date(2026, 9, 2)) == [date(2026, 9, 2)]


def test_features_carry_the_aggregation_facts(write_spec):
    reg, _ = _round_trip(write_spec())
    by_name = {f.name: f for f in reg.features}
    assert by_name["count_event_id_l7d"].window_days == 7
    assert by_name["count_event_id_all_time"].window_days is None
    assert by_name["count_event_id_l7d"].is_marginal
    assert not by_name["count_event_id_is_ios_l7d"].is_marginal
    assert not by_name["count_event_id_l7d"].nullable


def test_a_registry_from_another_layout_is_refused_by_name():
    payload = build_registry(build_plan(load_spec(SPECS[0])), generated_at="t")
    del payload["models"]
    with pytest.raises(RegistryError, match=payload["feature_name"]):
        Registry.from_dict(payload)


def test_a_missing_registry_says_how_to_make_one(tmp_path):
    with pytest.raises(RegistryError, match="make generate"):
        Registry.load(tmp_path / "nope.json")


def test_reading_a_registry_does_not_import_the_compiler():
    """A deployment can depend on the contract without the compiler's dependencies."""
    probe = (
        "import sys; from generator.registry import Registry; "
        "print(sorted(m for m in sys.modules if m.split('.')[0] in "
        "('yaml', 'pydantic', 'jinja2', 'click') or m in "
        "('generator.spec', 'generator.render', 'generator.expand')))"
    )
    out = subprocess.run([sys.executable, "-c", probe], capture_output=True, text=True, check=True)
    assert out.stdout.strip() == "[]"
