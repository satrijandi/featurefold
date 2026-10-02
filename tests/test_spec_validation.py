"""Every guard the spec parser enforces.

These matter more than they look. A feature store that compiles a subtly wrong
spec produces plausible numbers that a data scientist will trust, so each of
these is a bug that would otherwise ship silently into a model.
"""

import pytest

from generator.spec import SpecError, load_spec


def test_valid_spec_loads(write_spec):
    spec = load_spec(write_spec())
    assert spec.feature_name == "test_feature"
    assert spec.max_window_days == 7
    assert spec.has_all_time


def test_unknown_atomic_field_is_rejected(write_spec, base_spec):
    bad = base_spec.replace(
        "  - event_id:\n      apply_cond_cat", "  - nope_id:\n      apply_cond_cat"
    )
    with pytest.raises(SpecError, match="not produced by the source SELECT list"):
        load_spec(write_spec(bad))


def test_unknown_condition_category_is_rejected(write_spec, base_spec):
    with pytest.raises(SpecError, match="unknown category"):
        load_spec(write_spec(base_spec.replace("[login_os]", "[login_typo]")))


def test_unmapped_physical_relation_is_rejected(write_spec, base_spec):
    """A hard-coded relation resolves in one environment only, so it can never be tested."""
    bad = base_spec.replace(
        "relations:\n  bronze.db.events:\n    source_name: bronze\n    table_name: events\n"
        "    loaded_at: _loaded_at\n",
        "relations: {}\n",
    )
    with pytest.raises(SpecError, match="Hard-coded relations"):
        load_spec(write_spec(bad))


def test_target_date_dependent_column_without_a_derivation_is_rejected(write_spec, base_spec):
    bad = base_spec.replace(
        "    os_name,",
        "    SOME_FN(event_timestamp, '{{ target_date }}') AS weird_col,\n    os_name,",
    ).replace(
        "  - event_id:\n      apply_cond_cat: [login_os]\n      agg: ['count']",
        "  - weird_col:\n      apply_cond_cat: [login_os]\n      agg: ['min']\n      field_type: numeric",
    )
    with pytest.raises(SpecError, match="cannot be stored in the reusable daily partial layer"):
        load_spec(write_spec(bad))


def test_days_since_is_auto_detected_without_spec_changes(write_spec, base_spec):
    """The user's original YAML must work as written; the derivation is inferred."""
    src = base_spec.replace(
        "    os_name,",
        "    DATEDIFF(DAY, event_timestamp, '{{ target_date }}'::DATE) AS days_since_login,\n    os_name,",
    ).replace(
        "  - event_id:\n      apply_cond_cat: [login_os]\n      agg: ['count']",
        "  - days_since_login:\n      apply_cond_cat: [login_os]\n      agg: ['min','max']\n      field_type: numeric",
    )
    spec = load_spec(write_spec(src))
    field = next(f for f in spec.fields if f.name == "days_since_login")
    assert field.derived is not None
    assert field.derived.kind == "days_since"
    assert field.derived.from_field == "event_timestamp"
    assert field.derived.auto_detected


def test_days_since_rejects_aggregations_it_cannot_reconstruct(write_spec, base_spec):
    src = base_spec.replace(
        "    os_name,",
        "    DATEDIFF(DAY, event_timestamp, '{{ target_date }}'::DATE) AS days_since_login,\n    os_name,",
    ).replace(
        "  - event_id:\n      apply_cond_cat: [login_os]\n      agg: ['count']",
        "  - days_since_login:\n      apply_cond_cat: [login_os]\n      agg: ['sum']\n      field_type: numeric",
    )
    with pytest.raises(SpecError, match="only supports 'min' and 'max'"):
        load_spec(write_spec(src))


def test_time_varying_condition_predicate_is_rejected(write_spec, base_spec):
    """A moving condition would invalidate every stored partial the next day."""
    bad = base_spec.replace(
        "- is_ios: \"UPPER(os_name) = 'IOS'\"",
        "- is_recent: \"event_timestamp > '{{ target_date }}'::DATE - 1\"",
    )
    with pytest.raises(SpecError, match="must be time-invariant"):
        load_spec(write_spec(bad))


def test_min_max_without_a_declared_type_is_rejected(write_spec, base_spec):
    bad = base_spec.replace("agg: ['count']", "agg: ['min']")
    with pytest.raises(SpecError, match="need a declared type"):
        load_spec(write_spec(bad))


def test_sum_on_a_timestamp_is_rejected(write_spec, base_spec):
    bad = base_spec.replace(
        "  - event_id:\n      apply_cond_cat: [login_os]\n      agg: ['count']",
        "  - event_timestamp:\n      apply_cond_cat: [login_os]\n      agg: ['sum']\n      field_type: timestamp",
    )
    with pytest.raises(SpecError, match="need a numeric field"):
        load_spec(write_spec(bad))


def test_duplicate_member_names_across_applied_categories_are_rejected(write_spec, base_spec):
    """Two categories sharing a member name would collide in the generated column names."""
    bad = base_spec.replace(
        'time_cat: ["l7d", "all_time"]',
        """  - other_cat:
    - default: "TRUE"
    - is_ios: "1=1"

time_cat: ["l7d", "all_time"]""",
    ).replace("apply_cond_cat: [login_os]", "apply_cond_cat: [login_os, other_cat]")
    with pytest.raises(SpecError, match="Generated feature names would collide"):
        load_spec(write_spec(bad))


def test_unsupported_window_is_rejected(write_spec, base_spec):
    with pytest.raises(SpecError, match="is not supported"):
        load_spec(write_spec(base_spec.replace('"l7d"', '"last_week"')))


def test_star_projection_is_rejected(write_spec, base_spec):
    with pytest.raises(SpecError, match=r"projects `\*`"):
        load_spec(write_spec(base_spec.replace("    customer_id AS safe_id,", "    *,")))


def test_unaliased_expression_is_rejected(write_spec, base_spec):
    with pytest.raises(SpecError, match="no alias"):
        load_spec(write_spec(base_spec.replace("    os_name,", "    upper(os_name),")))


def test_non_daily_cadence_is_rejected(write_spec, base_spec):
    with pytest.raises(SpecError, match="not implemented"):
        load_spec(write_spec(base_spec.replace('feature_type: "daily"', 'feature_type: "hourly"')))


@pytest.mark.parametrize(
    ("addition", "names"),
    [
        # A misspelt setting would otherwise fall back to its default without a word.
        ("settings:\n  late_arival_days: 1\n", "settings.late_arival_days"),
        ("time_catt: ['l30d']\n", "time_catt"),
        ("settings:\n  target_dialects: [duckdb]\n", "settings.target_dialects"),
    ],
)
def test_unknown_keys_are_rejected(write_spec, base_spec, addition, names):
    with pytest.raises(SpecError, match=rf"{names}: unknown key"):
        load_spec(write_spec(base_spec + addition))


def test_unknown_atomic_field_key_is_rejected(write_spec, base_spec):
    bad = base_spec.replace(
        "      agg: ['count']", "      agg: ['count']\n      distinct_methd: approx"
    )
    with pytest.raises(SpecError, match=r"atomic_field\.event_id\.distinct_methd: unknown key"):
        load_spec(write_spec(bad))


def test_unknown_relation_key_is_rejected(write_spec, base_spec):
    bad = base_spec.replace("    table_name: events", "    table_name: events\n    tabel: x")
    with pytest.raises(SpecError, match=r"relations\.bronze\.db\.events\.tabel: unknown key"):
        load_spec(write_spec(bad))


@pytest.mark.parametrize(
    ("setting", "names"),
    [
        # A quoted number is a type error, not something to coerce and hope.
        ("late_arrival_days: '3'", "settings.late_arrival_days"),
        ("late_arrival_days: -1", "settings.late_arrival_days"),
        ("kmv_k: 8", "settings.kmv_k"),
        ("materialized_mart: banana", "settings.materialized_mart"),
        ("entity_spine: sometimes", "settings.entity_spine"),
        ("window_convention: centred", "settings.window_convention"),
        ("timezone: Mars/Olympus_Mons", "settings.timezone"),
        # The separator is spliced into every generated column name.
        ("separator: '-'", "settings.separator"),
    ],
)
def test_invalid_settings_are_rejected(write_spec, base_spec, setting, names):
    with pytest.raises(SpecError, match=rf"{names}: "):
        load_spec(write_spec(base_spec + f"settings:\n  {setting}\n"))


def test_empty_settings_block_takes_the_defaults(write_spec, base_spec):
    spec = load_spec(write_spec(base_spec + "settings:\n"))
    assert spec.settings.late_arrival_days == 3


def test_relation_must_appear_as_a_whole_name(write_spec, base_spec):
    """`bronze.db.event` is a prefix of `bronze.db.events`, not a mention of it."""
    bad = base_spec.replace("relations:\n  bronze.db.events:", "relations:\n  bronze.db.event:")
    with pytest.raises(SpecError, match="does not appear in the source SQL"):
        load_spec(write_spec(bad))


def test_spec_hash_tracks_content(write_spec, base_spec):
    a = load_spec(write_spec(base_spec)).spec_hash
    b = load_spec(
        write_spec(base_spec.replace('created_by: "test"', 'created_by: "other"'))
    ).spec_hash
    assert a != b


def test_a_hand_written_as_of_filter_is_rejected(write_spec, base_spec):
    """The generator owns the as-of read; a second definition would compete with it."""
    bad = base_spec.replace(
        "  WHERE customer_id IS NOT NULL",
        "  WHERE customer_id IS NOT NULL AND _loaded_at <= '{{ target_date }}'::DATE",
    )
    with pytest.raises(SpecError, match="writes the as-of filter itself"):
        load_spec(write_spec(bad))


def test_a_join_is_rejected_as_not_point_in_time(write_spec, base_spec):
    bad = (
        base_spec.replace(
            "  FROM bronze.db.events\n",
            "  FROM bronze.db.events e JOIN bronze.db.devices d ON e.device_id = d.id\n",
        )
        + "  bronze.db.devices: {source_name: bronze, table_name: devices, loaded_at: _l}\n"
    )
    with pytest.raises(SpecError, match="not point-in-time"):
        load_spec(write_spec(bad))


def test_the_word_join_inside_a_literal_is_not_a_join(write_spec, base_spec):
    ok = base_spec.replace("- is_ios: \"UPPER(os_name) = 'IOS'\"", "- is_ios: \"os_name = 'join'\"")
    load_spec(write_spec(ok))


def test_reading_a_second_relation_is_rejected(write_spec, base_spec):
    bad = (
        base_spec.replace(
            "  WHERE customer_id IS NOT NULL",
            "  WHERE customer_id NOT IN (SELECT id FROM bronze.db.blocklist)",
        )
        + "  bronze.db.blocklist: {source_name: bronze, table_name: blocklist, loaded_at: _l}\n"
    )
    with pytest.raises(SpecError, match="exactly one mapped relation"):
        load_spec(write_spec(bad))


def test_a_relation_must_say_when_its_rows_became_visible(write_spec, base_spec):
    bad = base_spec.replace("    loaded_at: _loaded_at\n", "")
    with pytest.raises(SpecError, match=r"relations\.bronze\.db\.events\.loaded_at: required"):
        load_spec(write_spec(bad))


def test_a_relation_time_zone_must_exist(write_spec, base_spec):
    bad = base_spec.replace(
        "    loaded_at: _loaded_at\n", "    loaded_at: _l\n    timezone: UTC+7\n"
    )
    with pytest.raises(SpecError, match="not an IANA time zone"):
        load_spec(write_spec(bad))


def test_reserved_column_names_are_rejected(write_spec, base_spec):
    bad = base_spec.replace("    os_name,", "    os_name AS _fs_loaded_at,")
    with pytest.raises(SpecError, match="reserve for themselves"):
        load_spec(write_spec(bad))


def test_entity_keys_can_be_typed(write_spec, base_spec):
    spec = load_spec(
        write_spec(base_spec.replace('entities: ["safe_id"]', "entities: [{safe_id: bigint}]"))
    )
    assert spec.entities == ("safe_id",)
    assert spec.entity_types == {"safe_id": "bigint"}


def test_an_untyped_entity_key_is_a_string(write_spec):
    assert load_spec(write_spec()).entity_types == {"safe_id": "varchar"}


def test_an_unknown_entity_type_is_rejected(write_spec, base_spec):
    bad = base_spec.replace('entities: ["safe_id"]', "entities: [{safe_id: uuidish}]")
    with pytest.raises(SpecError, match="not recognised"):
        load_spec(write_spec(bad))


def test_an_exposure_needs_an_owner(write_spec, base_spec):
    bad = base_spec + "exposures:\n  - {name: churn, type: ml, owner: {}}\n"
    with pytest.raises(SpecError, match="owner needs a name or an email"):
        load_spec(write_spec(bad))


def test_state_version_ignores_what_cannot_change_stored_state(write_spec, base_spec):
    a = load_spec(write_spec(base_spec))
    b = load_spec(
        write_spec(
            base_spec.replace('created_by: "test"', 'created_by: "other"').replace(
                '["l7d", "all_time"]', '["l7d", "l30d", "all_time"]'
            )
            + "settings:\n  late_arrival_days: 5\n  entity_spine: active_window\n"
        )
    )
    assert a.spec_hash != b.spec_hash
    assert a.state_version == b.state_version


@pytest.mark.parametrize(
    ("old", "new"),
    [
        ("\"UPPER(os_name) = 'IOS'\"", "\"UPPER(os_name) = 'IPADOS'\""),
        ("  WHERE customer_id IS NOT NULL", "  WHERE customer_id <> ''"),
        ("    loaded_at: _loaded_at\n", "    loaded_at: _ingested_at\n"),
        ("time_cat:", "settings:\n  timezone: Asia/Jakarta\ntime_cat:"),
    ],
)
def test_state_version_tracks_what_stored_state_means(write_spec, base_spec, old, new):
    assert old in base_spec
    a = load_spec(write_spec(base_spec)).state_version
    assert load_spec(write_spec(base_spec.replace(old, new))).state_version != a
