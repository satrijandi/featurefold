from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

BASE = """
feature_name: "test_feature"
feature_type: "daily"
created_by: "test"
source: |
  SELECT
    customer_id AS safe_id,
    device_id,
    event_id,
    event_timestamp,
    os_name,
    event_status
  FROM bronze.db.events
  WHERE customer_id IS NOT NULL
entities: ["safe_id"]
timestamp_col: "event_timestamp"
condition_cat:
  - login_os:
    - default: "TRUE"
    - is_ios: "UPPER(os_name) = 'IOS'"
    - is_android: "UPPER(os_name) = 'ANDROID'"
time_cat: ["l7d", "all_time"]
atomic_field:
  - event_id:
      apply_cond_cat: [login_os]
      agg: ['count']
relations:
  bronze.db.events:
    source_name: bronze
    table_name: events
    loaded_at: _loaded_at
"""


@pytest.fixture
def write_spec(tmp_path: Path):
    def _write(body: str = BASE) -> Path:
        p = tmp_path / "spec.yml"
        p.write_text(textwrap.dedent(body))
        return p

    return _write


@pytest.fixture
def base_spec() -> str:
    return BASE
