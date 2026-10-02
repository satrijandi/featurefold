"""The tools take object-store credentials from the environment, and only from it."""

import pytest

from tools.object_store import REQUIRED, ObjectStore


def test_reads_every_value_from_the_environment(monkeypatch):
    monkeypatch.setenv("S3_ENDPOINT", "store:9000")
    monkeypatch.setenv("S3_ACCESS_KEY", "key")
    monkeypatch.setenv("S3_SECRET_KEY", "secret")
    assert ObjectStore.from_env() == ObjectStore("store:9000", "key", "secret")


@pytest.mark.parametrize("missing", REQUIRED)
def test_a_missing_value_stops_rather_than_falling_back(monkeypatch, missing):
    for name in REQUIRED:
        monkeypatch.setenv(name, "set")
    monkeypatch.delenv(missing)
    with pytest.raises(SystemExit, match=f"{missing} unset"):
        ObjectStore.from_env()
