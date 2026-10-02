"""Audit the published offline store, independently of how it was produced.

The publisher refuses to write a leaking partition, but that only guards writes
from this version of the code. A store accumulates: partitions published by an
earlier build, by a different branch, or by a run whose guards had a bug are
still sitting there, and they are what a training pipeline will read.

So this audits the store itself, as a consumer sees it, and reports every
partition that violates its own contract. `--fix` republishes the offending
partitions from the warehouse, which is safe because the publisher re-checks
each one; `--prune` deletes those the warehouse can no longer reproduce.

A mixed store is the failure worth naming. Change a spec's entity_spine and the
partitions written before the change still sit there under the old semantics, so
a consumer reading a date range silently gets two different definitions of what
a row means. That is why the spec fingerprint travels with every row and why a
version mismatch is treated as a contract violation rather than as metadata.
"""

from __future__ import annotations

import argparse
import os
import subprocess
from pathlib import Path

import duckdb
from generator.registry import Registry

from tools.object_store import ObjectStore
from tools.paths import DB as DEFAULT_DB
from tools.paths import PYTHON, REGISTRY_DIR, SHOWCASE


def delete_prefix(bucket: str, prefix: str) -> int:
    """Delete every object under a prefix. Used only for partitions the warehouse
    can no longer reproduce, which are therefore unrecoverable test residue or
    the remains of a deleted spec."""
    s3 = ObjectStore.from_env().boto3_client()
    listed = s3.list_objects_v2(Bucket=bucket, Prefix=prefix).get("Contents", [])
    for obj in listed:
        s3.delete_object(Bucket=bucket, Key=obj["Key"])
    return len(listed)


def audit(feature_name: str, bucket: str, db: Path, fix: bool, prune: bool) -> int:
    registry = Registry.load(REGISTRY_DIR / f"{feature_name}.json")
    expected_spec = registry.spec_version
    base = f"s3://{bucket}/feature_store/{feature_name}"

    con = ObjectStore.from_env().configure(duckdb.connect())
    con.execute(
        f"create or replace view store as "
        f"select * from read_parquet('{base}/*/*.parquet', hive_partitioning=true)"
    )

    # Contract 1: no partition may reference an event after its own as-of date.
    leaking = con.execute("""
        select target_date, count(*) as bad_rows, max(_last_event_date) as newest_event
        from store where _last_event_date > target_date
        group by 1 order by 1
    """).fetchall()

    # Contract 2: every partition should carry the spec version the registry
    # describes, or a consumer resolving column meanings will be misled.
    stale_spec = con.execute(f"""
        select target_date, max(_spec_version) as spec_version
        from store where _spec_version <> '{expected_spec}'
        group by 1 order by 1
    """).fetchall()

    total, parts = con.execute("select count(*), count(distinct target_date) from store").fetchone()
    con.close()

    print(f"{feature_name}")
    print(f"  partitions        {parts}")
    print(f"  rows              {total}")
    print(f"  registry spec     {expected_spec}")
    print(f"  leaking           {len(leaking)} partition(s)")
    for d, n, newest in leaking:
        print(f"      {d}: {n} row(s), newest event {newest}")
    print(f"  stale spec        {len(stale_spec)} partition(s)")
    for d, v in stale_spec:
        print(f"      {d}: {v}")

    suspect = sorted({d for d, *_ in leaking} | {d for d, *_ in stale_spec})
    if not suspect:
        print("  STORE CLEAN")
        return 0

    if not (fix or prune):
        print(f"\n  {len(suspect)} partition(s) violate their contract.")
        print("  Re-publish them with --fix, or remove them with --prune.")
        return 1

    wh = duckdb.connect(str(db), read_only=True)
    available = {
        r[0]
        for r in wh.execute(
            f"select distinct target_date from marts.{registry.models.mart}"
        ).fetchall()
    }
    wh.close()

    failures = 0
    for d in suspect:
        if d in available and fix:
            print(f"  republishing {d} ...")
            res = subprocess.run(
                [
                    str(PYTHON),
                    "-m",
                    "tools.publish_offline_store",
                    feature_name,
                    d.isoformat(),
                ],
                cwd=SHOWCASE,
                capture_output=True,
                text=True,
            )
            if res.returncode != 0:
                print(f"    REFUSED: {res.stdout.strip().splitlines()[-1] if res.stdout else ''}")
                failures += 1
        elif prune:
            prefix = f"feature_store/{feature_name}/target_date={d}/"
            n = delete_prefix(bucket, prefix)
            print(f"  pruned {d} ({n} object(s) deleted)")
        else:
            print(f"  {d}: not in the warehouse; use --prune to drop it")
            failures += 1

    return 1 if failures else 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("feature_name", nargs="?")
    ap.add_argument("--bucket", default=os.getenv("FS_GOLD_BUCKET", "gold"))
    ap.add_argument("--db", type=Path, default=DEFAULT_DB)
    ap.add_argument("--fix", action="store_true", help="republish violating partitions")
    ap.add_argument("--prune", action="store_true", help="report partitions to remove")
    a = ap.parse_args()

    names = (
        [a.feature_name]
        if a.feature_name
        else [r.feature_name for r in Registry.load_dir(REGISTRY_DIR)]
    )
    rc = 0
    for i, name in enumerate(names):
        if i:
            print()
        rc |= audit(name, a.bucket, a.db, a.fix, a.prune)
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
