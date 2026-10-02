"""Publish a mart partition to the offline feature store as Parquet on S3.

The dbt mart is the source of truth; this is the copy that training pipelines
and other teams read. Publishing it as partitioned Parquet on object storage
rather than handing out warehouse credentials is what makes the feature store
consumable outside the warehouse -- from Spark, from a notebook, from a serving
job -- without any of them re-deriving the logic.

Layout:
    s3://gold/feature_store/<feature_name>/target_date=<YYYY-MM-DD>/part-0.parquet
    s3://gold/feature_store/<feature_name>/_registry.json

The registry travels with the data on purpose. A consumer that reads a
partition can tell, without asking anyone, which spec version produced it and
what every column means.
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path

import duckdb
from generator.registry import Registry

from tools.object_store import ObjectStore
from tools.paths import DB as DEFAULT_DB
from tools.paths import REGISTRY_DIR


def publish(feature_name: str, target_date: str, db: Path, bucket: str) -> int:
    registry_path = REGISTRY_DIR / f"{feature_name}.json"
    registry = Registry.load(registry_path)
    mart = f"marts.{registry.models.mart}"

    con = ObjectStore.from_env().configure(duckdb.connect(str(db), read_only=True))

    base = f"s3://{bucket}/feature_store/{feature_name}"
    partition = f"{base}/target_date={target_date}/part-0.parquet"

    n_rows = con.execute(
        f"select count(*) from {mart} where target_date = date '{target_date}'"
    ).fetchone()[0]
    if n_rows == 0:
        raise SystemExit(f"{feature_name} has no rows for {target_date}; refusing to publish empty")

    # Last line of defence. The dbt suite already refuses to build a partition
    # whose all_time accumulator is ahead of the as-of date, but publishing is
    # the irreversible step -- once a leaking partition is on object storage,
    # someone will train on it. Re-check here against the partition itself,
    # independently of whether any test happened to run.
    leaking = con.execute(f"""
        select count(*) from {mart}
        where target_date = date '{target_date}'
          and _last_event_date > date '{target_date}'
    """).fetchone()[0]
    if leaking:
        raise SystemExit(
            f"refusing to publish {feature_name} @ {target_date}: {leaking} row(s) reference "
            f"events after the as-of date. The partition was built while the all_time "
            f"accumulator was ahead of this date, so its all_time features contain the "
            f"future. Rebuild it after resetting the accumulator:\n"
            f"  {registry.reset_accumulator_command(target_date)}"
        )

    con.execute(f"""
        copy (
            select * from {mart} where target_date = date '{target_date}'
        ) to '{partition}' (format parquet, compression zstd)
    """)

    # Read back through the S3 API, not from the local warehouse, so a broken
    # endpoint or credential fails here rather than in a consumer's job.
    verify = con.execute(f"""
        select count(*) as n, count(distinct {registry.entities[0]}) as entities,
               max(_spec_version) as spec_version
        from read_parquet('{partition}')
    """).fetchone()

    if verify[0] != n_rows:
        raise SystemExit(f"round trip mismatch: wrote {n_rows}, read back {verify[0]}")
    if verify[2] != registry.spec_version:
        raise SystemExit(
            f"published data carries spec {verify[2]} but the registry says "
            f"{registry.spec_version}; regenerate and rebuild before publishing"
        )

    con.execute(f"""
        copy (select * from read_json_auto('{registry_path.as_posix()}'))
        to '{base}/_registry.json' (format json, array false)
    """)
    con.close()

    print(f"published {feature_name} @ {target_date}")
    print(f"  rows          {verify[0]}")
    print(f"  entities      {verify[1]}")
    print(f"  features      {len(registry.features)}")
    print(f"  spec version  {verify[2]}")
    print(f"  location      {partition}")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("feature_name")
    ap.add_argument("target_date")
    ap.add_argument("--db", type=Path, default=DEFAULT_DB)
    ap.add_argument("--bucket", default=os.getenv("FS_GOLD_BUCKET", "gold"))
    a = ap.parse_args()
    return publish(a.feature_name, a.target_date, a.db, a.bucket)


if __name__ == "__main__":
    raise SystemExit(main())
