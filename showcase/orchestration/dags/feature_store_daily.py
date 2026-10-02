"""Daily feature-store DAG, derived from the generated registry.

The DAG is not hand-maintained. It reads registry/*.json -- the same artefact
the compiler commits in the repository root -- and builds one task group per
feature spec. Adding a spec and running `make generate` adds orchestration,
and a DAG that disagrees with the models it runs is not representable.

AS-OF DATE. Every task passes `data_interval_start` as target_date, so the run
scheduled just after midnight on D+1 computes features as of day D, which is
the last day whose events are complete. Airflow's logical date is the single
source of truth: a manual run, a backfill and a scheduled run all take the same
path and produce the same numbers.

BACKFILL. The accumulator cannot be reconstructed for a date behind its
watermark, so history is loaded with an explicit backfill
(`make -C showcase dbt-backfill`, which full-refreshes the partial layer from a
start date) rather than by scheduler catchup. See FS_CATCHUP below.

REVISION WINDOW. A partition is not final the day it is built. The partial layer
keeps absorbing what changes after it -- late events, corrections, deletions --
so each run republishes the preceding `late_arrival_days` partitions as well as
its own. Every model is idempotent for a given as-of date, so this is a refresh,
not a rewrite of history. A partition is provisional until T + late_arrival_days
and final afterwards; a later change still reaches every partition built after it.

SCHEDULE. The run for day D starts at 02:00 UTC on D+1. That is after D has ended
on every business clock east of UTC-2; a spec on a clock further west is served
before its day is over. That is still correct -- a run's knowledge then ends when
it ran, and the next run picks up the rest of the day -- but the partition is
less complete when first published, so schedule such a deployment later.

ORDERING. Runs are serialised with max_active_runs=1, and deliberately NOT with
depends_on_past. The accumulator is self-healing -- it consumes
(watermark, seal] rather than assuming yesterday -- so ordering is an
efficiency concern, not a correctness one.

depends_on_past would add nothing and cost a deadlock class: one incomplete day
blocks every later day, including a manual rerun of the day that would fix it,
and a first run has no predecessor to satisfy it at all. Serialisation gives the
ordering; the watermark gives the correctness.
"""

from __future__ import annotations

import os
from datetime import timedelta
from pathlib import Path

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from generator.registry import Registry

# The generated dbt project and the registry are the compiler's output and live
# in the repository root; the profile, the warehouse and the operational tools
# are the showcase's and live beside this DAG. Keeping the two roots separate
# here is what lets the same DAG point at a production deployment of the same
# generated project by changing one environment variable.
PROJECT_ROOT = Path(os.getenv("FS_PROJECT_ROOT", "/opt/featurefold"))
SHOWCASE_ROOT = Path(os.getenv("FS_SHOWCASE_ROOT", str(PROJECT_ROOT / "showcase")))
DBT_DIR = PROJECT_ROOT / "transform"
REGISTRY_DIR = PROJECT_ROOT / "registry"
DBT = os.getenv("FS_DBT_BIN", "dbt")

# target_date is the day the interval covers, not the day the run happens.
TARGET_DATE = "{{ data_interval_start | ds }}"

DBT_ENV = {
    # The project is read-only generated output, so its run artefacts are
    # directed into the showcase rather than written back beside the models.
    "DBT_PROJECT_DIR": str(DBT_DIR),
    "DBT_PROFILES_DIR": str(SHOWCASE_ROOT),
    "DBT_TARGET_PATH": str(SHOWCASE_ROOT / "target"),
    "DBT_LOG_PATH": str(SHOWCASE_ROOT / "logs"),
    "DBT_TARGET": os.getenv("DBT_TARGET", "seaweed"),
    "DBT_DUCKDB_PATH": os.getenv("DBT_DUCKDB_PATH", str(SHOWCASE_ROOT / "warehouse.duckdb")),
    "FS_BRONZE_SCHEMA": os.getenv("FS_BRONZE_SCHEMA", "bronze_events"),
    "PATH": os.getenv("PATH", "/usr/local/bin:/usr/bin:/bin"),
    # Object-store credentials come from the `fs_object_store` Connection,
    # rendered when each task runs. Nothing here names a credential or falls
    # back to one: a missing Connection fails the task, and Airflow masks the
    # password wherever the rendered environment is logged. The local stack
    # defines the Connection from infra/local.env; a deployment backs it with
    # its secrets manager.
    "S3_ENDPOINT": "{{ conn.fs_object_store.host }}:{{ conn.fs_object_store.port }}",
    "S3_ACCESS_KEY": "{{ conn.fs_object_store.login }}",
    "S3_SECRET_KEY": "{{ conn.fs_object_store.password }}",
}


def dbt_task(task_id: str, command: str, select: str, **kwargs) -> BashOperator:
    return BashOperator(
        task_id=task_id,
        bash_command=(
            f"cd {SHOWCASE_ROOT} && "
            f"{DBT} {command} "
            f"--select '{select}' "
            f'--vars \'{{"target_date": "{TARGET_DATE}"}}\' '
            f"--target $DBT_TARGET"
        ),
        env=DBT_ENV,
        append_env=True,
        **kwargs,
    )


with DAG(
    dag_id="feature_store_daily",
    description="Daily batch feature marts, one task group per generated feature spec",
    schedule="0 2 * * *",  # 02:00 UTC, after the upstream journal settles
    start_date=__import__("pendulum").datetime(2026, 8, 20, tz="UTC"),
    # Catchup is OFF by default, deliberately. The all_time accumulator is a
    # single forward-only fold, so letting the scheduler improvise its way
    # through history means asking it to build as-of dates that sit BEHIND the
    # watermark -- which the guard test correctly refuses, leaving a wall of red
    # tasks that no retry can clear. An initial load is a deliberate operation:
    #     make -C showcase dbt-backfill TARGET_DATE=<first day> BACKFILL_FROM=<start>
    # then unpause from there. Set FS_CATCHUP=1 once the accumulator has been
    # reset for the window being replayed.
    catchup=os.getenv("FS_CATCHUP", "0") == "1",
    max_active_runs=1,  # the all_time accumulator is a serial fold
    # DuckDB in the local stack is a single-writer file, so feature groups must
    # not build concurrently there. A real warehouse has no such limit: raise
    # FS_MAX_ACTIVE_TASKS to fan the groups out.
    max_active_tasks=int(os.getenv("FS_MAX_ACTIVE_TASKS", "1")),
    default_args={
        "owner": "data-platform",
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
        # Safe because every model is idempotent for a given target_date: the
        # partial layer replaces its date range, the accumulator consumes an
        # empty range on a repeat, and the mart replaces its partition.
        "retry_exponential_backoff": True,
    },
    tags=["feature-store", "dbt", "batch"],
    doc_md=__doc__,
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    # A stale source silently produces a day of zeros that looks like genuine
    # customer inactivity. Fail before building rather than after publishing.
    freshness = BashOperator(
        task_id="check_source_freshness",
        bash_command=(
            f"cd {SHOWCASE_ROOT} && {DBT} source freshness "
            f'--vars \'{{"target_date": "{TARGET_DATE}"}}\' --target $DBT_TARGET'
        ),
        env=DBT_ENV,
        append_env=True,
    )

    # Feature specs to orchestrate, read from committed generator output.
    registries = Registry.load_dir(REGISTRY_DIR)
    if not registries:
        start >> freshness >> end

    for registry in registries:
        name = registry.feature_name
        models = registry.models

        with TaskGroup(group_id=name, tooltip=registry.description) as group:
            # Reusable per-day state, and everything it reads: the shared
            # staging model, whose tests check the source's knowledge-time
            # columns before anything is folded from them, and the spec's events
            # model. Rewrites exactly the days that changed since the last run
            # -- new, late, corrected or deleted -- however far back they lie.
            partials = dbt_task(
                "build_daily_partials",
                "build",
                f"+{models.daily_partials}",
            )

            # The serial fold. Correct in any order thanks to the watermark;
            # running in order simply keeps each run's cost to a single day.
            accumulator = (
                dbt_task("fold_alltime_state", "build", models.alltime_state)
                if models.alltime_state
                else None
            )

            # Windows, unsealed tail, and the published wide table. `dbt build`
            # interleaves the generated invariant tests, so a violated
            # invariant stops the run before the partition is exposed.
            publish = dbt_task(
                "build_and_test_mart",
                "build",
                f"tag:{name}",
            )

            # Refresh the days whose inputs are still settling. Cheap, because
            # every model is a no-op for a date it has already absorbed.
            late = registry.late_arrival_days

            # Which past partitions are still refreshable is not a fixed offset:
            # it is bounded below by the accumulator's watermark, because a date
            # the accumulator has already sealed past is FINAL and rebuilding it
            # would fold future events into its all_time features. The rule
            # lives in generator.revision; showcase/tools/revision_window.py
            # resolves it against this deployment's registry and warehouse.
            revise = None
            if late > 0:
                revise = BashOperator(
                    task_id="refresh_revision_window",
                    bash_command=(
                        f"set -euo pipefail; cd {SHOWCASE_ROOT} && "
                        f"DATES=$(python -m tools.revision_window {name} {TARGET_DATE}) && "
                        f'if [ -z "$DATES" ]; then '
                        f'echo "nothing refreshable: all earlier partitions are final"; '
                        f"else for d in $DATES; do "
                        f'echo "refreshing $d" && '
                        f"{DBT} build --select 'tag:{name}' "
                        f'--vars "{{target_date: $d}}" --target $DBT_TARGET; '
                        f"done; fi"
                    ),
                    env=DBT_ENV,
                    append_env=True,
                )

            # Publish exactly the dates this run built or refreshed -- the same
            # resolver, so the two can never drift apart. Publishing a partition
            # this run did not refresh would trip the publisher's leakage gate,
            # correctly.
            offline = BashOperator(
                task_id="publish_offline_store",
                bash_command=(
                    f"set -euo pipefail; cd {SHOWCASE_ROOT} && "
                    f"DATES=$(python -m tools.revision_window {name} {TARGET_DATE} "
                    f"--include-target) && "
                    f"for d in $DATES; do "
                    f"python -m tools.publish_offline_store {name} $d; done"
                ),
                env=DBT_ENV,
                append_env=True,
            )

            chain = [partials]
            if accumulator is not None:
                chain.append(accumulator)
            chain.append(publish)
            if revise is not None:
                chain.append(revise)
            chain.append(offline)
            for upstream, downstream in zip(chain, chain[1:], strict=False):
                upstream >> downstream

        start >> freshness >> group >> end
