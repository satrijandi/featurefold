# feature-mart

A config-driven feature store compiler.
One YAML spec compiles into a complete dbt project: staging, a reusable partial-aggregate layer, window roll-ups, an incremental `all_time` accumulator, a wide published mart, column-level documentation, a machine-readable feature registry, and over a thousand generated invariant tests.

The spec in `features/fact_agg_features_login_history_v2.yml` expands to **480 features** backed by **96 stored columns per entity-day**.

```
features/*.yml          <-- the only file a human edits
      |  make generate
      v
transform/models/       generated dbt models      (committed, drift-checked)
transform/tests/        generated invariants      (committed, drift-checked)
registry/*.json         machine-readable contract (committed, drift-checked)
```

## Two halves, and why they are separate

Compiling a spec into a dbt project needs no warehouse, no dbt-core, no object store and no scheduler.
Proving that the compiled project produces the right numbers needs all four.
Those are different jobs with different dependencies, so they are different things in this repository.

| | what it is | what it depends on |
|---|---|---|
| **this directory** | the compiler, its generated output, and the dbt runtime library that output calls into | `pyyaml`, `jinja2`, `pydantic`, `click` |
| **[`showcase/`](showcase/)** | an end-to-end proof: DuckDB, a synthetic fixture, SeaweedFS, Airflow, JupyterLab | all of the above plus dbt-core, dbt-duckdb, boto3, Jupyter |

CI asserts the boundary rather than trusting it: the generator job fails if installing this package pulls in dbt, and the showcase job fails if running the pipeline writes anything back into `transform/`.

The practical consequence is that the compiler can be embedded anywhere - a platform repo, another team's dbt project, a service - without dragging a stack behind it, and the showcase can be replaced wholesale by a production deployment without touching a line of the compiler.
`showcase/profiles.yml`, `showcase/orchestration/` and `showcase/infra/` are exactly the files such a deployment would supply its own versions of.

## Why it is built this way

A feature store fails in a specific, expensive way: it produces numbers that look right.
A window off by one day, an `all_time` counter that double-counts on a retry, a feature that quietly sees past its own as-of date - none of these throw an error.
They ship into a model, score well offline, and collapse in production.

Every significant decision here is aimed at making those failures impossible to express, or loud when they happen.

## How the combinatorics work

Each atomic field declares which condition categories apply to it.
Those categories are crossed with each other, then with the aggregations, then with the time windows.

| atomic field | condition combos | x windows | x aggs | features |
|---|--:|--:|--:|--:|
| `event_id` | 3 x 4 x 5 = 60 | 4 | 1 | 240 |
| `device_id` | 3 x 4 = 12 | 4 | 1 | 48 |
| `event_timestamp` | 12 | 4 | 2 | 96 |
| `days_since_login` | 12 | 4 | 2 | 96 |
| | | | **total** | **480** |

The `default: "TRUE"` member in every category is what makes this work.
It puts the marginal (uncut) features inside the same cross-product instead of needing a special case, so `count_event_id_l7d` and `count_event_id_is_login_success_is_ios_is_late_night_l7d` come from one code path.

Run `make explain` to see the expansion for any spec.

## The core idea: one fold, many windows

Every aggregation is defined as a commutative monoid over a partial state:

```
partial   : source rows   -> state    one state per entity, per event_date
state_agg : many states   -> state    roll a range of days up
merge     : state, state  -> state    fold sealed history into a fresh tail
finalize  : state         -> value    what the mart publishes
```

A bounded window is exactly `finalize(state_agg(days in window))`.
`all_time` is exactly `finalize(merge(sealed_state, state_agg(unsealed tail)))`.

There is no second code path, so `l30d` and `all_time` **cannot** drift apart - they are the same fold over different ranges.
Adding an aggregation means implementing four small methods in `generator/aggregates.py`; it does not mean touching the templates, the mart, or the orchestration.

This is also what makes the cost sane.
Reading a day of source once produces state that every window reuses, so adding `l60d` and `l90d` to the spec costs no extra source reads.

## The decisions that shaped it

### Reusable daily partials, not per-window recomputation

`int_<name>__daily_partials` holds composable state at entity x event_date, and every window folds over it.
The alternative - rescanning raw source per window - re-reads the same days once per window and makes `all_time` cost grow without bound.

### `all_time` is a sealed accumulator with an unsealed tail

The accumulator folds in only event_dates at or before `target_date - late_arrival_days`.
More recent days may still receive late-arriving events and get rewritten, so the mart completes `all_time` by merging the sealed state with the unsealed tail **using the same merge function**.
The published number is never stale, and late events still land on the day they happened.

The consumed range is `(stored watermark, seal date]`, not "yesterday".
Two properties follow, and both are tested:

- **Retries are idempotent.** A repeated run consumes an empty range and changes nothing.
  Without this, a retried task silently double-counts every `all_time` feature.
- **Gaps self-heal.** A run following a missed day absorbs the gap on its own, with no operator involved.

### Runs are not guaranteed to arrive in order

This turned out to be the sharpest edge in the whole design, and three separate
invariants exist because of it.
Each was found by an end-to-end scenario, not by reasoning, and each is now a test.

Every run sees the source *as of its own* `target_date`.
That makes a forward-only sequence trivially correct and every other sequence subtly wrong.

**1. Stored partials may only ever gain information.**
Replaying a past date would recompute an already-complete day under a narrower view and silently drop events a later run had captured.
The partial layer stamps `_computed_for` and refuses to recompute any `event_date` already written under a later as-of date.

**2. The rewrite window is anchored to the last run that actually happened.**
A window measured from today steps straight over days that were still inside their late-arrival window when the last run occurred.
Skipping one day would lose those events permanently, with nothing to signal it.
Taking the earlier of `last_run` and `target_date` makes the partial layer self-heal across a gap, exactly as the accumulator does.

**3. The unsealed tail starts at the accumulator's real watermark.**
`target_date - late_arrival_days` and the watermark coincide on an ordinary forward run and diverge on a replay, where the watermark sits *ahead*.
A target-derived tail would then re-add days the accumulator already holds and double-count every `all_time` feature.
Anchoring to the watermark makes sealed and unsealed complementary by construction, in any order.

**4. As-of dates must be served in order, and the build refuses when they are not.**
The accumulator stores `_state_as_of_date = W` having folded in exactly the event_dates `<= W`, so serving an as-of date `T` requires `W <= T`.
An in-order run satisfies this automatically, leaving `W = T - late_arrival_days`.
If `W` moves ahead of the date being served, the sealed fold already holds events that partition must not see; the excess is inside the fold rather than beside it, and no tail can subtract it.
A generated test fails the build rather than publishing numbers that look plausible and score well offline, and the publisher re-checks the partition independently before writing anything to object storage.
The remedy is a one-line reset: `dbt run --full-refresh --select int_<name>__alltime_state --vars 'target_date: <date>'`.

The same rule bounds the revision window from below, which is the part that is easy to get backwards.
A partition dated before the watermark is **final** - its own late-arrival window closed before the seal - so rebuilding it would fold sealed state that postdates it into its `all_time` features.
`generator/revision.py` resolves the refreshable set to `[max(W, T - late_arrival_days), T - 1]`, and it lives in the compiler rather than in an operational script because it is a property of what the generated models mean.
A deployment binds it to its own registry and warehouse - `showcase/tools/revision_window.py` is one such binding - and both the DAG and `make dbt-revise` take their dates from it, so the rule has one home and is unit-tested directly.

Guarding the write is not the same as guarding the store, either.
The publisher refuses to write a leaking partition, but a store accumulates: partitions written by an earlier build or a buggier branch are still what a training pipeline reads.
`make -C showcase audit` checks the published Parquet as a consumer sees it and `audit-fix` republishes anything that violates its contract.

The accumulator writes only entities with activity in the range it consumes, which keeps write volume proportional to activity rather than to population.
One consequence is worth knowing: across a range with no events at all, nothing is written and the watermark does not move.
That is accurate rather than stuck - the state really is sealed only through the old watermark - and the next run with data consumes the wider range.
The watermark therefore tracks the last day that had events, not the last day attempted.

### The spine decides who gets a row, and what a missing one means

`entity_spine` is per spec, and the two specs in this repository deliberately differ so the trade-off can be read off the same source:

| | `fact_agg_features_login_history_v2` | `fact_agg_features_login_device_v1` |
|---|---|---|
| spine | `active_window` | `all_time` |
| rows over 17 dates | 2,493 | 2,903 |
| newest partition | 141 entities, decaying | 172 entities, flat |

The `active_window` mart decays from 165 entities to 141 as customers go quiet; the `all_time` mart holds flat at 172 and publishes every customer ever seen on every single day.
The gap widens over time, which is the point: `all_time`'s cost grows with the dormant tail, and on a real login journal that tail is most of the table.

What `active_window` costs is coverage, and it is worth stating plainly rather than discovering downstream.
A dormant entity has **no row** for that date, so an as-of equi-join misses rather than resolving to an older snapshot; on the fixture, 14.9% of the population known by a given cut-off is unscorable that day.
Treating a missing row as inactivity is usually right for counts and wrong for extrema and `all_time`, which are not zero for a dormant entity - merely unpublished.
The generated model carries that warning at the join site, and the showcase notebook measures the excluded population instead of letting an inner join hide it.

Running two spines against one source has a consequence of its own: joining the two marts on `(safe_id, target_date)` is asymmetric, because rows exist in the `all_time` mart that have no counterpart in the other.
That is fine as long as it is deliberate.

The risk `active_window` introduces is that going quiet might truncate an entity's history, which would make `all_time` silently reset on their return.
It does not: the spine narrows the mart, never the accumulator.
A scenario asserts each spec against its own setting in the same run - 28 entities drop out of the `active_window` mart and all 28 keep their full history, while the `all_time` mart drops nobody and publishes everything the accumulator holds.

### A partition is provisional until its late-arrival window closes

The partial layer keeps absorbing late events for `late_arrival_days`, which means a mart built on day *T* has stale inputs until day *T + late_arrival_days*.
Rewriting only today's partition is therefore not enough.
Each run republishes the preceding `late_arrival_days` partitions as well as its own; because every model is idempotent for a given as-of date, this is a refresh rather than a rewrite of history.

A partition is **provisional** inside its window and **final** afterwards, and the brute-force verifier holds the pipeline to exactly that contract rather than to a stricter one it never promised.

The revision window and the ordering rule are compatible rather than in tension, which is worth stating because it is easy to get backwards: an in-order run leaves the watermark at `T - late_arrival_days`, so every date the window rebuilds is at or after it.
Both are covered by scenarios, in both directions - the window's rebuilds are verified exact, and a date behind the watermark is verified to fail.

### As-of-date-dependent columns are removed, not merely ignored

`days_since_login` is `DATEDIFF(DAY, event_timestamp, target_date)`.
Its value changes every day, so storing it in the partial layer would silently invalidate every stored partial the next morning.

The generator detects this shape, **removes the expression from the staging projection entirely**, and rebuilds the feature in the mart from stored timestamps:

```
min(days_since_login) = target_date - max(event_timestamp)   -- flips the aggregation
max(days_since_login) = target_date - min(event_timestamp)
```

This is exact, keeps the partials reusable, and needs no change to the original spec - the derivation is inferred.
A target_date-dependent column the generator cannot interpret is a **hard error** with instructions, never a silent guess.

### Exact and approximate distinct, chosen per field

`count_distinct` is the one aggregation that cannot be summed from daily partials.

- `distinct_method: exact` retains the actual key set.
  Right for `device_id`: a customer owns a handful of devices, so the set is small and worth having exactly.
- `distinct_method: approx` uses a **KMV sketch** - fixed-size, mergeable, and built entirely from array primitives, so one implementation runs identically on DuckDB, Databricks and Snowflake.

Native HLL was rejected deliberately.
Databricks and Snowflake both have it; DuckDB exposes no mergeable sketch state, and the three binary formats are mutually unreadable.
That would mean the local stack could not reproduce production's numbers, which defeats the purpose of having a local stack.

Measured on the fixture (`make -C showcase kmv`): exact below k, mean error 5.4% above it, worst case inside 3 sigma.

### Generated SQL is committed and drift-checked

`make check` regenerates from the specs and fails on any difference.
CI runs it on every PR.

A PR adding one condition member shows the exact new feature columns in the diff, reviewable by people who do not read Jinja.
Hand-editing a generated model fails CI.
A spec and its models cannot disagree.

### Warehouse-agnostic through a small dispatch surface

The entire cross-dialect surface is **nine primitives** in `transform/macros/adapters/`.
Exact distinct, KMV sketches and the `all_time` merges are all composed from them, so porting to a new warehouse means implementing that one file.

`transform/tests/conformance/` asserts all 25 primitive contracts against whichever adapter is configured, so `dbt test --target databricks` *proves* the Databricks implementations agree rather than merely compiling.

The generated project itself carries no connection details and no fixture.
`transform/dbt_project.yml` names the profile it wants and stops there, which is why the same committed models run against the showcase's DuckDB and against a production warehouse without a diff.

## What is tested, and why it is enough

Passing tests that only check self-consistency are worthless here: every layer would agree on the same wrong answer.
So the suite is layered, and the layers are split across the two halves of the repository along the same line as everything else: what can be checked without a warehouse is checked here, and what needs one is checked in the showcase.

| Layer | What it catches | Run |
|---|---|---|
| 83 unit tests | expansion, naming, Jinja composition, the refreshable-window rule, every spec guard | `make test` |
| drift check | a generated model edited by hand, or a spec changed without regenerating | `make check` |
| ~1,170 generated invariants | window monotonicity, marginal dominance, min <= max, non-negativity - checked in one scan per test | `make -C showcase dbt-test` |
| 25 conformance contracts | a dialect primitive behaving differently from its spec | `make -C showcase dbt-test` |
| Brute-force recomputation | a *systematic* error the pipeline would agree with itself about | `make -C showcase verify` |
| 7 operational scenarios | retry double-counting, gap loss, late-arrival misbucketing, backwards-replay data loss, stale revision-window partitions, out-of-order serving, dormancy truncating history | `make -C showcase e2e` |
| Offline-store audit | published partitions that violate their own contract - future leakage, or a superseded spec version left behind by an earlier build | `make -C showcase audit` |
| Sketch accuracy | approximate counts drifting outside their bound | `make -C showcase kmv` |

The generated invariants are the interesting ones.
They do not check that the SQL ran; they check that the **algebra held on real data**.
A wider window that contains fewer events, a marginal count smaller than one of its own subsets, a minimum above its maximum - each is an arithmetic contradiction that surfaces the exact feature by name.

`make -C showcase verify` is the backstop: it recomputes a sample of features the naive way, one flat query straight over raw source, and requires an exact match on every entity.

## Quick start

```bash
make setup          # venv + the compiler, nothing else
make generate       # compile the specs into dbt models
make explain        # see how a spec expands
make ci             # drift check, lint, unit tests, examples
```

That is the whole loop for changing a spec.
To watch the generated project actually run - against DuckDB, with the fixture, the invariants, the brute-force check and the operational scenarios - see [`showcase/README.md`](showcase/README.md):

```bash
make -C showcase setup
make -C showcase seed
make -C showcase dbt-backfill TARGET_DATE=2026-08-20
```

## Writing a spec

`features/examples/` holds a worked spec for each capability - the six required keys alone, all six aggregations together, a composite entity key, and one exercising every optional key at once.
`make examples` parses and expands them all, and CI runs it, so an example that stops being valid fails the build rather than sitting wrong in the docs.

```bash
make explain FEATURE=<name>     # show the expansion, generate nothing
make validate                   # check every spec parses
make examples                   # check the documented examples still work
```

## Adding a feature

1. Edit or add a file in `features/`.
2. `make generate` - review the diff; it names every column added or removed.
3. `make -C showcase dbt-run && make -C showcase dbt-test`.
4. Commit spec and generated output together; CI enforces that they match.

The Airflow DAG in the showcase is built from `registry/*.json`, so a new spec picks up orchestration with no DAG edit.

## Repository layout

```
features/            feature specs -- the only hand-edited contract
generator/           spec -> dbt compiler
  spec.py            parsing, validation, derivation inference
  aggregates.py      the monoid algebra
  expand.py          cross-product expansion and naming
  expr.py            SQL/Jinja composition
  render.py          model, doc and test emission
  registry.py        the machine-readable contract
  revision.py        which past partitions may still be rebuilt
transform/           the generated dbt project
  dbt_project.yml    warehouse-agnostic; declares no connection, ships no data
  macros/adapters/   the 9-primitive dialect surface
  models/            GENERATED
  tests/             GENERATED invariants + hand-written conformance
registry/            GENERATED feature registry
tests/               compiler unit tests -- no warehouse, no dbt

showcase/            the end-to-end proof; see showcase/README.md
```

## Known limits

- **`feature_type` supports `daily` only.**
  Other cadences raise rather than silently mis-window.
- **The `source` block is hand-written SQL and is the one dialect-specific surface.**
  The generator removes as-of-date expressions from it, but the `FROM`/`WHERE` remain as authored.
  Everything the generator emits is portable.
- **The two specs run different spines on purpose.**
  `history_v2` is `active_window` and `device_v1` is `all_time`, so a join between them on `(safe_id, target_date)` is asymmetric by construction.
  Under `active_window` a dormant entity has **no row** rather than a row of zeros; their history is not lost, and they return with `all_time` intact.
- **Backfill reconstructs what was knowable at the backfill date**, not at each historical date.
  For events arriving later than `late_arrival_days`, replay day by day instead.
