# featurefold

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
Adding an aggregation is a change to `generator/aggregates.py` alone: four small methods for the fold, plus the facts the generated invariant tests assert about it - which way it moves as it covers more rows, and what bounds it.
It does not mean touching spec validation, the templates, the mart, the registry, or the orchestration.

This is also what makes the cost sane.
Reading a day of source once produces state that every window reuses, so adding `l60d` and `l90d` to the spec costs no extra source reads.

## The decisions that shaped it

### Reusable daily partials, not per-window recomputation

`int_<name>__daily_partials` holds composable state at entity x event_date, and every window folds over it.
The alternative - rescanning raw source per window - re-reads the same days once per window and makes `all_time` cost grow without bound.

### A run reads the source as of a moment in time, and rewrites exactly what changed

A feature dated *T* may only see what was knowable when *T* ended.
The spec says how to tell, once per source table, instead of hand-writing an as-of filter:

```yaml
relations:
  bronze.events.customer_login:
    source_name: bronze_events
    table_name: customer_login
    loaded_at: _scd_valid_from      # when each row version became visible
    superseded_at: _scd_valid_to    # when it was corrected or deleted, if ever
    timezone: UTC                   # what the table's naive timestamps mean
    key: [event_id]                 # what identifies a row across its versions
```

A run for *T* sees a row version when it was loaded before *T* ended and was not superseded by then.
So a correction is read as a correction and a deletion as a deletion, not frozen at whatever the first run saw.

The same two columns tell each run which days to rewrite: every event_date with a row version loaded or superseded since the previous run's knowledge ended, plus any day newly inside the as-of horizon.
A late arrival, a correction and a deletion all announce their day this way, however far back it lies.
An earlier version of this design rewrote a fixed look-back window instead, and an end-to-end scenario showed what that costs: a correction to a day behind the window never reached the mart, in `l30d` as well as in `all_time`.

A day is rewritten whole.
An entity-day whose every row has since been superseded is written as a **tombstone** - each aggregation's identity state, with `_n_rows = 0` - so the stale row cannot survive, and an entity left with no events at all stops being published.

Where a run's knowledge ends is recorded on every row it writes (`_known_through`), and the next run looks for changes from exactly there.
If a run starts before its as-of day has ended on the business clock, its knowledge ends at the moment it ran instead, so rows loaded later that day are picked up by the next run rather than skipped as already seen.

The staging model tests the history this rests on: no row version is superseded before it was loaded, and no two versions of one key are ever current at once, which would count that row twice.

### `all_time` is a sealed accumulator with an unsealed tail

The accumulator folds in only event_dates at or before `target_date - late_arrival_days`.
More recent days are still provisional and rewritten often, so the mart completes `all_time` by merging the sealed state with the unsealed tail **using the same merge function**.
The published number is never stale, and late events still land on the day they happened.

The consumed range is `(stored watermark, seal date]`, not "yesterday".
Two properties follow, and both are tested:

- **Retries are idempotent.** A repeated run consumes an empty range and changes nothing.
  Without this, a retried task silently double-counts every `all_time` feature.
- **Gaps self-heal.** A run following a missed day absorbs the gap on its own, with no operator involved.

A sealed day can still change, and most aggregations cannot be undone: there is no way to "un-merge" a `max`.
So an entity whose sealed history was rewritten after it was folded is **restated**: re-folded from every stored day, by the same fold.
It is exact by construction and touches only the entities a change reached.

### Runs are not guaranteed to arrive in order

This turned out to be the sharpest edge in the whole design, and several separate invariants exist because of it.
Each was found by an end-to-end scenario, not by reasoning, and each is now a test.

Every run sees the source *as of its own* `target_date`.
That makes a forward-only sequence trivially correct and every other sequence subtly wrong.

**1. Stored partials may only ever gain information.**
Replaying a past date would recompute an already-complete day under a narrower view and silently drop events a later run had captured.
The partial layer stamps `_computed_for` and refuses to recompute any `event_date` already written under a later as-of date.

**2. Change detection is anchored to what the last run actually knew.**
A range measured from today would step straight over changes made while runs were missed.
Reading the previous run's `_known_through` from the stored rows makes the partial layer self-heal across a gap, exactly as the accumulator does, and makes a replay of an earlier date a no-op.

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

The accumulator writes only entities with activity in the range it consumes, or restated, which keeps write volume proportional to change rather than to population.
One consequence is worth knowing: across a range with no events at all, nothing is written and the watermark does not move.
That is accurate rather than stuck - the state really is sealed only through the old watermark - and the next run with data consumes the wider range.
The watermark therefore tracks the last day that had events, not the last day attempted.

### One business clock

An event's date, the hour a condition such as `is_late_night` sees, and the moment an as-of day ends are all read in the spec's `settings.timezone`.
Each source table declares the zone its naive timestamps are recorded in, and the generated models convert explicitly, through one dispatched primitive that leaves daylight saving to the zone database.
Nothing depends on the warehouse session's time zone, which differs between engines and between sessions on the same engine.
`fact_agg_features_login_history_v2` runs on `Asia/Jakarta` over a UTC journal, so the brute-force check proves the conversion end to end rather than on a zone where it is a no-op.

### The spine decides who gets a row, and what a missing one means

`entity_spine` is per spec, and the two specs in this repository deliberately differ so the trade-off can be read off the same source:

| | `fact_agg_features_login_history_v2` | `fact_agg_features_login_device_v1` |
|---|---|---|
| spine | `active_window` | `all_time` |
| rows over 17 dates | 2,463 | 2,914 |
| newest partition | 143 entities, decaying | 174 entities, never shrinking |

The `active_window` mart decays from 162 entities to 143 as customers go quiet; the `all_time` mart only grows, from 169 to 174, and publishes every customer ever seen on every single day.
The gap widens over time, which is the point: `all_time`'s cost grows with the dormant tail, and on a real login journal that tail is most of the table.

What `active_window` costs is coverage, and it is worth stating plainly rather than discovering downstream.
A dormant entity has **no row** for that date, so an as-of equi-join misses rather than resolving to an older snapshot; on the fixture, 12.8% of the population known by a given cut-off is unscorable that day.
Treating a missing row as inactivity is usually right for counts and wrong for extrema and `all_time`, which are not zero for a dormant entity - merely unpublished.
The generated model carries that warning at the join site, and the showcase notebook measures the excluded population instead of letting an inner join hide it.

Running two spines against one source has a consequence of its own: joining the two marts on `(safe_id, target_date)` is asymmetric, because rows exist in the `all_time` mart that have no counterpart in the other.
That is fine as long as it is deliberate.

The risk `active_window` introduces is that going quiet might truncate an entity's history, which would make `all_time` silently reset on their return.
It does not: the spine narrows the mart, never the accumulator.
A scenario asserts each spec against its own setting in the same run - 24 entities drop out of the `active_window` mart and all 24 keep their full history, while the `all_time` mart drops nobody and publishes everything the accumulator holds.

### A partition is provisional until its late-arrival window closes

Changes keep arriving after a partition is first built: late events, corrections, deletions.
Each run therefore republishes the preceding `late_arrival_days` partitions as well as its own, and each of those rebuilds reads what the partial layer knows by then.
Because every model is idempotent for a given as-of date, this is a refresh rather than a rewrite of history.

A partition is **provisional** inside its window and **final** afterwards, and the brute-force verifier holds the pipeline to exactly that contract rather than to a stricter one it never promised.
A change that lands later still reaches every partition built after it; the final ones keep what was knowable while they were open.

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

The generated SQL is held to a style guide like hand-written SQL would be.
`make -C showcase sqllint` runs sqlfluff through dbt's own compiler, twice, so both sides of every `is_incremental()` branch are linted; `make -C showcase evaluate` runs dbt-labs' `dbt_project_evaluator` over the project's structure - naming, layering, directories, documentation, test coverage and primary keys.
Both fail the build on any finding.
The rules live in `transform/.sqlfluff` and `showcase/evaluator/`, and the few deliberate departures are written down there with their reasons: line length, because one feature per line is what keeps a diff reviewable, and the accumulator feeding both the tail and the mart, which is the design.

### The mart is a contract

The published mart is the one model consumers read, so it is `access: public` with an enforced dbt contract: every column has a declared type, entity keys and `target_date` carry `not null` constraints, and every value is cast explicitly.
Everything below it is `private` to the owning team's group, so nothing outside can come to depend on an intermediate model.

A change to the set of columns **fails** the incremental build instead of syncing it.
Syncing would publish a new feature as NULL across every earlier partition, which a model reads as genuine absence.
Ship a changed feature set as a new spec version, or replay the history it needs.

Columns are not the only way stored state can change meaning.
Edit a predicate, a source expression or the time zone and every row folded before the edit still holds the old meaning, with nothing in its columns to show it.
So every partial and accumulator row carries a `_state_version`: a fingerprint of exactly what decides its meaning, deliberately blind to descriptions, windows and the spine.
A generated test fails the build while any stored row carries another.

### Ownership and lineage are declared, not inferred

Each spec's `created_by` becomes a dbt group that owns its models, and `exposures:` in the spec record who reads the mart - the offline store, a notebook, a model - so lineage runs from the bronze table to the things that would break.
Each source table has exactly one staging model, `stg_<source>__<table>`, and it is the only model that reads the source; specs that share a table share it, and must agree on what the table is.

### Warehouse-agnostic through a small dispatch surface

The entire cross-dialect surface is **ten primitives** in `transform/macros/adapters/`.
Exact distinct, KMV sketches, the `all_time` merges and the time zone conversions are all composed from them, so porting to a new warehouse means implementing that one directory.

`transform/tests/conformance/` asserts all 34 primitive contracts against whichever adapter is configured, so `dbt test --target databricks` *proves* the Databricks implementations agree rather than merely compiling.
They include both sides of a daylight-saving jump, and the end of a day on which one happens.

The generated project itself carries no connection details and no fixture.
`transform/dbt_project.yml` names the profile it wants and stops there, which is why the same committed models run against the showcase's DuckDB and against a production warehouse without a diff.

## What is tested, and why it is enough

Passing tests that only check self-consistency are worthless here: every layer would agree on the same wrong answer.
So the suite is layered, and the layers are split across the two halves of the repository along the same line as everything else: what can be checked without a warehouse is checked here, and what needs one is checked in the showcase.

| Layer | What it catches | Run |
|---|---|---|
| 140 unit tests | expansion, naming, Jinja composition, the refreshable-window rule, every spec guard, the knowledge-time and contract emission | `make test` |
| drift check | a generated model edited by hand, or a spec changed without regenerating | `make check` |
| SQL style and project structure | generated SQL that drifts from the style guide; naming, layering, docs, test coverage or keys that drift from dbt practice | `make -C showcase sqllint evaluate` |
| Source history tests | a row version superseded before it was loaded, two versions of one row current at once | `make -C showcase dbt-test` |
| ~1,700 generated invariants | window monotonicity, marginal dominance, min <= max, non-negativity - checked in one scan per test | `make -C showcase dbt-test` |
| Stored-state version | rows folded under an earlier meaning of the spec, kept beside rows folded under the new one | `make -C showcase dbt-test` |
| 34 conformance contracts | a dialect primitive behaving differently from its spec | `make -C showcase dbt-test` |
| Brute-force recomputation | a *systematic* error the pipeline would agree with itself about | `make -C showcase verify` |
| 8 operational scenarios | retry double-counting, gap loss, late-arrival misbucketing, backwards-replay data loss, stale revision-window partitions, out-of-order serving, dormancy truncating history, corrections and deletions to sealed history | `make -C showcase e2e` |
| Offline-store audit | published partitions that violate their own contract - future leakage, or a superseded spec version left behind by an earlier build | `make -C showcase audit` |
| Sketch accuracy | approximate counts drifting outside their bound | `make -C showcase kmv` |

The generated invariants are the interesting ones.
They do not check that the SQL ran; they check that the **algebra held on real data**.
A wider window that contains fewer events, a marginal count smaller than one of its own subsets, a minimum above its maximum - each is an arithmetic contradiction that surfaces the exact feature by name.

`make -C showcase verify` is the backstop: it recomputes a sample of features the naive way, one flat query straight over raw source, and requires an exact match on every entity.
It caught a bug the invariants could not: an early version of the change-driven partial layer counted every event of a first build several times over, uniformly, so every ratio between features still held.

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

`features/examples/` holds a worked spec for each capability - the required keys alone, all six aggregations together, a composite entity key, and one exercising every optional key at once.
The one thing every spec must say about its source table is `loaded_at`: when each row became visible, which is what every point-in-time read is built on.
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
  render.py          per-spec model, doc and test emission
  project.py         per-source staging, sources and owner groups
  registry.py        the machine-readable contract
  revision.py        which past partitions may still be rebuilt
transform/           the generated dbt project
  dbt_project.yml    warehouse-agnostic; declares no connection, ships no data
  .sqlfluff          the SQL style the generated models are held to
  macros/adapters/   the 10-primitive dialect surface
  models/            GENERATED: staging/<source>/, intermediate/<spec>/, marts/
  tests/             GENERATED invariants + hand-written conformance and generic tests
registry/            GENERATED feature registry
tests/               compiler unit tests -- no warehouse, no dbt

showcase/            the end-to-end proof; see showcase/README.md
```

## Known limits

- **`feature_type` supports `daily` only.**
  Other cadences raise rather than silently mis-window.
- **The `source` block is hand-written SQL and is the one dialect-specific surface.**
  The generator removes as-of-date expressions from it, writes the as-of filter itself and normalises its case, but the `WHERE` remains as authored.
  Everything the generator emits is portable.
- **A spec reads exactly one source table, and joins are rejected.**
  A joined table would be read as it stands today for every as-of date, which is a leak no test downstream can see.
  Materialise the join upstream into one table that carries its own `loaded_at`.
- **Source freshness reads `loaded_at` as UTC.**
  dbt evaluates `loaded_at_field` without the generator's macros, so a table whose timestamps are recorded in another zone has its freshness thresholds shifted by the offset.
- **The two specs run different spines on purpose.**
  `history_v2` is `active_window` and `device_v1` is `all_time`, so a join between them on `(safe_id, target_date)` is asymmetric by construction.
  Under `active_window` a dormant entity has **no row** rather than a row of zeros; their history is not lost, and they return with `all_time` intact.
- **Backfill reconstructs what was knowable at the backfill date**, not at each historical date.
  The partitions it builds afterwards see corrections made before the backfill as if they had always been there.
