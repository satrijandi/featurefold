# =============================================================================
# feature-mart -- the end-to-end showcase.
#
# The compiler in the repository root turns feature specs into a dbt project.
# This directory is what proves that project produces the right numbers: a
# DuckDB warehouse, a synthetic fixture with the awkward cases in it, SeaweedFS
# standing in for the lake, Airflow running the registry-derived DAG, and
# JupyterLab reading the published store the way a training pipeline would.
#
# None of it is a dependency of the compiler. The dbt project lives in
# ../transform and is read-only from here: it is generated, committed output.
# Everything a run produces -- the warehouse, target/, logs/ -- stays in this
# directory so the core repository is never dirtied by executing it.
# =============================================================================

REPO          := $(abspath $(CURDIR)/..)
PY            := $(CURDIR)/.venv/bin/python
DBT           := $(CURDIR)/.venv/bin/dbt
SQLFLUFF      := $(CURDIR)/.venv/bin/sqlfluff
LOCAL_ENV     := infra/local.env
COMPOSE       := docker compose -f infra/docker-compose.yml --env-file $(LOCAL_ENV)

TARGET_DATE   ?= $(shell date -u +%F)
BACKFILL_FROM ?= 2026-07-01
FEATURE       ?= fact_agg_features_login_history_v2

# The rehearsal the gate walks. The fixture runs 2026-07-01 to 2026-09-05, so
# these dates sit inside it with a provisional tail still open at the end.
FIRST_DATE    ?= 2026-08-20
LAST_DATE     ?= 2026-09-03
VERIFY_DATES  ?= 2026-08-25 2026-08-31 2026-09-01 2026-09-02 2026-09-03

# The generated project is over in the core repository; its run artefacts are
# not. Setting all four here means every recipe below is a plain dbt command.
export DBT_PROJECT_DIR  = $(REPO)/transform
export DBT_PROFILES_DIR = $(CURDIR)
export DBT_TARGET_PATH  = $(CURDIR)/target
export DBT_LOG_PATH     = $(CURDIR)/logs
export DBT_DUCKDB_PATH  = $(CURDIR)/warehouse.duckdb

# Object-store credentials for the local stack, read from the one file that
# holds them. `?=` so that anything already in the environment -- a real store,
# a CI secret -- wins over the throwaway local keys. Only the S3_ keys: the
# tools need nothing else from it, and the Airflow ones are docker compose's.
$(foreach kv,$(shell grep -E '^S3_[A-Z0-9_]+=' $(LOCAL_ENV)),\
  $(eval $(firstword $(subst =, ,$(kv))) ?= $(word 2,$(subst =, ,$(kv)))))
export S3_ENDPOINT S3_ACCESS_KEY S3_SECRET_KEY

.DEFAULT_GOAL := help
.PHONY: help setup seed dbt-backfill dbt-run dbt-revise dbt-test dbt-docs \
        dbt-docs-generate rehearse verify e2e kmv audit audit-fix publish \
        notebook lint sqllint evaluate test stack-up stack-down stack-logs ci clean

help:  ## Show this help
	@grep -hE '^[a-zA-Z0-9_-]+:.*?## ' $(MAKEFILE_LIST) \
	  | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

# --- toolchain --------------------------------------------------------------
setup:  ## Create the showcase venv: the compiler plus a dbt runtime
	uv venv --python 3.11
	uv pip install -e .. -e .

# --- the source data --------------------------------------------------------
seed:  ## Generate the synthetic fixture and load it as the bronze source
	$(PY) -m tools.gen_seed

# --- dbt --------------------------------------------------------------------
# --full-refresh, because a backfill restarts history. Without it an existing
# all_time accumulator keeps its watermark, which then sits ahead of every date
# the backfill goes on to serve, and every all_time feature leaks the future.
dbt-backfill:  ## Initial load: rebuild everything from BACKFILL_FROM in one pass
	$(DBT) run --full-refresh --vars '{target_date: $(TARGET_DATE), fs_backfill_from: $(BACKFILL_FROM)}'

dbt-run:  ## Build one as-of date
	$(DBT) run --vars '{target_date: $(TARGET_DATE)}'

# Every spec, as the DAG does: each has its own window, resolved from its own
# registry and accumulator. Set REVISE to narrow it to some of them.
REVISE ?= $(basename $(notdir $(wildcard $(REPO)/registry/*.json)))

dbt-revise:  ## Refresh the partitions that are still provisional, for every spec
	@for f in $(REVISE); do \
	  dates=$$($(PY) -m tools.revision_window $$f $(TARGET_DATE)) || exit 1; \
	  if [ -z "$$dates" ]; then echo "$$f: nothing refreshable, earlier partitions are final"; \
	  else for d in $$dates; do echo "$$f: refreshing $$d"; \
	    $(DBT) build --select "tag:$$f" --vars "{target_date: $$d}" -q || exit 1; \
	  done; fi; done

dbt-test:  ## Run schema tests, the generated invariants, and adapter conformance
	$(DBT) test --vars '{target_date: $(TARGET_DATE)}'

dbt-docs-generate:  ## Build the dbt docs site
	$(DBT) docs generate --vars '{target_date: $(TARGET_DATE)}'

dbt-docs: dbt-docs-generate  ## Build and serve the dbt docs site
	$(DBT) docs serve --port 8082

rehearse:  ## Backfill, then walk daily increments to LAST_DATE as the DAG would
	$(MAKE) dbt-backfill TARGET_DATE=$(FIRST_DATE)
	@dates=$$($(PY) -c "import datetime as d; a=d.date.fromisoformat('$(FIRST_DATE)'); b=d.date.fromisoformat('$(LAST_DATE)'); print(' '.join(str(a + d.timedelta(days=i)) for i in range(1, (b - a).days + 1)))"); \
	for t in $$dates; do \
	  echo ""; echo "=== $$t ==="; \
	  $(MAKE) dbt-run    TARGET_DATE=$$t || exit 1; \
	  $(MAKE) dbt-revise TARGET_DATE=$$t || exit 1; \
	done

# --- verification -----------------------------------------------------------
verify:  ## Compare the mart against an independent brute-force recomputation
	$(PY) -m tools.verify_against_bruteforce $(TARGET_DATE)

e2e:  ## Retry-idempotency, gap self-healing and late-arrival scenarios
	$(PY) -m tools.e2e_scenarios

kmv:  ## Measure the approximate-distinct sketch against exact ground truth
	$(PY) -m tools.verify_kmv_accuracy $(TARGET_DATE)

audit:  ## Audit the published offline store against its own contract
	$(PY) -m tools.audit_offline_store

audit-fix:  ## Republish any offline-store partition that violates its contract
	$(PY) -m tools.audit_offline_store --fix

publish:  ## Publish a mart partition to the offline store on S3
	$(PY) -m tools.publish_offline_store $(FEATURE) $(TARGET_DATE)

notebook:  ## Rebuild the validation notebook from its builder
	$(PY) -m tools.build_notebook

lint:  ## Lint the showcase tools
	$(CURDIR)/.venv/bin/ruff check tools tests orchestration
	$(CURDIR)/.venv/bin/ruff format --check tools tests orchestration

# sqlfluff renders the Jinja before it lints, so it only ever sees one side of
# each `is_incremental()` branch: the side the warehouse it compiles against
# selects. Two passes see both -- an empty warehouse renders the first-build
# branches, the real one (after `rehearse`) the incremental ones. The style
# rules themselves live with the project, in ../transform/.sqlfluff.
sqllint:  ## Lint the generated SQL and its runtime, on both sides of is_incremental()
	@empty=$$(mktemp -d); \
	echo "first-build branches (empty warehouse)"; \
	(cd $(DBT_PROJECT_DIR) && DBT_DUCKDB_PATH=$$empty/empty.duckdb $(SQLFLUFF) lint models tests); \
	status=$$?; rm -rf $$empty; exit $$status
	@echo "incremental branches (built warehouse)"
	cd $(DBT_PROJECT_DIR) && $(SQLFLUFF) lint models tests

# The evaluator is a dbt project of its own that installs ../transform as a
# package (see evaluator/dbt_project.yml for why). It builds into a scratch
# DuckDB file of its own, so it never touches the rehearsal warehouse.
evaluate:  ## Lint the generated project's structure with dbt_project_evaluator
	cd evaluator && \
	  export DBT_PROJECT_DIR=$(CURDIR)/evaluator DBT_TARGET_PATH=$(CURDIR)/target/evaluator \
	         DBT_DUCKDB_PATH=$(CURDIR)/target/evaluator.duckdb && \
	  $(DBT) deps && \
	  $(DBT) build --select package:dbt_project_evaluator dbt_project_evaluator_exceptions

test:  ## Unit-test the showcase tools
	$(PY) -m pytest tests -q

# --- local stack ------------------------------------------------------------
stack-up:  ## Start SeaweedFS, Airflow and Jupyter
	$(COMPOSE) --profile full up -d --build
	@echo ""
	@echo "  SeaweedFS S3   http://localhost:$${FS_S3_PORT:-8433}"
	@echo "  SeaweedFS UI   http://localhost:$${FS_SEAWEED_UI_PORT:-9433}"
	@echo "  Airflow        http://localhost:$${FS_AIRFLOW_PORT:-8081}  (login: $(LOCAL_ENV))"
	@echo "  JupyterLab     http://localhost:$${FS_JUPYTER_PORT:-8900}"

stack-down:  ## Stop the stack and drop its volumes
	$(COMPOSE) --profile full down -v

stack-logs:  ## Tail stack logs
	$(COMPOSE) --profile full logs -f --tail=100

# --- CI ---------------------------------------------------------------------
# Spelled out as recipe lines rather than prerequisites so the as-of dates are
# explicit. This is what the showcase job in CI runs, so a green run here means
# the same thing a green run there does.
ci:  ## The whole end-to-end gate, on the rehearsal dates
	$(MAKE) lint
	$(MAKE) test
	$(MAKE) evaluate
	$(MAKE) seed
	$(MAKE) rehearse
	$(MAKE) sqllint
	$(MAKE) dbt-test TARGET_DATE=$(LAST_DATE)
	@for d in $(VERIFY_DATES); do $(MAKE) verify TARGET_DATE=$$d || exit 1; done
	$(MAKE) kmv TARGET_DATE=$(LAST_DATE)
	$(MAKE) e2e
	$(MAKE) dbt-docs-generate TARGET_DATE=$(LAST_DATE)
	@echo ""
	@echo "showcase OK"

clean:  ## Remove the warehouse and every run artefact
	rm -rf target logs warehouse.duckdb warehouse.duckdb.wal seeds .pytest_cache .ruff_cache
	find . -name __pycache__ -type d -prune -exec rm -rf {} +
