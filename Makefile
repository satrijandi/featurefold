# =============================================================================
# featurefold -- the compiler.
#
# The contract: `features/*.yml` is the only file a human edits. Everything
# under transform/models, transform/tests and registry/ is generated, committed
# and drift-checked, so a pull request shows exactly which feature columns a
# spec change adds or removes.
#
# Nothing here touches a warehouse, and nothing here needs dbt installed.
# Running the generated project end to end -- DuckDB, SeaweedFS, Airflow,
# Jupyter -- is `make -C showcase`.
# =============================================================================

PY      := .venv/bin/python
FEATURE ?= fact_agg_features_login_history_v2

.DEFAULT_GOAL := help
.PHONY: help setup generate check validate explain examples lint test ci clean showcase

help:  ## Show this help
	@grep -hE '^[a-zA-Z0-9_-]+:.*?## ' $(MAKEFILE_LIST) \
	  | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

# --- toolchain --------------------------------------------------------------
setup:  ## Create the venv and install the compiler
	uv venv --python 3.11
	uv pip install -e ".[dev]"

# --- the generator ----------------------------------------------------------
generate:  ## Compile feature specs into dbt models, tests and the registry
	$(PY) -m generator.cli generate

check:  ## Fail if generated output is stale (this is what CI runs)
	$(PY) -m generator.cli generate --check

validate:  ## Validate every spec without generating
	$(PY) -m generator.cli validate

explain:  ## Show how FEATURE=<name> expands
	$(PY) -m generator.cli explain features/$(FEATURE).yml

examples:  ## Parse and expand every spec in features/examples/
	@for f in features/examples/*.yml; do \
	  $(PY) -m generator.cli explain $$f > /dev/null || exit 1; \
	  echo "  ok   $$f"; \
	done

lint:  ## Lint the compiler
	.venv/bin/ruff check generator tests
	.venv/bin/ruff format --check generator tests

test:  ## Unit-test the compiler
	$(PY) -m pytest tests -q

# --- CI ---------------------------------------------------------------------
ci: check lint test examples  ## Everything CI runs before touching a warehouse
	@echo "generator OK"

# --- the end-to-end showcase ------------------------------------------------
showcase:  ## Show what the end-to-end showcase can do
	@$(MAKE) -C showcase help

clean:  ## Remove build artefacts
	rm -rf .pytest_cache .ruff_cache
	find . -name __pycache__ -type d -prune -exec rm -rf {} +
