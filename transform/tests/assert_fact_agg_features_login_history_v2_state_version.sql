-- ============================================================================
-- GENERATED FILE - DO NOT EDIT BY HAND
--
--   layer      : test / stored state was built by this spec
--   feature    : fact_agg_features_login_history_v2
--   spec       : features/fact_agg_features_login_history_v2.yml
--   spec hash  : a7a086250bfc
--   generator  : featurefold
--
-- Edit the spec and run `make generate`. CI fails when a generated file
-- differs from what the spec produces, so this file and the spec cannot
-- drift apart.
-- ============================================================================

{{ config(group='data_platform') }}

-- Stored state is only reusable while it means what the spec says. Change a
-- predicate, a source expression or the time zone and every row folded
-- before the change still holds the OLD meaning, with nothing in its
-- columns to show it: the partial layer only rewrites the days that
-- changed, and the accumulator never revisits sealed history unprompted.
-- So every stored row carries the fingerprint of the meaning it was built
-- under (dfceaf462f00), and this fails the build while any row
-- carries another.
--
-- Remedy: rebuild the stored history under the new meaning,
--   make -C showcase dbt-backfill TARGET_DATE=<date> BACKFILL_FROM=<start>
-- or, when the published history must stay as it was, ship the change
-- as a new spec version alongside this one.

select
    'daily_partials' as layer,
    _state_version as found,
    count(*) as n_rows
from {{ ref('int_fact_agg_features_login_history_v2__daily_partials') }}
where _state_version is distinct from 'dfceaf462f00'
group by _state_version

union all

select
    'alltime_state' as layer,
    _state_version as found,
    count(*) as n_rows
from {{ ref('int_fact_agg_features_login_history_v2__alltime_state') }}
where _state_version is distinct from 'dfceaf462f00'
group by _state_version
