-- ============================================================================
-- GENERATED FILE - DO NOT EDIT BY HAND
--
--   layer      : test / all_time state is usable for this as-of date
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

-- The accumulator is a single forward-only fold, so its watermark is global
-- while a mart partition is per-date. This asserts the one relationship
-- between them that all_time correctness depends on.
--
-- The accumulator stores `_state_as_of_date = W` having folded in exactly
-- the event_dates <= W. So for an as-of date T, whose all_time must see
-- event_dates <= T and nothing later:
--
--     W <= T
--
-- Recomputing a PAST mart partition is safe and required -- that is what the
-- revision window does -- and it satisfies this automatically: an in-order
-- run leaves W = T - 3, so every date in the revision window
-- (T-1 .. T-3) is at or after W. The unsealed tail, anchored to W rather
-- than to the as-of date, supplies the remainder exactly.
--
-- What this catches is a watermark that has moved AHEAD of the date being
-- served, which happens when as-of dates are run out of order. Then the
-- sealed fold already contains events this partition must not see, the
-- excess is inside the fold rather than beside it, and no tail can subtract
-- it: every all_time feature for this date would leak the future. So the
-- build fails here instead of publishing numbers that look plausible and
-- score well offline.
--
-- Remedy -- rebuild the accumulator at the date being served:
--   dbt run --full-refresh --select int_fact_agg_features_login_history_v2__alltime_state --vars 'target_date: <date>'

with watermark as (

    select coalesce(max(_state_as_of_date), cast('1900-01-01' as date)) as wm
    from {{ ref('int_fact_agg_features_login_history_v2__alltime_state') }}

)

select
    wm as sealed_through,
    {{ fs_target_date() }} as as_of_date,
    {{ fs_datediff_day(fs_target_date(), 'wm') }} as days_ahead,
    'sealed all_time state contains events this as-of date must not see'
        as problem
from watermark
where wm > {{ fs_target_date() }}
