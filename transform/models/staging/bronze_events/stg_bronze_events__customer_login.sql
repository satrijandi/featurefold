-- ============================================================================
-- GENERATED FILE - DO NOT EDIT BY HAND
--
--   layer      : staging
--   source     : bronze_events.customer_login
--   read by    : fact_agg_features_login_device_v1, fact_agg_features_login_history_v2
--   generator  : featurefold
--
-- Generated from the `relations:` block of the specs above. Edit those and
-- run `make generate`.
-- ============================================================================

-- The only model that reads this source, 1:1 with it. It passes every
-- column through untouched and adds the two knowledge-time columns every
-- point-in-time read depends on, under fixed names and in UTC:
--
--   _fs_loaded_at       when the row version became visible (_scd_valid_from)
--   _fs_superseded_at   when it was replaced or deleted, NULL if never
--
-- The table is versioned: a correction or deletion closes a row version.
-- Its naive timestamps are recorded in UTC.

{{ config(materialized='view', tags=['feature_store']) }}

select
    -- Every column, on purpose: specs name the ones they read, and a
    -- 1:1 staging model has no business choosing among them.
    src.*,  -- noqa: AM04
    {{ fs_convert_tz('src._scd_valid_from', 'UTC', 'UTC') }} as _fs_loaded_at,
    {{ fs_convert_tz('src._scd_valid_to', 'UTC', 'UTC') }} as _fs_superseded_at
from {{ source('bronze_events', 'customer_login') }} as src
