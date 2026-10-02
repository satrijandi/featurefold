-- ============================================================================
-- GENERATED FILE - DO NOT EDIT BY HAND
--
--   layer      : intermediate / events
--   feature    : fact_agg_features_login_device_v1
--   spec       : features/fact_agg_features_login_device_v1.yml
--   spec hash  : e1ebed8aec82
--   generator  : featuremart
--
-- Edit the spec and run `make generate`. CI fails when a generated file
-- differs from what the spec produces, so this file and the spec cannot
-- drift apart.
-- ============================================================================

-- The spec's projection over every row version the source holds, with no
-- as-of filter. Deciding which versions a run may see is the partial
-- layer's job, because it needs the versions it may NOT see as well: a
-- version superseded since the last run is how a correction or a deletion
-- announces which day it changed.
--
-- CLOCK. Timestamps are recorded in UTC and read here on the
-- business clock, UTC, so an event's date and the hour a
-- condition sees never depend on the warehouse session's time zone.

{{ config(materialized='view', tags=['feature_store', 'fact_agg_features_login_device_v1']) }}

select
    customer_id as safe_id,
    device_id,
    event_id,
    {{ fs_convert_tz("event_timestamp", 'UTC', 'UTC') }} as event_timestamp,
    login_source,
    os_name,
    event_status,
    cast({{ fs_convert_tz("event_timestamp", 'UTC', 'UTC') }} as date) as event_date,
    _fs_loaded_at,
    _fs_superseded_at
from {{ ref('stg_bronze_events__customer_login') }}
where customer_id is not null
