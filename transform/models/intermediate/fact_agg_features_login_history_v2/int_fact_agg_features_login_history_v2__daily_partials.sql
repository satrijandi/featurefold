-- ============================================================================
-- GENERATED FILE - DO NOT EDIT BY HAND
--
--   layer      : intermediate / daily partial aggregates
--   feature    : fact_agg_features_login_history_v2
--   spec       : features/fact_agg_features_login_history_v2.yml
--   spec hash  : a7a086250bfc
--   generator  : featuremart
--
-- Edit the spec and run `make generate`. CI fails when a generated file
-- differs from what the spec produces, so this file and the spec cannot
-- drift apart.
-- ============================================================================

-- One row per entity per event_date holding COMPOSABLE state, not finished
-- features. Every window and all_time is a fold over these rows, so a day is
-- read from the source exactly once per change, however many windows use it.
--
-- WHAT A RUN MAY KNOW. As-of date T sees a row version when it was loaded
-- before T ended on the business clock and was not superseded by then.
-- Corrections and deletions are therefore read, not ignored: a superseded
-- version drops out and its successor, if any, takes its place.
--
-- WHAT A RUN REWRITES. Exactly the event_dates whose content can differ
-- from what is stored: dates newly inside the as-of horizon, and dates with
-- a row version loaded or superseded since the previous run's knowledge
-- ended. A late arrival, a correction and a deletion all announce the day
-- they change this way, however far back it lies, so no fixed look-back
-- window can step over one. A day is rewritten whole.

{{ config(
    materialized='incremental',
    incremental_strategy=fs_partition_replace_strategy(),
    unique_key=['event_date'],
    partition_by=fs_partition_config(['event_date']),
    cluster_by=fs_cluster_config(['event_date']),
    on_schema_change='fail',
    tags=['feature_store', 'fact_agg_features_login_history_v2']
) }}

with

{% if is_incremental() %}
run_state as (

    -- The previous run's as-of date and the instant its knowledge ended.
    -- Taken from the stored rows rather than from the calendar, so a run
    -- that follows missed days covers every change made while it was away.
    select
        coalesce(max(_computed_for), cast('1900-01-01' as date)) as as_of,
        coalesce(max(_known_through), cast('1900-01-01' as timestamp)) as known_through
    from {{ this }}

),

-- MONOTONICITY GUARD.
-- Each run sees the source as of ITS OWN target_date, so recomputing an
-- event_date under an earlier as-of date would see less than a later run
-- already stored and silently drop what it had captured. Runs are not
-- guaranteed to arrive in order -- backfills, manual replays and retried
-- tasks all break that assumption -- so an event_date already computed
-- under a LATER as-of date is left alone.
already_fresher as (

    select distinct event_date
    from {{ this }}
    where _computed_for > {{ fs_date_offset_lit(0) }}

),
{% else %}
run_state as (

    -- First build: nothing is stored, so every day is new.
    select
        cast('1900-01-01' as date) as as_of,
        cast('1900-01-01' as timestamp) as known_through

),

already_fresher as (

    select cast(null as date) as event_date
    where 1 = 0

),
{% endif %}

backfill as (

    -- Initial load or replay: rebuild every day from the requested start.
    select
        cast('{{ var('fs_backfill_from', '1900-01-01') }}' as date) as rebuild_from,
        {{ 'true' if var('fs_backfill_from', none) else 'false' }} as rebuild_all

),

recompute as (

    select distinct e.event_date
    from {{ ref('int_fact_agg_features_login_history_v2__events') }} as e
    cross join run_state as r
    cross join backfill as b
    where
        e.event_date <= {{ fs_date_offset_lit(0) }}
        and e.event_date >= b.rebuild_from
        and (
            b.rebuild_all
            or e.event_date > r.as_of
            or (e._fs_loaded_at >= r.known_through and e._fs_loaded_at < {{ fs_knowledge_cutoff('Asia/Jakarta') }})
            or (e._fs_superseded_at >= r.known_through and e._fs_superseded_at < {{ fs_knowledge_cutoff('Asia/Jakarta') }})
        )
        and e.event_date not in (select already_fresher.event_date from already_fresher)

),

visible as (

    select e.*
    from {{ ref('int_fact_agg_features_login_history_v2__events') }} as e
    where
        e.event_date in (select recompute.event_date from recompute)
        and e._fs_loaded_at < {{ fs_knowledge_cutoff('Asia/Jakarta') }}
        and (e._fs_superseded_at is null or e._fs_superseded_at >= {{ fs_knowledge_cutoff('Asia/Jakarta') }})

),

keys as (

    -- Every entity-day this run writes, once. An entity-day stored earlier
    -- whose every row has since been superseded is written again as a
    -- TOMBSTONE: folded over no rows, which is each monoid's identity
    -- state, with _n_rows = 0. Writing nothing instead would leave the
    -- stale row in place wherever a day is only partly rewritten.
    select distinct
        safe_id,
        event_date
    from visible
    {% if is_incremental() %}

    union

    select distinct
        safe_id,
        event_date
    from {{ this }}
    where event_date in (select recompute.event_date from recompute)
    {% endif %}

),

keyed_rows as (

    -- One row per visible source row, and one all-NULL row per tombstone.
    select
        keys.safe_id,
        keys.event_date,
        visible.device_id,
        visible.event_id,
        visible.event_timestamp,
        visible.login_source,
        visible.os_name,
        visible.event_status,
        visible._fs_loaded_at,
        visible._fs_superseded_at
    from keys
    left join visible
        on
            keys.safe_id = visible.safe_id
            and keys.event_date = visible.event_date

)

select
    safe_id,
    event_date,
    -- event_id / count ----------------------------------------------------
    count(event_id) as p_count_event_id,
    count(case when (extract(hour from event_timestamp) between 0 and 4) then event_id end) as p_count_event_id_is_late_night,
    count(case when (extract(hour from event_timestamp) between 5 and 8) then event_id end) as p_count_event_id_is_early_morning,
    count(case when (extract(hour from event_timestamp) between 9 and 17) then event_id end) as p_count_event_id_is_office_hours,
    count(case when (extract(hour from event_timestamp) between 18 and 23) then event_id end) as p_count_event_id_is_evening_leisure,
    count(case when (upper(os_name) = 'IOS') then event_id end) as p_count_event_id_is_ios,
    count(case when (upper(os_name) = 'IOS') and (extract(hour from event_timestamp) between 0 and 4) then event_id end) as p_count_event_id_is_ios_is_late_night,
    count(case when (upper(os_name) = 'IOS') and (extract(hour from event_timestamp) between 5 and 8) then event_id end) as p_count_event_id_is_ios_is_early_morning,
    count(case when (upper(os_name) = 'IOS') and (extract(hour from event_timestamp) between 9 and 17) then event_id end) as p_count_event_id_is_ios_is_office_hours,
    count(case when (upper(os_name) = 'IOS') and (extract(hour from event_timestamp) between 18 and 23) then event_id end) as p_count_event_id_is_ios_is_evening_leisure,
    count(case when (upper(os_name) = 'ANDROID') then event_id end) as p_count_event_id_is_android,
    count(case when (upper(os_name) = 'ANDROID') and (extract(hour from event_timestamp) between 0 and 4) then event_id end) as p_count_event_id_is_android_is_late_night,
    count(case when (upper(os_name) = 'ANDROID') and (extract(hour from event_timestamp) between 5 and 8) then event_id end) as p_count_event_id_is_android_is_early_morning,
    count(case when (upper(os_name) = 'ANDROID') and (extract(hour from event_timestamp) between 9 and 17) then event_id end) as p_count_event_id_is_android_is_office_hours,
    count(case when (upper(os_name) = 'ANDROID') and (extract(hour from event_timestamp) between 18 and 23) then event_id end) as p_count_event_id_is_android_is_evening_leisure,
    count(case when (upper(os_name) not in ('IOS', 'ANDROID')) then event_id end) as p_count_event_id_is_others,
    count(case when (upper(os_name) not in ('IOS', 'ANDROID')) and (extract(hour from event_timestamp) between 0 and 4) then event_id end) as p_count_event_id_is_others_is_late_night,
    count(case when (upper(os_name) not in ('IOS', 'ANDROID')) and (extract(hour from event_timestamp) between 5 and 8) then event_id end) as p_count_event_id_is_others_is_early_morning,
    count(case when (upper(os_name) not in ('IOS', 'ANDROID')) and (extract(hour from event_timestamp) between 9 and 17) then event_id end) as p_count_event_id_is_others_is_office_hours,
    count(case when (upper(os_name) not in ('IOS', 'ANDROID')) and (extract(hour from event_timestamp) between 18 and 23) then event_id end) as p_count_event_id_is_others_is_evening_leisure,
    count(case when (upper(event_status) = 'SUCCESS') then event_id end) as p_count_event_id_is_login_success,
    count(case when (upper(event_status) = 'SUCCESS') and (extract(hour from event_timestamp) between 0 and 4) then event_id end) as p_count_event_id_is_login_success_is_late_night,
    count(case when (upper(event_status) = 'SUCCESS') and (extract(hour from event_timestamp) between 5 and 8) then event_id end) as p_count_event_id_is_login_success_is_early_morning,
    count(case when (upper(event_status) = 'SUCCESS') and (extract(hour from event_timestamp) between 9 and 17) then event_id end) as p_count_event_id_is_login_success_is_office_hours,
    count(case when (upper(event_status) = 'SUCCESS') and (extract(hour from event_timestamp) between 18 and 23) then event_id end) as p_count_event_id_is_login_success_is_evening_leisure,
    count(case when (upper(event_status) = 'SUCCESS') and (upper(os_name) = 'IOS') then event_id end) as p_count_event_id_is_login_success_is_ios,
    count(case when (upper(event_status) = 'SUCCESS') and (upper(os_name) = 'IOS') and (extract(hour from event_timestamp) between 0 and 4) then event_id end) as p_count_event_id_is_login_success_is_ios_is_late_night,
    count(case when (upper(event_status) = 'SUCCESS') and (upper(os_name) = 'IOS') and (extract(hour from event_timestamp) between 5 and 8) then event_id end) as p_count_event_id_is_login_success_is_ios_is_early_morning,
    count(case when (upper(event_status) = 'SUCCESS') and (upper(os_name) = 'IOS') and (extract(hour from event_timestamp) between 9 and 17) then event_id end) as p_count_event_id_is_login_success_is_ios_is_office_hours,
    count(case when (upper(event_status) = 'SUCCESS') and (upper(os_name) = 'IOS') and (extract(hour from event_timestamp) between 18 and 23) then event_id end) as p_count_event_id_is_login_success_is_ios_is_evening_leisure,
    count(case when (upper(event_status) = 'SUCCESS') and (upper(os_name) = 'ANDROID') then event_id end) as p_count_event_id_is_login_success_is_android,
    count(case when (upper(event_status) = 'SUCCESS') and (upper(os_name) = 'ANDROID') and (extract(hour from event_timestamp) between 0 and 4) then event_id end) as p_count_event_id_is_login_success_is_android_is_late_night,
    count(case when (upper(event_status) = 'SUCCESS') and (upper(os_name) = 'ANDROID') and (extract(hour from event_timestamp) between 5 and 8) then event_id end) as p_count_event_id_is_login_success_is_android_is_early_morning,
    count(case when (upper(event_status) = 'SUCCESS') and (upper(os_name) = 'ANDROID') and (extract(hour from event_timestamp) between 9 and 17) then event_id end) as p_count_event_id_is_login_success_is_android_is_office_hours,
    count(case when (upper(event_status) = 'SUCCESS') and (upper(os_name) = 'ANDROID') and (extract(hour from event_timestamp) between 18 and 23) then event_id end) as p_count_event_id_is_login_success_is_android_is_evening_leisure,
    count(case when (upper(event_status) = 'SUCCESS') and (upper(os_name) not in ('IOS', 'ANDROID')) then event_id end) as p_count_event_id_is_login_success_is_others,
    count(case when (upper(event_status) = 'SUCCESS') and (upper(os_name) not in ('IOS', 'ANDROID')) and (extract(hour from event_timestamp) between 0 and 4) then event_id end) as p_count_event_id_is_login_success_is_others_is_late_night,
    count(case when (upper(event_status) = 'SUCCESS') and (upper(os_name) not in ('IOS', 'ANDROID')) and (extract(hour from event_timestamp) between 5 and 8) then event_id end) as p_count_event_id_is_login_success_is_others_is_early_morning,
    count(case when (upper(event_status) = 'SUCCESS') and (upper(os_name) not in ('IOS', 'ANDROID')) and (extract(hour from event_timestamp) between 9 and 17) then event_id end) as p_count_event_id_is_login_success_is_others_is_office_hours,
    count(case when (upper(event_status) = 'SUCCESS') and (upper(os_name) not in ('IOS', 'ANDROID')) and (extract(hour from event_timestamp) between 18 and 23) then event_id end) as p_count_event_id_is_login_success_is_others_is_evening_leisure,
    count(case when (upper(event_status) = 'FAILED') then event_id end) as p_count_event_id_is_login_failed,
    count(case when (upper(event_status) = 'FAILED') and (extract(hour from event_timestamp) between 0 and 4) then event_id end) as p_count_event_id_is_login_failed_is_late_night,
    count(case when (upper(event_status) = 'FAILED') and (extract(hour from event_timestamp) between 5 and 8) then event_id end) as p_count_event_id_is_login_failed_is_early_morning,
    count(case when (upper(event_status) = 'FAILED') and (extract(hour from event_timestamp) between 9 and 17) then event_id end) as p_count_event_id_is_login_failed_is_office_hours,
    count(case when (upper(event_status) = 'FAILED') and (extract(hour from event_timestamp) between 18 and 23) then event_id end) as p_count_event_id_is_login_failed_is_evening_leisure,
    count(case when (upper(event_status) = 'FAILED') and (upper(os_name) = 'IOS') then event_id end) as p_count_event_id_is_login_failed_is_ios,
    count(case when (upper(event_status) = 'FAILED') and (upper(os_name) = 'IOS') and (extract(hour from event_timestamp) between 0 and 4) then event_id end) as p_count_event_id_is_login_failed_is_ios_is_late_night,
    count(case when (upper(event_status) = 'FAILED') and (upper(os_name) = 'IOS') and (extract(hour from event_timestamp) between 5 and 8) then event_id end) as p_count_event_id_is_login_failed_is_ios_is_early_morning,
    count(case when (upper(event_status) = 'FAILED') and (upper(os_name) = 'IOS') and (extract(hour from event_timestamp) between 9 and 17) then event_id end) as p_count_event_id_is_login_failed_is_ios_is_office_hours,
    count(case when (upper(event_status) = 'FAILED') and (upper(os_name) = 'IOS') and (extract(hour from event_timestamp) between 18 and 23) then event_id end) as p_count_event_id_is_login_failed_is_ios_is_evening_leisure,
    count(case when (upper(event_status) = 'FAILED') and (upper(os_name) = 'ANDROID') then event_id end) as p_count_event_id_is_login_failed_is_android,
    count(case when (upper(event_status) = 'FAILED') and (upper(os_name) = 'ANDROID') and (extract(hour from event_timestamp) between 0 and 4) then event_id end) as p_count_event_id_is_login_failed_is_android_is_late_night,
    count(case when (upper(event_status) = 'FAILED') and (upper(os_name) = 'ANDROID') and (extract(hour from event_timestamp) between 5 and 8) then event_id end) as p_count_event_id_is_login_failed_is_android_is_early_morning,
    count(case when (upper(event_status) = 'FAILED') and (upper(os_name) = 'ANDROID') and (extract(hour from event_timestamp) between 9 and 17) then event_id end) as p_count_event_id_is_login_failed_is_android_is_office_hours,
    count(case when (upper(event_status) = 'FAILED') and (upper(os_name) = 'ANDROID') and (extract(hour from event_timestamp) between 18 and 23) then event_id end) as p_count_event_id_is_login_failed_is_android_is_evening_leisure,
    count(case when (upper(event_status) = 'FAILED') and (upper(os_name) not in ('IOS', 'ANDROID')) then event_id end) as p_count_event_id_is_login_failed_is_others,
    count(case when (upper(event_status) = 'FAILED') and (upper(os_name) not in ('IOS', 'ANDROID')) and (extract(hour from event_timestamp) between 0 and 4) then event_id end) as p_count_event_id_is_login_failed_is_others_is_late_night,
    count(case when (upper(event_status) = 'FAILED') and (upper(os_name) not in ('IOS', 'ANDROID')) and (extract(hour from event_timestamp) between 5 and 8) then event_id end) as p_count_event_id_is_login_failed_is_others_is_early_morning,
    count(case when (upper(event_status) = 'FAILED') and (upper(os_name) not in ('IOS', 'ANDROID')) and (extract(hour from event_timestamp) between 9 and 17) then event_id end) as p_count_event_id_is_login_failed_is_others_is_office_hours,
    count(case when (upper(event_status) = 'FAILED') and (upper(os_name) not in ('IOS', 'ANDROID')) and (extract(hour from event_timestamp) between 18 and 23) then event_id end) as p_count_event_id_is_login_failed_is_others_is_evening_leisure,

    -- device_id / count_distinct ------------------------------------------
    {{ fs_collect_set("cast(device_id as varchar)") }} as p_count_distinct_device_id,
    {{ fs_collect_set("cast(case when (upper(os_name) = 'IOS') then device_id end as varchar)") }} as p_count_distinct_device_id_is_ios,
    {{ fs_collect_set("cast(case when (upper(os_name) = 'ANDROID') then device_id end as varchar)") }} as p_count_distinct_device_id_is_android,
    {{ fs_collect_set("cast(case when (upper(os_name) not in ('IOS', 'ANDROID')) then device_id end as varchar)") }} as p_count_distinct_device_id_is_others,
    {{ fs_collect_set("cast(case when (upper(event_status) = 'SUCCESS') then device_id end as varchar)") }} as p_count_distinct_device_id_is_login_success,
    {{ fs_collect_set("cast(case when (upper(event_status) = 'SUCCESS') and (upper(os_name) = 'IOS') then device_id end as varchar)") }} as p_count_distinct_device_id_is_login_success_is_ios,
    {{ fs_collect_set("cast(case when (upper(event_status) = 'SUCCESS') and (upper(os_name) = 'ANDROID') then device_id end as varchar)") }} as p_count_distinct_device_id_is_login_success_is_android,
    {{ fs_collect_set("cast(case when (upper(event_status) = 'SUCCESS') and (upper(os_name) not in ('IOS', 'ANDROID')) then device_id end as varchar)") }} as p_count_distinct_device_id_is_login_success_is_others,
    {{ fs_collect_set("cast(case when (upper(event_status) = 'FAILED') then device_id end as varchar)") }} as p_count_distinct_device_id_is_login_failed,
    {{ fs_collect_set("cast(case when (upper(event_status) = 'FAILED') and (upper(os_name) = 'IOS') then device_id end as varchar)") }} as p_count_distinct_device_id_is_login_failed_is_ios,
    {{ fs_collect_set("cast(case when (upper(event_status) = 'FAILED') and (upper(os_name) = 'ANDROID') then device_id end as varchar)") }} as p_count_distinct_device_id_is_login_failed_is_android,
    {{ fs_collect_set("cast(case when (upper(event_status) = 'FAILED') and (upper(os_name) not in ('IOS', 'ANDROID')) then device_id end as varchar)") }} as p_count_distinct_device_id_is_login_failed_is_others,

    -- event_timestamp / min -----------------------------------------------
    min(event_timestamp) as p_min_event_timestamp,
    min(case when (upper(os_name) = 'IOS') then event_timestamp end) as p_min_event_timestamp_is_ios,
    min(case when (upper(os_name) = 'ANDROID') then event_timestamp end) as p_min_event_timestamp_is_android,
    min(case when (upper(os_name) not in ('IOS', 'ANDROID')) then event_timestamp end) as p_min_event_timestamp_is_others,
    min(case when (upper(event_status) = 'SUCCESS') then event_timestamp end) as p_min_event_timestamp_is_login_success,
    min(case when (upper(event_status) = 'SUCCESS') and (upper(os_name) = 'IOS') then event_timestamp end) as p_min_event_timestamp_is_login_success_is_ios,
    min(case when (upper(event_status) = 'SUCCESS') and (upper(os_name) = 'ANDROID') then event_timestamp end) as p_min_event_timestamp_is_login_success_is_android,
    min(case when (upper(event_status) = 'SUCCESS') and (upper(os_name) not in ('IOS', 'ANDROID')) then event_timestamp end) as p_min_event_timestamp_is_login_success_is_others,
    min(case when (upper(event_status) = 'FAILED') then event_timestamp end) as p_min_event_timestamp_is_login_failed,
    min(case when (upper(event_status) = 'FAILED') and (upper(os_name) = 'IOS') then event_timestamp end) as p_min_event_timestamp_is_login_failed_is_ios,
    min(case when (upper(event_status) = 'FAILED') and (upper(os_name) = 'ANDROID') then event_timestamp end) as p_min_event_timestamp_is_login_failed_is_android,
    min(case when (upper(event_status) = 'FAILED') and (upper(os_name) not in ('IOS', 'ANDROID')) then event_timestamp end) as p_min_event_timestamp_is_login_failed_is_others,

    -- event_timestamp / max -----------------------------------------------
    max(event_timestamp) as p_max_event_timestamp,
    max(case when (upper(os_name) = 'IOS') then event_timestamp end) as p_max_event_timestamp_is_ios,
    max(case when (upper(os_name) = 'ANDROID') then event_timestamp end) as p_max_event_timestamp_is_android,
    max(case when (upper(os_name) not in ('IOS', 'ANDROID')) then event_timestamp end) as p_max_event_timestamp_is_others,
    max(case when (upper(event_status) = 'SUCCESS') then event_timestamp end) as p_max_event_timestamp_is_login_success,
    max(case when (upper(event_status) = 'SUCCESS') and (upper(os_name) = 'IOS') then event_timestamp end) as p_max_event_timestamp_is_login_success_is_ios,
    max(case when (upper(event_status) = 'SUCCESS') and (upper(os_name) = 'ANDROID') then event_timestamp end) as p_max_event_timestamp_is_login_success_is_android,
    max(case when (upper(event_status) = 'SUCCESS') and (upper(os_name) not in ('IOS', 'ANDROID')) then event_timestamp end) as p_max_event_timestamp_is_login_success_is_others,
    max(case when (upper(event_status) = 'FAILED') then event_timestamp end) as p_max_event_timestamp_is_login_failed,
    max(case when (upper(event_status) = 'FAILED') and (upper(os_name) = 'IOS') then event_timestamp end) as p_max_event_timestamp_is_login_failed_is_ios,
    max(case when (upper(event_status) = 'FAILED') and (upper(os_name) = 'ANDROID') then event_timestamp end) as p_max_event_timestamp_is_login_failed_is_android,
    max(case when (upper(event_status) = 'FAILED') and (upper(os_name) not in ('IOS', 'ANDROID')) then event_timestamp end) as p_max_event_timestamp_is_login_failed_is_others,

    count(_fs_loaded_at) as _n_rows,
    {{ fs_date_offset_lit(0) }} as _computed_for,
    {{ fs_knowledge_cutoff('Asia/Jakarta') }} as _known_through,
    'dfceaf462f00' as _state_version
from keyed_rows
group by safe_id, event_date
