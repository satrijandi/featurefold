-- ============================================================================
-- GENERATED FILE - DO NOT EDIT BY HAND
--
--   layer      : intermediate / all_time sealed state
--   feature    : fact_agg_features_login_history_v2
--   spec       : features/fact_agg_features_login_history_v2.yml
--   spec hash  : a7a086250bfc
--   generator  : featurefold
--
-- Edit the spec and run `make generate`. CI fails when a generated file
-- differs from what the spec produces, so this file and the spec cannot
-- drift apart.
-- ============================================================================

-- The running all_time accumulator: one row per entity, holding the same
-- composable state as a daily partial but folded over all sealed history.
--
-- SEALING. Only event_dates at or before target_date - 3 are folded in,
-- because more recent days are still provisional and are rewritten often.
-- The mart completes all_time by merging this with the unsealed tail, using
-- the same merge function, so the published number is never stale.
--
-- SELF-HEALING AND IDEMPOTENT. The consumed range is
-- (stored watermark, seal date], not "yesterday". A retried run consumes an
-- empty range and changes nothing; a run that follows a missed day picks the
-- gap up automatically. Neither case needs an operator.
--
-- RESTATEMENT. A sealed day can still change: a correction, a deletion or
-- a very late arrival rewrites its partial row, stamped with the knowledge
-- of the run that rewrote it. Most aggregations have no inverse -- a max
-- cannot be "un-merged" -- so an entity whose sealed history changed after
-- it was folded is not patched but re-folded from every stored day. That
-- touches only the entities a change reached, and is exact by
-- construction: it is the same fold, over the same days.
--
-- Only entities with activity in the range, or restated, are written. An
-- entity with neither has nothing to fold, so leaving its row untouched is
-- correct and keeps write volume proportional to change, not population.
--
-- A consequence worth knowing: across a range with NO activity at all,
-- nothing is written and the watermark does not advance. That is accurate
-- rather than stuck -- the state really is sealed only through the old
-- watermark -- and the next run with data simply consumes the wider range.
-- It does mean the watermark tracks the last day with events, not the last
-- day attempted.

{{ config(
    materialized='incremental',
    incremental_strategy=fs_upsert_strategy(),
    unique_key=['safe_id'],
    on_schema_change='fail',
    tags=['feature_store', 'fact_agg_features_login_history_v2']
) }}

with watermark as (

    {% if is_incremental() %}
    select
        coalesce(max(_state_as_of_date), cast('1900-01-01' as date)) as wm,
        coalesce(max(_folded_through), cast('1900-01-01' as timestamp)) as folded_through
    from {{ this }}
    {% else %}
    select
        cast('1900-01-01' as date) as wm,
        cast('1900-01-01' as timestamp) as folded_through
    {% endif %}

),

seal as (

    -- Never behind the stored watermark. A run for an earlier as-of date
    -- has nothing new to seal, and must not re-fold a restated entity to
    -- a shorter history than every other entity holds.
    select
        w.wm,
        w.folded_through,
        {{ fs_greatest2('w.wm', fs_date_offset_lit(3)) }} as through
    from watermark as w

),

restated as (

    select distinct p.safe_id
    from {{ ref('int_fact_agg_features_login_history_v2__daily_partials') }} as p
    cross join seal as s
    where
        p.event_date <= s.wm
        and p._known_through > s.folded_through

),

new_days as (

    -- A restated entity folds every stored day through the seal; any
    -- other folds only the days after the watermark.
    select
        p.safe_id,
        -- event_id / count ----------------------------------------------------
        sum(p.p_count_event_id) as p_count_event_id,
        sum(p.p_count_event_id_is_late_night) as p_count_event_id_is_late_night,
        sum(p.p_count_event_id_is_early_morning) as p_count_event_id_is_early_morning,
        sum(p.p_count_event_id_is_office_hours) as p_count_event_id_is_office_hours,
        sum(p.p_count_event_id_is_evening_leisure) as p_count_event_id_is_evening_leisure,
        sum(p.p_count_event_id_is_ios) as p_count_event_id_is_ios,
        sum(p.p_count_event_id_is_ios_is_late_night) as p_count_event_id_is_ios_is_late_night,
        sum(p.p_count_event_id_is_ios_is_early_morning) as p_count_event_id_is_ios_is_early_morning,
        sum(p.p_count_event_id_is_ios_is_office_hours) as p_count_event_id_is_ios_is_office_hours,
        sum(p.p_count_event_id_is_ios_is_evening_leisure) as p_count_event_id_is_ios_is_evening_leisure,
        sum(p.p_count_event_id_is_android) as p_count_event_id_is_android,
        sum(p.p_count_event_id_is_android_is_late_night) as p_count_event_id_is_android_is_late_night,
        sum(p.p_count_event_id_is_android_is_early_morning) as p_count_event_id_is_android_is_early_morning,
        sum(p.p_count_event_id_is_android_is_office_hours) as p_count_event_id_is_android_is_office_hours,
        sum(p.p_count_event_id_is_android_is_evening_leisure) as p_count_event_id_is_android_is_evening_leisure,
        sum(p.p_count_event_id_is_others) as p_count_event_id_is_others,
        sum(p.p_count_event_id_is_others_is_late_night) as p_count_event_id_is_others_is_late_night,
        sum(p.p_count_event_id_is_others_is_early_morning) as p_count_event_id_is_others_is_early_morning,
        sum(p.p_count_event_id_is_others_is_office_hours) as p_count_event_id_is_others_is_office_hours,
        sum(p.p_count_event_id_is_others_is_evening_leisure) as p_count_event_id_is_others_is_evening_leisure,
        sum(p.p_count_event_id_is_login_success) as p_count_event_id_is_login_success,
        sum(p.p_count_event_id_is_login_success_is_late_night) as p_count_event_id_is_login_success_is_late_night,
        sum(p.p_count_event_id_is_login_success_is_early_morning) as p_count_event_id_is_login_success_is_early_morning,
        sum(p.p_count_event_id_is_login_success_is_office_hours) as p_count_event_id_is_login_success_is_office_hours,
        sum(p.p_count_event_id_is_login_success_is_evening_leisure) as p_count_event_id_is_login_success_is_evening_leisure,
        sum(p.p_count_event_id_is_login_success_is_ios) as p_count_event_id_is_login_success_is_ios,
        sum(p.p_count_event_id_is_login_success_is_ios_is_late_night) as p_count_event_id_is_login_success_is_ios_is_late_night,
        sum(p.p_count_event_id_is_login_success_is_ios_is_early_morning) as p_count_event_id_is_login_success_is_ios_is_early_morning,
        sum(p.p_count_event_id_is_login_success_is_ios_is_office_hours) as p_count_event_id_is_login_success_is_ios_is_office_hours,
        sum(p.p_count_event_id_is_login_success_is_ios_is_evening_leisure) as p_count_event_id_is_login_success_is_ios_is_evening_leisure,
        sum(p.p_count_event_id_is_login_success_is_android) as p_count_event_id_is_login_success_is_android,
        sum(p.p_count_event_id_is_login_success_is_android_is_late_night) as p_count_event_id_is_login_success_is_android_is_late_night,
        sum(p.p_count_event_id_is_login_success_is_android_is_early_morning) as p_count_event_id_is_login_success_is_android_is_early_morning,
        sum(p.p_count_event_id_is_login_success_is_android_is_office_hours) as p_count_event_id_is_login_success_is_android_is_office_hours,
        sum(p.p_count_event_id_is_login_success_is_android_is_evening_leisure) as p_count_event_id_is_login_success_is_android_is_evening_leisure,
        sum(p.p_count_event_id_is_login_success_is_others) as p_count_event_id_is_login_success_is_others,
        sum(p.p_count_event_id_is_login_success_is_others_is_late_night) as p_count_event_id_is_login_success_is_others_is_late_night,
        sum(p.p_count_event_id_is_login_success_is_others_is_early_morning) as p_count_event_id_is_login_success_is_others_is_early_morning,
        sum(p.p_count_event_id_is_login_success_is_others_is_office_hours) as p_count_event_id_is_login_success_is_others_is_office_hours,
        sum(p.p_count_event_id_is_login_success_is_others_is_evening_leisure) as p_count_event_id_is_login_success_is_others_is_evening_leisure,
        sum(p.p_count_event_id_is_login_failed) as p_count_event_id_is_login_failed,
        sum(p.p_count_event_id_is_login_failed_is_late_night) as p_count_event_id_is_login_failed_is_late_night,
        sum(p.p_count_event_id_is_login_failed_is_early_morning) as p_count_event_id_is_login_failed_is_early_morning,
        sum(p.p_count_event_id_is_login_failed_is_office_hours) as p_count_event_id_is_login_failed_is_office_hours,
        sum(p.p_count_event_id_is_login_failed_is_evening_leisure) as p_count_event_id_is_login_failed_is_evening_leisure,
        sum(p.p_count_event_id_is_login_failed_is_ios) as p_count_event_id_is_login_failed_is_ios,
        sum(p.p_count_event_id_is_login_failed_is_ios_is_late_night) as p_count_event_id_is_login_failed_is_ios_is_late_night,
        sum(p.p_count_event_id_is_login_failed_is_ios_is_early_morning) as p_count_event_id_is_login_failed_is_ios_is_early_morning,
        sum(p.p_count_event_id_is_login_failed_is_ios_is_office_hours) as p_count_event_id_is_login_failed_is_ios_is_office_hours,
        sum(p.p_count_event_id_is_login_failed_is_ios_is_evening_leisure) as p_count_event_id_is_login_failed_is_ios_is_evening_leisure,
        sum(p.p_count_event_id_is_login_failed_is_android) as p_count_event_id_is_login_failed_is_android,
        sum(p.p_count_event_id_is_login_failed_is_android_is_late_night) as p_count_event_id_is_login_failed_is_android_is_late_night,
        sum(p.p_count_event_id_is_login_failed_is_android_is_early_morning) as p_count_event_id_is_login_failed_is_android_is_early_morning,
        sum(p.p_count_event_id_is_login_failed_is_android_is_office_hours) as p_count_event_id_is_login_failed_is_android_is_office_hours,
        sum(p.p_count_event_id_is_login_failed_is_android_is_evening_leisure) as p_count_event_id_is_login_failed_is_android_is_evening_leisure,
        sum(p.p_count_event_id_is_login_failed_is_others) as p_count_event_id_is_login_failed_is_others,
        sum(p.p_count_event_id_is_login_failed_is_others_is_late_night) as p_count_event_id_is_login_failed_is_others_is_late_night,
        sum(p.p_count_event_id_is_login_failed_is_others_is_early_morning) as p_count_event_id_is_login_failed_is_others_is_early_morning,
        sum(p.p_count_event_id_is_login_failed_is_others_is_office_hours) as p_count_event_id_is_login_failed_is_others_is_office_hours,
        sum(p.p_count_event_id_is_login_failed_is_others_is_evening_leisure) as p_count_event_id_is_login_failed_is_others_is_evening_leisure,

        -- device_id / count_distinct ------------------------------------------
        {{ fs_array_union_agg("p.p_count_distinct_device_id") }} as p_count_distinct_device_id,
        {{ fs_array_union_agg("p.p_count_distinct_device_id_is_ios") }} as p_count_distinct_device_id_is_ios,
        {{ fs_array_union_agg("p.p_count_distinct_device_id_is_android") }} as p_count_distinct_device_id_is_android,
        {{ fs_array_union_agg("p.p_count_distinct_device_id_is_others") }} as p_count_distinct_device_id_is_others,
        {{ fs_array_union_agg("p.p_count_distinct_device_id_is_login_success") }} as p_count_distinct_device_id_is_login_success,
        {{ fs_array_union_agg("p.p_count_distinct_device_id_is_login_success_is_ios") }} as p_count_distinct_device_id_is_login_success_is_ios,
        {{ fs_array_union_agg("p.p_count_distinct_device_id_is_login_success_is_android") }} as p_count_distinct_device_id_is_login_success_is_android,
        {{ fs_array_union_agg("p.p_count_distinct_device_id_is_login_success_is_others") }} as p_count_distinct_device_id_is_login_success_is_others,
        {{ fs_array_union_agg("p.p_count_distinct_device_id_is_login_failed") }} as p_count_distinct_device_id_is_login_failed,
        {{ fs_array_union_agg("p.p_count_distinct_device_id_is_login_failed_is_ios") }} as p_count_distinct_device_id_is_login_failed_is_ios,
        {{ fs_array_union_agg("p.p_count_distinct_device_id_is_login_failed_is_android") }} as p_count_distinct_device_id_is_login_failed_is_android,
        {{ fs_array_union_agg("p.p_count_distinct_device_id_is_login_failed_is_others") }} as p_count_distinct_device_id_is_login_failed_is_others,

        -- event_timestamp / min -----------------------------------------------
        min(p.p_min_event_timestamp) as p_min_event_timestamp,
        min(p.p_min_event_timestamp_is_ios) as p_min_event_timestamp_is_ios,
        min(p.p_min_event_timestamp_is_android) as p_min_event_timestamp_is_android,
        min(p.p_min_event_timestamp_is_others) as p_min_event_timestamp_is_others,
        min(p.p_min_event_timestamp_is_login_success) as p_min_event_timestamp_is_login_success,
        min(p.p_min_event_timestamp_is_login_success_is_ios) as p_min_event_timestamp_is_login_success_is_ios,
        min(p.p_min_event_timestamp_is_login_success_is_android) as p_min_event_timestamp_is_login_success_is_android,
        min(p.p_min_event_timestamp_is_login_success_is_others) as p_min_event_timestamp_is_login_success_is_others,
        min(p.p_min_event_timestamp_is_login_failed) as p_min_event_timestamp_is_login_failed,
        min(p.p_min_event_timestamp_is_login_failed_is_ios) as p_min_event_timestamp_is_login_failed_is_ios,
        min(p.p_min_event_timestamp_is_login_failed_is_android) as p_min_event_timestamp_is_login_failed_is_android,
        min(p.p_min_event_timestamp_is_login_failed_is_others) as p_min_event_timestamp_is_login_failed_is_others,

        -- event_timestamp / max -----------------------------------------------
        max(p.p_max_event_timestamp) as p_max_event_timestamp,
        max(p.p_max_event_timestamp_is_ios) as p_max_event_timestamp_is_ios,
        max(p.p_max_event_timestamp_is_android) as p_max_event_timestamp_is_android,
        max(p.p_max_event_timestamp_is_others) as p_max_event_timestamp_is_others,
        max(p.p_max_event_timestamp_is_login_success) as p_max_event_timestamp_is_login_success,
        max(p.p_max_event_timestamp_is_login_success_is_ios) as p_max_event_timestamp_is_login_success_is_ios,
        max(p.p_max_event_timestamp_is_login_success_is_android) as p_max_event_timestamp_is_login_success_is_android,
        max(p.p_max_event_timestamp_is_login_success_is_others) as p_max_event_timestamp_is_login_success_is_others,
        max(p.p_max_event_timestamp_is_login_failed) as p_max_event_timestamp_is_login_failed,
        max(p.p_max_event_timestamp_is_login_failed_is_ios) as p_max_event_timestamp_is_login_failed_is_ios,
        max(p.p_max_event_timestamp_is_login_failed_is_android) as p_max_event_timestamp_is_login_failed_is_android,
        max(p.p_max_event_timestamp_is_login_failed_is_others) as p_max_event_timestamp_is_login_failed_is_others,

        sum(p._n_rows) as _n_rows,
        min(case when p._n_rows > 0 then p.event_date end) as _min_event_date,
        max(case when p._n_rows > 0 then p.event_date end) as _max_event_date,
        max(case when r.safe_id is null then 0 else 1 end) as _restated
    from {{ ref('int_fact_agg_features_login_history_v2__daily_partials') }} as p
    cross join seal as s
    left join restated as r on p.safe_id = r.safe_id
    where
        p.event_date <= s.through
        and (p.event_date > s.wm or r.safe_id is not null)
    group by p.safe_id

),

prev as (

    {% if is_incremental() %}
    select * from {{ this }}
    {% else %}
    -- First build: no rows, so the merge below is the only projection in
    -- the model and cannot diverge between branches.
    select * from new_days
    where 1 = 0
    {% endif %}

)

select
    n.safe_id,
    -- event_id / count ----------------------------------------------------
    (coalesce(prev.p_count_event_id, 0) + coalesce(n.p_count_event_id, 0)) as p_count_event_id,
    (coalesce(prev.p_count_event_id_is_late_night, 0) + coalesce(n.p_count_event_id_is_late_night, 0)) as p_count_event_id_is_late_night,
    (coalesce(prev.p_count_event_id_is_early_morning, 0) + coalesce(n.p_count_event_id_is_early_morning, 0)) as p_count_event_id_is_early_morning,
    (coalesce(prev.p_count_event_id_is_office_hours, 0) + coalesce(n.p_count_event_id_is_office_hours, 0)) as p_count_event_id_is_office_hours,
    (coalesce(prev.p_count_event_id_is_evening_leisure, 0) + coalesce(n.p_count_event_id_is_evening_leisure, 0)) as p_count_event_id_is_evening_leisure,
    (coalesce(prev.p_count_event_id_is_ios, 0) + coalesce(n.p_count_event_id_is_ios, 0)) as p_count_event_id_is_ios,
    (coalesce(prev.p_count_event_id_is_ios_is_late_night, 0) + coalesce(n.p_count_event_id_is_ios_is_late_night, 0)) as p_count_event_id_is_ios_is_late_night,
    (coalesce(prev.p_count_event_id_is_ios_is_early_morning, 0) + coalesce(n.p_count_event_id_is_ios_is_early_morning, 0)) as p_count_event_id_is_ios_is_early_morning,
    (coalesce(prev.p_count_event_id_is_ios_is_office_hours, 0) + coalesce(n.p_count_event_id_is_ios_is_office_hours, 0)) as p_count_event_id_is_ios_is_office_hours,
    (coalesce(prev.p_count_event_id_is_ios_is_evening_leisure, 0) + coalesce(n.p_count_event_id_is_ios_is_evening_leisure, 0)) as p_count_event_id_is_ios_is_evening_leisure,
    (coalesce(prev.p_count_event_id_is_android, 0) + coalesce(n.p_count_event_id_is_android, 0)) as p_count_event_id_is_android,
    (coalesce(prev.p_count_event_id_is_android_is_late_night, 0) + coalesce(n.p_count_event_id_is_android_is_late_night, 0)) as p_count_event_id_is_android_is_late_night,
    (coalesce(prev.p_count_event_id_is_android_is_early_morning, 0) + coalesce(n.p_count_event_id_is_android_is_early_morning, 0)) as p_count_event_id_is_android_is_early_morning,
    (coalesce(prev.p_count_event_id_is_android_is_office_hours, 0) + coalesce(n.p_count_event_id_is_android_is_office_hours, 0)) as p_count_event_id_is_android_is_office_hours,
    (coalesce(prev.p_count_event_id_is_android_is_evening_leisure, 0) + coalesce(n.p_count_event_id_is_android_is_evening_leisure, 0)) as p_count_event_id_is_android_is_evening_leisure,
    (coalesce(prev.p_count_event_id_is_others, 0) + coalesce(n.p_count_event_id_is_others, 0)) as p_count_event_id_is_others,
    (coalesce(prev.p_count_event_id_is_others_is_late_night, 0) + coalesce(n.p_count_event_id_is_others_is_late_night, 0)) as p_count_event_id_is_others_is_late_night,
    (coalesce(prev.p_count_event_id_is_others_is_early_morning, 0) + coalesce(n.p_count_event_id_is_others_is_early_morning, 0)) as p_count_event_id_is_others_is_early_morning,
    (coalesce(prev.p_count_event_id_is_others_is_office_hours, 0) + coalesce(n.p_count_event_id_is_others_is_office_hours, 0)) as p_count_event_id_is_others_is_office_hours,
    (coalesce(prev.p_count_event_id_is_others_is_evening_leisure, 0) + coalesce(n.p_count_event_id_is_others_is_evening_leisure, 0)) as p_count_event_id_is_others_is_evening_leisure,
    (coalesce(prev.p_count_event_id_is_login_success, 0) + coalesce(n.p_count_event_id_is_login_success, 0)) as p_count_event_id_is_login_success,
    (coalesce(prev.p_count_event_id_is_login_success_is_late_night, 0) + coalesce(n.p_count_event_id_is_login_success_is_late_night, 0)) as p_count_event_id_is_login_success_is_late_night,
    (coalesce(prev.p_count_event_id_is_login_success_is_early_morning, 0) + coalesce(n.p_count_event_id_is_login_success_is_early_morning, 0)) as p_count_event_id_is_login_success_is_early_morning,
    (coalesce(prev.p_count_event_id_is_login_success_is_office_hours, 0) + coalesce(n.p_count_event_id_is_login_success_is_office_hours, 0)) as p_count_event_id_is_login_success_is_office_hours,
    (coalesce(prev.p_count_event_id_is_login_success_is_evening_leisure, 0) + coalesce(n.p_count_event_id_is_login_success_is_evening_leisure, 0)) as p_count_event_id_is_login_success_is_evening_leisure,
    (coalesce(prev.p_count_event_id_is_login_success_is_ios, 0) + coalesce(n.p_count_event_id_is_login_success_is_ios, 0)) as p_count_event_id_is_login_success_is_ios,
    (coalesce(prev.p_count_event_id_is_login_success_is_ios_is_late_night, 0) + coalesce(n.p_count_event_id_is_login_success_is_ios_is_late_night, 0)) as p_count_event_id_is_login_success_is_ios_is_late_night,
    (coalesce(prev.p_count_event_id_is_login_success_is_ios_is_early_morning, 0) + coalesce(n.p_count_event_id_is_login_success_is_ios_is_early_morning, 0)) as p_count_event_id_is_login_success_is_ios_is_early_morning,
    (coalesce(prev.p_count_event_id_is_login_success_is_ios_is_office_hours, 0) + coalesce(n.p_count_event_id_is_login_success_is_ios_is_office_hours, 0)) as p_count_event_id_is_login_success_is_ios_is_office_hours,
    (coalesce(prev.p_count_event_id_is_login_success_is_ios_is_evening_leisure, 0) + coalesce(n.p_count_event_id_is_login_success_is_ios_is_evening_leisure, 0)) as p_count_event_id_is_login_success_is_ios_is_evening_leisure,
    (coalesce(prev.p_count_event_id_is_login_success_is_android, 0) + coalesce(n.p_count_event_id_is_login_success_is_android, 0)) as p_count_event_id_is_login_success_is_android,
    (coalesce(prev.p_count_event_id_is_login_success_is_android_is_late_night, 0) + coalesce(n.p_count_event_id_is_login_success_is_android_is_late_night, 0)) as p_count_event_id_is_login_success_is_android_is_late_night,
    (coalesce(prev.p_count_event_id_is_login_success_is_android_is_early_morning, 0) + coalesce(n.p_count_event_id_is_login_success_is_android_is_early_morning, 0)) as p_count_event_id_is_login_success_is_android_is_early_morning,
    (coalesce(prev.p_count_event_id_is_login_success_is_android_is_office_hours, 0) + coalesce(n.p_count_event_id_is_login_success_is_android_is_office_hours, 0)) as p_count_event_id_is_login_success_is_android_is_office_hours,
    (coalesce(prev.p_count_event_id_is_login_success_is_android_is_evening_leisure, 0) + coalesce(n.p_count_event_id_is_login_success_is_android_is_evening_leisure, 0)) as p_count_event_id_is_login_success_is_android_is_evening_leisure,
    (coalesce(prev.p_count_event_id_is_login_success_is_others, 0) + coalesce(n.p_count_event_id_is_login_success_is_others, 0)) as p_count_event_id_is_login_success_is_others,
    (coalesce(prev.p_count_event_id_is_login_success_is_others_is_late_night, 0) + coalesce(n.p_count_event_id_is_login_success_is_others_is_late_night, 0)) as p_count_event_id_is_login_success_is_others_is_late_night,
    (coalesce(prev.p_count_event_id_is_login_success_is_others_is_early_morning, 0) + coalesce(n.p_count_event_id_is_login_success_is_others_is_early_morning, 0)) as p_count_event_id_is_login_success_is_others_is_early_morning,
    (coalesce(prev.p_count_event_id_is_login_success_is_others_is_office_hours, 0) + coalesce(n.p_count_event_id_is_login_success_is_others_is_office_hours, 0)) as p_count_event_id_is_login_success_is_others_is_office_hours,
    (coalesce(prev.p_count_event_id_is_login_success_is_others_is_evening_leisure, 0) + coalesce(n.p_count_event_id_is_login_success_is_others_is_evening_leisure, 0)) as p_count_event_id_is_login_success_is_others_is_evening_leisure,
    (coalesce(prev.p_count_event_id_is_login_failed, 0) + coalesce(n.p_count_event_id_is_login_failed, 0)) as p_count_event_id_is_login_failed,
    (coalesce(prev.p_count_event_id_is_login_failed_is_late_night, 0) + coalesce(n.p_count_event_id_is_login_failed_is_late_night, 0)) as p_count_event_id_is_login_failed_is_late_night,
    (coalesce(prev.p_count_event_id_is_login_failed_is_early_morning, 0) + coalesce(n.p_count_event_id_is_login_failed_is_early_morning, 0)) as p_count_event_id_is_login_failed_is_early_morning,
    (coalesce(prev.p_count_event_id_is_login_failed_is_office_hours, 0) + coalesce(n.p_count_event_id_is_login_failed_is_office_hours, 0)) as p_count_event_id_is_login_failed_is_office_hours,
    (coalesce(prev.p_count_event_id_is_login_failed_is_evening_leisure, 0) + coalesce(n.p_count_event_id_is_login_failed_is_evening_leisure, 0)) as p_count_event_id_is_login_failed_is_evening_leisure,
    (coalesce(prev.p_count_event_id_is_login_failed_is_ios, 0) + coalesce(n.p_count_event_id_is_login_failed_is_ios, 0)) as p_count_event_id_is_login_failed_is_ios,
    (coalesce(prev.p_count_event_id_is_login_failed_is_ios_is_late_night, 0) + coalesce(n.p_count_event_id_is_login_failed_is_ios_is_late_night, 0)) as p_count_event_id_is_login_failed_is_ios_is_late_night,
    (coalesce(prev.p_count_event_id_is_login_failed_is_ios_is_early_morning, 0) + coalesce(n.p_count_event_id_is_login_failed_is_ios_is_early_morning, 0)) as p_count_event_id_is_login_failed_is_ios_is_early_morning,
    (coalesce(prev.p_count_event_id_is_login_failed_is_ios_is_office_hours, 0) + coalesce(n.p_count_event_id_is_login_failed_is_ios_is_office_hours, 0)) as p_count_event_id_is_login_failed_is_ios_is_office_hours,
    (coalesce(prev.p_count_event_id_is_login_failed_is_ios_is_evening_leisure, 0) + coalesce(n.p_count_event_id_is_login_failed_is_ios_is_evening_leisure, 0)) as p_count_event_id_is_login_failed_is_ios_is_evening_leisure,
    (coalesce(prev.p_count_event_id_is_login_failed_is_android, 0) + coalesce(n.p_count_event_id_is_login_failed_is_android, 0)) as p_count_event_id_is_login_failed_is_android,
    (coalesce(prev.p_count_event_id_is_login_failed_is_android_is_late_night, 0) + coalesce(n.p_count_event_id_is_login_failed_is_android_is_late_night, 0)) as p_count_event_id_is_login_failed_is_android_is_late_night,
    (coalesce(prev.p_count_event_id_is_login_failed_is_android_is_early_morning, 0) + coalesce(n.p_count_event_id_is_login_failed_is_android_is_early_morning, 0)) as p_count_event_id_is_login_failed_is_android_is_early_morning,
    (coalesce(prev.p_count_event_id_is_login_failed_is_android_is_office_hours, 0) + coalesce(n.p_count_event_id_is_login_failed_is_android_is_office_hours, 0)) as p_count_event_id_is_login_failed_is_android_is_office_hours,
    (coalesce(prev.p_count_event_id_is_login_failed_is_android_is_evening_leisure, 0) + coalesce(n.p_count_event_id_is_login_failed_is_android_is_evening_leisure, 0)) as p_count_event_id_is_login_failed_is_android_is_evening_leisure,
    (coalesce(prev.p_count_event_id_is_login_failed_is_others, 0) + coalesce(n.p_count_event_id_is_login_failed_is_others, 0)) as p_count_event_id_is_login_failed_is_others,
    (coalesce(prev.p_count_event_id_is_login_failed_is_others_is_late_night, 0) + coalesce(n.p_count_event_id_is_login_failed_is_others_is_late_night, 0)) as p_count_event_id_is_login_failed_is_others_is_late_night,
    (coalesce(prev.p_count_event_id_is_login_failed_is_others_is_early_morning, 0) + coalesce(n.p_count_event_id_is_login_failed_is_others_is_early_morning, 0)) as p_count_event_id_is_login_failed_is_others_is_early_morning,
    (coalesce(prev.p_count_event_id_is_login_failed_is_others_is_office_hours, 0) + coalesce(n.p_count_event_id_is_login_failed_is_others_is_office_hours, 0)) as p_count_event_id_is_login_failed_is_others_is_office_hours,
    (coalesce(prev.p_count_event_id_is_login_failed_is_others_is_evening_leisure, 0) + coalesce(n.p_count_event_id_is_login_failed_is_others_is_evening_leisure, 0)) as p_count_event_id_is_login_failed_is_others_is_evening_leisure,

    -- device_id / count_distinct ------------------------------------------
    {{ fs_array_union2("prev.p_count_distinct_device_id", "n.p_count_distinct_device_id") }} as p_count_distinct_device_id,
    {{ fs_array_union2("prev.p_count_distinct_device_id_is_ios", "n.p_count_distinct_device_id_is_ios") }} as p_count_distinct_device_id_is_ios,
    {{ fs_array_union2("prev.p_count_distinct_device_id_is_android", "n.p_count_distinct_device_id_is_android") }} as p_count_distinct_device_id_is_android,
    {{ fs_array_union2("prev.p_count_distinct_device_id_is_others", "n.p_count_distinct_device_id_is_others") }} as p_count_distinct_device_id_is_others,
    {{ fs_array_union2("prev.p_count_distinct_device_id_is_login_success", "n.p_count_distinct_device_id_is_login_success") }} as p_count_distinct_device_id_is_login_success,
    {{ fs_array_union2("prev.p_count_distinct_device_id_is_login_success_is_ios", "n.p_count_distinct_device_id_is_login_success_is_ios") }} as p_count_distinct_device_id_is_login_success_is_ios,
    {{ fs_array_union2("prev.p_count_distinct_device_id_is_login_success_is_android", "n.p_count_distinct_device_id_is_login_success_is_android") }} as p_count_distinct_device_id_is_login_success_is_android,
    {{ fs_array_union2("prev.p_count_distinct_device_id_is_login_success_is_others", "n.p_count_distinct_device_id_is_login_success_is_others") }} as p_count_distinct_device_id_is_login_success_is_others,
    {{ fs_array_union2("prev.p_count_distinct_device_id_is_login_failed", "n.p_count_distinct_device_id_is_login_failed") }} as p_count_distinct_device_id_is_login_failed,
    {{ fs_array_union2("prev.p_count_distinct_device_id_is_login_failed_is_ios", "n.p_count_distinct_device_id_is_login_failed_is_ios") }} as p_count_distinct_device_id_is_login_failed_is_ios,
    {{ fs_array_union2("prev.p_count_distinct_device_id_is_login_failed_is_android", "n.p_count_distinct_device_id_is_login_failed_is_android") }} as p_count_distinct_device_id_is_login_failed_is_android,
    {{ fs_array_union2("prev.p_count_distinct_device_id_is_login_failed_is_others", "n.p_count_distinct_device_id_is_login_failed_is_others") }} as p_count_distinct_device_id_is_login_failed_is_others,

    -- event_timestamp / min -----------------------------------------------
    {{ fs_least2("prev.p_min_event_timestamp", "n.p_min_event_timestamp") }} as p_min_event_timestamp,
    {{ fs_least2("prev.p_min_event_timestamp_is_ios", "n.p_min_event_timestamp_is_ios") }} as p_min_event_timestamp_is_ios,
    {{ fs_least2("prev.p_min_event_timestamp_is_android", "n.p_min_event_timestamp_is_android") }} as p_min_event_timestamp_is_android,
    {{ fs_least2("prev.p_min_event_timestamp_is_others", "n.p_min_event_timestamp_is_others") }} as p_min_event_timestamp_is_others,
    {{ fs_least2("prev.p_min_event_timestamp_is_login_success", "n.p_min_event_timestamp_is_login_success") }} as p_min_event_timestamp_is_login_success,
    {{ fs_least2("prev.p_min_event_timestamp_is_login_success_is_ios", "n.p_min_event_timestamp_is_login_success_is_ios") }} as p_min_event_timestamp_is_login_success_is_ios,
    {{ fs_least2("prev.p_min_event_timestamp_is_login_success_is_android", "n.p_min_event_timestamp_is_login_success_is_android") }} as p_min_event_timestamp_is_login_success_is_android,
    {{ fs_least2("prev.p_min_event_timestamp_is_login_success_is_others", "n.p_min_event_timestamp_is_login_success_is_others") }} as p_min_event_timestamp_is_login_success_is_others,
    {{ fs_least2("prev.p_min_event_timestamp_is_login_failed", "n.p_min_event_timestamp_is_login_failed") }} as p_min_event_timestamp_is_login_failed,
    {{ fs_least2("prev.p_min_event_timestamp_is_login_failed_is_ios", "n.p_min_event_timestamp_is_login_failed_is_ios") }} as p_min_event_timestamp_is_login_failed_is_ios,
    {{ fs_least2("prev.p_min_event_timestamp_is_login_failed_is_android", "n.p_min_event_timestamp_is_login_failed_is_android") }} as p_min_event_timestamp_is_login_failed_is_android,
    {{ fs_least2("prev.p_min_event_timestamp_is_login_failed_is_others", "n.p_min_event_timestamp_is_login_failed_is_others") }} as p_min_event_timestamp_is_login_failed_is_others,

    -- event_timestamp / max -----------------------------------------------
    {{ fs_greatest2("prev.p_max_event_timestamp", "n.p_max_event_timestamp") }} as p_max_event_timestamp,
    {{ fs_greatest2("prev.p_max_event_timestamp_is_ios", "n.p_max_event_timestamp_is_ios") }} as p_max_event_timestamp_is_ios,
    {{ fs_greatest2("prev.p_max_event_timestamp_is_android", "n.p_max_event_timestamp_is_android") }} as p_max_event_timestamp_is_android,
    {{ fs_greatest2("prev.p_max_event_timestamp_is_others", "n.p_max_event_timestamp_is_others") }} as p_max_event_timestamp_is_others,
    {{ fs_greatest2("prev.p_max_event_timestamp_is_login_success", "n.p_max_event_timestamp_is_login_success") }} as p_max_event_timestamp_is_login_success,
    {{ fs_greatest2("prev.p_max_event_timestamp_is_login_success_is_ios", "n.p_max_event_timestamp_is_login_success_is_ios") }} as p_max_event_timestamp_is_login_success_is_ios,
    {{ fs_greatest2("prev.p_max_event_timestamp_is_login_success_is_android", "n.p_max_event_timestamp_is_login_success_is_android") }} as p_max_event_timestamp_is_login_success_is_android,
    {{ fs_greatest2("prev.p_max_event_timestamp_is_login_success_is_others", "n.p_max_event_timestamp_is_login_success_is_others") }} as p_max_event_timestamp_is_login_success_is_others,
    {{ fs_greatest2("prev.p_max_event_timestamp_is_login_failed", "n.p_max_event_timestamp_is_login_failed") }} as p_max_event_timestamp_is_login_failed,
    {{ fs_greatest2("prev.p_max_event_timestamp_is_login_failed_is_ios", "n.p_max_event_timestamp_is_login_failed_is_ios") }} as p_max_event_timestamp_is_login_failed_is_ios,
    {{ fs_greatest2("prev.p_max_event_timestamp_is_login_failed_is_android", "n.p_max_event_timestamp_is_login_failed_is_android") }} as p_max_event_timestamp_is_login_failed_is_android,
    {{ fs_greatest2("prev.p_max_event_timestamp_is_login_failed_is_others", "n.p_max_event_timestamp_is_login_failed_is_others") }} as p_max_event_timestamp_is_login_failed_is_others,

    (coalesce(prev._n_rows, 0) + n._n_rows) as _n_rows,
    {{ fs_least2('prev._min_event_date', 'n._min_event_date') }} as _min_event_date,
    {{ fs_greatest2('prev._max_event_date', 'n._max_event_date') }} as _max_event_date,
    s.through as _state_as_of_date,
    {{ fs_knowledge_cutoff('Asia/Jakarta') }} as _folded_through,
    'dfceaf462f00' as _state_version
from new_days as n
cross join seal as s
-- A restated entity is re-folded from scratch, so it takes nothing from
-- its previous state: merging with no row is each monoid's identity.
left join prev on n.safe_id = prev.safe_id and n._restated = 0
