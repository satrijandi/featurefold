-- ============================================================================
-- GENERATED FILE - DO NOT EDIT BY HAND
--
--   layer      : intermediate / all_time unsealed tail
--   feature    : fact_agg_features_login_history_v2
--   spec       : features/fact_agg_features_login_history_v2.yml
--   spec hash  : a7a086250bfc
--   generator  : featuremart
--
-- Edit the spec and run `make generate`. CI fails when a generated file
-- differs from what the spec produces, so this file and the spec cannot
-- drift apart.
-- ============================================================================

-- The days the sealed accumulator has not absorbed yet. Merging this into
-- the sealed state gives an all_time value current to the as-of date while
-- still tolerating late-arriving events.
--
-- The range starts at the accumulator's ACTUAL watermark, not at
-- target_date - late_arrival_days. Those two coincide on an ordinary
-- forward run, but they diverge whenever a past as-of date is replayed: the
-- watermark is global and would then sit AHEAD of the nominal seal, so a
-- target-derived range would re-add days the accumulator already holds and
-- silently double-count every all_time feature. Anchoring to the watermark
-- makes sealed and unsealed complementary by construction, for any as-of
-- date and in any order.

{{ config(materialized='view', tags=['feature_store', 'fact_agg_features_login_history_v2']) }}

with watermark as (

    select coalesce(max(_state_as_of_date), cast('1900-01-01' as date)) as wm
    from {{ ref('int_fact_agg_features_login_history_v2__alltime_state') }}

)

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
    max(case when p._n_rows > 0 then p.event_date end) as _max_event_date
from {{ ref('int_fact_agg_features_login_history_v2__daily_partials') }} as p
cross join watermark as w
where
    p.event_date > w.wm
    and p.event_date <= {{ fs_date_offset_lit(0) }}
group by p.safe_id
