-- ============================================================================
-- GENERATED FILE - DO NOT EDIT BY HAND
--
--   layer      : intermediate / all_time unsealed tail
--   feature    : fact_agg_features_login_device_v1
--   spec       : features/fact_agg_features_login_device_v1.yml
--   spec hash  : e1ebed8aec82
--   generator  : featurefold
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

{{ config(materialized='view', tags=['feature_store', 'fact_agg_features_login_device_v1']) }}

with watermark as (

    select coalesce(max(_state_as_of_date), cast('1900-01-01' as date)) as wm
    from {{ ref('int_fact_agg_features_login_device_v1__alltime_state') }}

)

select
    p.safe_id,
    -- event_id / count ----------------------------------------------------
    sum(p.p_count_event_id) as p_count_event_id,
    sum(p.p_count_event_id_is_login_success) as p_count_event_id_is_login_success,
    sum(p.p_count_event_id_is_login_failed) as p_count_event_id_is_login_failed,

    -- event_id / count_distinct -------------------------------------------
    {{ fs_kmv_union_agg("p.p_count_distinct_event_id", 64) }} as p_count_distinct_event_id,
    {{ fs_kmv_union_agg("p.p_count_distinct_event_id_is_login_success", 64) }} as p_count_distinct_event_id_is_login_success,
    {{ fs_kmv_union_agg("p.p_count_distinct_event_id_is_login_failed", 64) }} as p_count_distinct_event_id_is_login_failed,

    -- device_id / count_distinct ------------------------------------------
    {{ fs_array_union_agg("p.p_count_distinct_device_id") }} as p_count_distinct_device_id,
    {{ fs_array_union_agg("p.p_count_distinct_device_id_is_login_success") }} as p_count_distinct_device_id_is_login_success,
    {{ fs_array_union_agg("p.p_count_distinct_device_id_is_login_failed") }} as p_count_distinct_device_id_is_login_failed,

    -- login_source / count_distinct ---------------------------------------
    {{ fs_array_union_agg("p.p_count_distinct_login_source") }} as p_count_distinct_login_source,
    {{ fs_array_union_agg("p.p_count_distinct_login_source_is_login_success") }} as p_count_distinct_login_source_is_login_success,
    {{ fs_array_union_agg("p.p_count_distinct_login_source_is_login_failed") }} as p_count_distinct_login_source_is_login_failed,

    sum(p._n_rows) as _n_rows,
    min(case when p._n_rows > 0 then p.event_date end) as _min_event_date,
    max(case when p._n_rows > 0 then p.event_date end) as _max_event_date
from {{ ref('int_fact_agg_features_login_device_v1__daily_partials') }} as p
cross join watermark as w
where
    p.event_date > w.wm
    and p.event_date <= {{ fs_date_offset_lit(0) }}
group by p.safe_id
