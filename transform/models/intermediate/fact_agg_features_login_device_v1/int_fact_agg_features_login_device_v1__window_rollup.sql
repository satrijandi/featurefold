-- ============================================================================
-- GENERATED FILE - DO NOT EDIT BY HAND
--
--   layer      : intermediate / bounded window roll-up
--   feature    : fact_agg_features_login_device_v1
--   spec       : features/fact_agg_features_login_device_v1.yml
--   spec hash  : e1ebed8aec82
--   generator  : featurefold
--
-- Edit the spec and run `make generate`. CI fails when a generated file
-- differs from what the spec produces, so this file and the spec cannot
-- drift apart.
-- ============================================================================

-- Folds at most 30 rows of stored partial state per entity into the
-- bounded windows. Nothing is recomputed from the raw source here.
--
-- An entity whose rows in the widest window are all tombstones has no
-- activity there, so it is left out exactly as if it had never had any.

{{ config(materialized='view', tags=['feature_store', 'fact_agg_features_login_device_v1']) }}

with bounds as (

    select
        {{ fs_date_offset_lit(0) }} as d_hi,
        {{ fs_date_offset_lit(6) }} as lo_l7d,
        {{ fs_date_offset_lit(29) }} as lo_l30d

),

partials as (

    select
        p.*,
        b.*
    from {{ ref('int_fact_agg_features_login_device_v1__daily_partials') }} as p
    cross join bounds as b
    where
        p.event_date >= b.lo_l30d
        and p.event_date <= b.d_hi

)

select
    safe_id,
    -- event_id / count ----------------------------------------------------
    coalesce(sum(case when event_date >= lo_l7d then p_count_event_id end), 0) as count_event_id_l7d,
    coalesce(sum(p_count_event_id), 0) as count_event_id_l30d,
    coalesce(sum(case when event_date >= lo_l7d then p_count_event_id_is_login_success end), 0) as count_event_id_is_login_success_l7d,
    coalesce(sum(p_count_event_id_is_login_success), 0) as count_event_id_is_login_success_l30d,
    coalesce(sum(case when event_date >= lo_l7d then p_count_event_id_is_login_failed end), 0) as count_event_id_is_login_failed_l7d,
    coalesce(sum(p_count_event_id_is_login_failed), 0) as count_event_id_is_login_failed_l30d,

    -- event_id / count_distinct -------------------------------------------
    {{ fs_kmv_estimate(fs_kmv_union_agg("case when event_date >= lo_l7d then p_count_distinct_event_id end", 64), 64) }} as count_distinct_event_id_l7d,
    {{ fs_kmv_estimate(fs_kmv_union_agg("p_count_distinct_event_id", 64), 64) }} as count_distinct_event_id_l30d,
    {{ fs_kmv_estimate(fs_kmv_union_agg("case when event_date >= lo_l7d then p_count_distinct_event_id_is_login_success end", 64), 64) }} as count_distinct_event_id_is_login_success_l7d,
    {{ fs_kmv_estimate(fs_kmv_union_agg("p_count_distinct_event_id_is_login_success", 64), 64) }} as count_distinct_event_id_is_login_success_l30d,
    {{ fs_kmv_estimate(fs_kmv_union_agg("case when event_date >= lo_l7d then p_count_distinct_event_id_is_login_failed end", 64), 64) }} as count_distinct_event_id_is_login_failed_l7d,
    {{ fs_kmv_estimate(fs_kmv_union_agg("p_count_distinct_event_id_is_login_failed", 64), 64) }} as count_distinct_event_id_is_login_failed_l30d,

    -- device_id / count_distinct ------------------------------------------
    {{ fs_array_size(fs_array_union_agg("case when event_date >= lo_l7d then p_count_distinct_device_id end")) }} as count_distinct_device_id_l7d,
    {{ fs_array_size(fs_array_union_agg("p_count_distinct_device_id")) }} as count_distinct_device_id_l30d,
    {{ fs_array_size(fs_array_union_agg("case when event_date >= lo_l7d then p_count_distinct_device_id_is_login_success end")) }} as count_distinct_device_id_is_login_success_l7d,
    {{ fs_array_size(fs_array_union_agg("p_count_distinct_device_id_is_login_success")) }} as count_distinct_device_id_is_login_success_l30d,
    {{ fs_array_size(fs_array_union_agg("case when event_date >= lo_l7d then p_count_distinct_device_id_is_login_failed end")) }} as count_distinct_device_id_is_login_failed_l7d,
    {{ fs_array_size(fs_array_union_agg("p_count_distinct_device_id_is_login_failed")) }} as count_distinct_device_id_is_login_failed_l30d,

    -- login_source / count_distinct ---------------------------------------
    {{ fs_array_size(fs_array_union_agg("case when event_date >= lo_l7d then p_count_distinct_login_source end")) }} as count_distinct_login_source_l7d,
    {{ fs_array_size(fs_array_union_agg("p_count_distinct_login_source")) }} as count_distinct_login_source_l30d,
    {{ fs_array_size(fs_array_union_agg("case when event_date >= lo_l7d then p_count_distinct_login_source_is_login_success end")) }} as count_distinct_login_source_is_login_success_l7d,
    {{ fs_array_size(fs_array_union_agg("p_count_distinct_login_source_is_login_success")) }} as count_distinct_login_source_is_login_success_l30d,
    {{ fs_array_size(fs_array_union_agg("case when event_date >= lo_l7d then p_count_distinct_login_source_is_login_failed end")) }} as count_distinct_login_source_is_login_failed_l7d,
    {{ fs_array_size(fs_array_union_agg("p_count_distinct_login_source_is_login_failed")) }} as count_distinct_login_source_is_login_failed_l30d,

    max(case when _n_rows > 0 then event_date end) as _rollup_last_event_date
from partials
group by safe_id
having sum(_n_rows) > 0
