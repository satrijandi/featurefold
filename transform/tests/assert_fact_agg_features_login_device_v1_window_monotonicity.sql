-- ============================================================================
-- GENERATED FILE - DO NOT EDIT BY HAND
--
--   layer      : test / nested windows stay ordered
--   feature    : fact_agg_features_login_device_v1
--   spec       : features/fact_agg_features_login_device_v1.yml
--   spec hash  : e1ebed8aec82
--   generator  : featurefold
--
-- Edit the spec and run `make generate`. CI fails when a generated file
-- differs from what the spec produces, so this file and the spec cannot
-- drift apart.
-- ============================================================================

{{ config(group='data_platform') }}

-- 24 invariant(s), evaluated in a SINGLE scan of the as-of partition.
-- Each violated invariant names itself in the `violations` column, so a failure
-- points at the exact feature rather than at the table.

with checked as (

    select
        target_date,
        safe_id,
        coalesce(case when count_event_id_l7d > count_event_id_l30d then 'count_event_id_l7d!>l30d;' end, '')
        || coalesce(case when count_event_id_l30d > count_event_id_all_time then 'count_event_id_l30d!>all_time;' end, '')
        || coalesce(case when count_event_id_is_login_success_l7d > count_event_id_is_login_success_l30d then 'count_event_id_is_login_success_l7d!>l30d;' end, '')
        || coalesce(case when count_event_id_is_login_success_l30d > count_event_id_is_login_success_all_time then 'count_event_id_is_login_success_l30d!>all_time;' end, '')
        || coalesce(case when count_event_id_is_login_failed_l7d > count_event_id_is_login_failed_l30d then 'count_event_id_is_login_failed_l7d!>l30d;' end, '')
        || coalesce(case when count_event_id_is_login_failed_l30d > count_event_id_is_login_failed_all_time then 'count_event_id_is_login_failed_l30d!>all_time;' end, '')
        || coalesce(case when count_distinct_event_id_l7d > count_distinct_event_id_l30d then 'count_distinct_event_id_l7d!>l30d;' end, '')
        || coalesce(case when count_distinct_event_id_l30d > count_distinct_event_id_all_time then 'count_distinct_event_id_l30d!>all_time;' end, '')
        || coalesce(case when count_distinct_event_id_is_login_success_l7d > count_distinct_event_id_is_login_success_l30d then 'count_distinct_event_id_is_login_success_l7d!>l30d;' end, '')
        || coalesce(case when count_distinct_event_id_is_login_success_l30d > count_distinct_event_id_is_login_success_all_time then 'count_distinct_event_id_is_login_success_l30d!>all_time;' end, '')
        || coalesce(case when count_distinct_event_id_is_login_failed_l7d > count_distinct_event_id_is_login_failed_l30d then 'count_distinct_event_id_is_login_failed_l7d!>l30d;' end, '')
        || coalesce(case when count_distinct_event_id_is_login_failed_l30d > count_distinct_event_id_is_login_failed_all_time then 'count_distinct_event_id_is_login_failed_l30d!>all_time;' end, '')
        || coalesce(case when count_distinct_device_id_l7d > count_distinct_device_id_l30d then 'count_distinct_device_id_l7d!>l30d;' end, '')
        || coalesce(case when count_distinct_device_id_l30d > count_distinct_device_id_all_time then 'count_distinct_device_id_l30d!>all_time;' end, '')
        || coalesce(case when count_distinct_device_id_is_login_success_l7d > count_distinct_device_id_is_login_success_l30d then 'count_distinct_device_id_is_login_success_l7d!>l30d;' end, '')
        || coalesce(case when count_distinct_device_id_is_login_success_l30d > count_distinct_device_id_is_login_success_all_time then 'count_distinct_device_id_is_login_success_l30d!>all_time;' end, '')
        || coalesce(case when count_distinct_device_id_is_login_failed_l7d > count_distinct_device_id_is_login_failed_l30d then 'count_distinct_device_id_is_login_failed_l7d!>l30d;' end, '')
        || coalesce(case when count_distinct_device_id_is_login_failed_l30d > count_distinct_device_id_is_login_failed_all_time then 'count_distinct_device_id_is_login_failed_l30d!>all_time;' end, '')
        || coalesce(case when count_distinct_login_source_l7d > count_distinct_login_source_l30d then 'count_distinct_login_source_l7d!>l30d;' end, '')
        || coalesce(case when count_distinct_login_source_l30d > count_distinct_login_source_all_time then 'count_distinct_login_source_l30d!>all_time;' end, '')
        || coalesce(case when count_distinct_login_source_is_login_success_l7d > count_distinct_login_source_is_login_success_l30d then 'count_distinct_login_source_is_login_success_l7d!>l30d;' end, '')
        || coalesce(case when count_distinct_login_source_is_login_success_l30d > count_distinct_login_source_is_login_success_all_time then 'count_distinct_login_source_is_login_success_l30d!>all_time;' end, '')
        || coalesce(case when count_distinct_login_source_is_login_failed_l7d > count_distinct_login_source_is_login_failed_l30d then 'count_distinct_login_source_is_login_failed_l7d!>l30d;' end, '')
        || coalesce(case when count_distinct_login_source_is_login_failed_l30d > count_distinct_login_source_is_login_failed_all_time then 'count_distinct_login_source_is_login_failed_l30d!>all_time;' end, '')
        || '' as violations
    from {{ ref('fact_agg_features_login_device_v1') }}
    where target_date = {{ fs_target_date() }}

)

select
    target_date,
    safe_id,
    violations
from checked
where violations <> ''
