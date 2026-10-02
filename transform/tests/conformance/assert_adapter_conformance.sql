-- ============================================================================
-- ADAPTER CONFORMANCE SUITE
--
-- Every dialect primitive in macros/adapters/ has a written contract. This
-- asserts each one against known inputs on WHICHEVER adapter is configured, so
-- `dbt test --target databricks` proves the Databricks implementations agree
-- with the DuckDB ones rather than merely compiling.
--
-- Porting the feature store to a new warehouse means: implement the macros,
-- run this, ship. If this passes, every generated model is portable, because
-- generated models use nothing else.
--
-- Each violated contract names itself, so a failure identifies the primitive.
-- ============================================================================

with

-- 'a' twice (dedup) and a NULL (must be dropped by collect_set)
vals as (
    select
        1 as g,
        'a' as v
    union all
    select
        1 as g,
        'b' as v
    union all
    select
        1 as g,
        'a' as v
    union all
    select
        1 as g,
        cast(null as varchar) as v
),

other_vals as (
    select
        1 as g,
        'b' as v
    union all
    select
        1 as g,
        'c' as v
),

all_null as (
    select
        1 as g,
        cast(null as varchar) as v
),

set_a as (
    select
        g,
        {{ fs_collect_set('v') }} as s
    from vals
    group by g
),

set_b as (
    select
        g,
        {{ fs_collect_set('v') }} as s
    from other_vals
    group by g
),

set_nul as (
    select
        g,
        {{ fs_collect_set('v') }} as s
    from all_null
    group by g
),

-- An empty relation of the right shape, for a left join that matches nothing.
no_set as (
    select * from set_b
    where 1 = 0
),

-- A genuinely NULL array of the right element type, obtained the way the
-- pipeline obtains one: a left join that does not match.
with_null_array as (
    select
        a.g,
        a.s,
        missing.s as null_s
    from set_a as a
    left join no_set as missing on a.g = missing.g
),

-- Union of set_a and set_b across ROWS, exercising the aggregate form.
unioned as (
    select
        g,
        {{ fs_array_union_agg('s') }} as s
    from (
        select * from set_a
        union all
        select * from set_b
    ) as u
    group by g
),

-- KMV round trip: build, union across rows, merge two sketches, estimate.
-- Below k a sketch retains every hash, so all three must be exact.
sketch_a as (
    select
        g,
        {{ fs_kmv_build('v', 64) }} as k
    from vals
    group by g
),

sketch_b as (
    select
        g,
        {{ fs_kmv_build('v', 64) }} as k
    from other_vals
    group by g
),

sketch_union as (
    select
        g,
        {{ fs_kmv_union_agg('k', 64) }} as k
    from (
        select * from sketch_a
        union all
        select * from sketch_b
    ) as u
    group by g
),

no_sketch as (
    select * from sketch_b
    where 1 = 0
),

-- A NULL sketch of the correct element type (array<double>, not array<varchar>),
-- which is what the mart sees for an entity absent from the unsealed tail.
with_null_sketch as (
    select
        sa.g,
        missing.k as null_k
    from sketch_a as sa
    left join no_sketch as missing on sa.g = missing.g
),

-- Time zone conversion on known instants: an ordinary offset, both sides of
-- a daylight-saving jump, a round trip, and a same-zone call.
clocks as (
    select
        {{ fs_convert_tz("cast('2026-08-20 23:30:00' as timestamp)", 'UTC', 'Asia/Jakarta') }}
            as jakarta,
        {{ fs_convert_tz("cast('2026-03-08 06:30:00' as timestamp)", 'UTC', 'America/New_York') }}
            as ny_before_dst,
        {{ fs_convert_tz("cast('2026-03-08 07:30:00' as timestamp)", 'UTC', 'America/New_York') }}
            as ny_after_dst,
        {{ fs_convert_tz(fs_convert_tz("cast('2026-08-20 23:30:00' as timestamp)",
                                       'UTC', 'Asia/Jakarta'), 'Asia/Jakarta', 'UTC') }}
            as round_trip,
        {{ fs_convert_tz("cast('2026-08-20 23:30:00' as timestamp)", 'UTC', 'UTC') }}
            as same_zone,
        -- The end of an as-of day, resolved by the zone database at compile
        -- time: on an ordinary day, and on the day New York springs forward,
        -- whose local midnight that follows is EDT rather than EST.
        {{ fs_knowledge_cutoff('Asia/Jakarta', '2026-08-20') }} as cutoff_jakarta,
        {{ fs_knowledge_cutoff('America/New_York', '2026-03-08') }} as cutoff_ny_dst,
        {{ fs_knowledge_cutoff('UTC', '2026-08-20') }} as cutoff_utc
),

checked as (

    select
        coalesce(
            case
                when {{ fs_array_size('a.s') }} <> 2
                    then 'fs_collect_set:dedups_and_drops_nulls;'
            end, ''
        )
        || coalesce(
            case
                when {{ fs_array_size('n.s') }} <> 0
                    then 'fs_collect_set:all_null_group_is_empty;'
            end, ''
        )
        || coalesce(
            case
                when {{ fs_array_size('w.null_s') }} <> 0
                    then 'fs_array_size:null_is_zero;'
            end, ''
        )
        || coalesce(
            case
                when {{ fs_array_size('u.s') }} <> 3
                    then 'fs_array_union_agg:unions_across_rows;'
            end, ''
        )
        || coalesce(
            case
                when {{ fs_array_size(fs_array_union2('a.s', 'b.s')) }} <> 3
                    then 'fs_array_union2:dedups;'
            end, ''
        )
        || coalesce(
            case
                when {{ fs_array_size(fs_array_union2('a.s', 'w.null_s')) }} <> 2
                    then 'fs_array_union2:null_safe;'
            end, ''
        )
        || coalesce(
            case
                when {{ fs_array_elem(fs_array_sort('a.s'), 1) }} <> 'a'
                    then 'fs_array_sort:ascending;'
            end, ''
        )
        || coalesce(
            case
                when {{ fs_array_elem(fs_array_sort('a.s'), 2) }} <> 'b'
                    then 'fs_array_elem:is_one_based;'
            end, ''
        )
        || coalesce(
            case
                when {{ fs_array_elem(fs_array_sort('a.s'), 9) }} is not null
                    then 'fs_array_elem:out_of_range_is_null;'
            end, ''
        )
        || coalesce(
            case
                when {{ fs_array_size(fs_array_head(fs_array_sort('a.s'), 1)) }} <> 1
                    then 'fs_array_head:takes_n;'
            end, ''
        )
        || coalesce(
            case
                when {{ fs_array_size(fs_array_head(fs_array_sort('a.s'), 99)) }} <> 2
                    then 'fs_array_head:tolerates_short_arrays;'
            end, ''
        )

        || coalesce(
            case
                when {{ fs_hash_unit("'x'") }} < 0 or {{ fs_hash_unit("'x'") }} >= 1
                    then 'fs_hash_unit:in_unit_interval;'
            end, ''
        )
        || coalesce(
            case
                when {{ fs_hash_unit("'x'") }} <> {{ fs_hash_unit("'x'") }}
                    then 'fs_hash_unit:deterministic;'
            end, ''
        )
        || coalesce(
            case
                when {{ fs_hash_unit("'x'") }} = {{ fs_hash_unit("'y'") }}
                    then 'fs_hash_unit:distinguishes_inputs;'
            end, ''
        )

        || coalesce(
            case
                when {{ fs_datediff_day("cast('2026-01-01' as date)", "cast('2026-01-10' as date)") }} <> 9
                    then 'fs_datediff_day:counts_calendar_days;'
            end, ''
        )
        || coalesce(
            case
                when {{ fs_datediff_day("cast('2026-01-01 23:59:00' as timestamp)", "cast('2026-01-02 00:01:00' as timestamp)") }} <> 1
                    then 'fs_datediff_day:truncates_to_date;'
            end, ''
        )
        || coalesce(
            case
                when {{ fs_datediff_day("cast('2026-01-10' as date)", "cast('2026-01-01' as date)") }} <> -9
                    then 'fs_datediff_day:signed;'
            end, ''
        )

        || coalesce(
            case
                when {{ fs_least2('cast(null as bigint)', 'cast(5 as bigint)') }} <> 5
                    then 'fs_least2:null_is_absorbing_not_poisoning;'
            end, ''
        )
        || coalesce(
            case
                when {{ fs_least2('cast(3 as bigint)', 'cast(5 as bigint)') }} <> 3
                    then 'fs_least2:picks_smaller;'
            end, ''
        )
        || coalesce(
            case
                when {{ fs_greatest2('cast(null as bigint)', 'cast(5 as bigint)') }} <> 5
                    then 'fs_greatest2:null_is_absorbing;'
            end, ''
        )
        || coalesce(
            case
                when {{ fs_greatest2('cast(3 as bigint)', 'cast(5 as bigint)') }} <> 5
                    then 'fs_greatest2:picks_larger;'
            end, ''
        )

        || coalesce(
            case
                when {{ fs_kmv_estimate('sa.k', 64) }} <> 2
                    then 'fs_kmv_build:exact_below_k;'
            end, ''
        )
        || coalesce(
            case
                when {{ fs_kmv_estimate('su.k', 64) }} <> 3
                    then 'fs_kmv_union_agg:exact_below_k;'
            end, ''
        )
        || coalesce(
            case
                when {{ fs_kmv_estimate(fs_kmv_merge2('sa.k', 'sb.k', 64), 64) }} <> 3
                    then 'fs_kmv_merge2:exact_below_k;'
            end, ''
        )
        || coalesce(
            case
                when {{ fs_kmv_estimate('ns.null_k', 64) }} <> 0
                    then 'fs_kmv_estimate:null_sketch_is_zero;'
            end, ''
        )
        || coalesce(
            case
                when {{ fs_kmv_estimate(fs_kmv_merge2('sa.k', 'ns.null_k', 64), 64) }} <> 2
                    then 'fs_kmv_merge2:null_safe;'
            end, ''
        )
        || coalesce(
            case
                when c.jakarta <> cast('2026-08-21 06:30:00' as timestamp)
                    then 'fs_convert_tz:fixed_offset;'
            end, ''
        )
        || coalesce(
            case
                when c.ny_before_dst <> cast('2026-03-08 01:30:00' as timestamp)
                    then 'fs_convert_tz:before_dst;'
            end, ''
        )
        || coalesce(
            case
                when c.ny_after_dst <> cast('2026-03-08 03:30:00' as timestamp)
                    then 'fs_convert_tz:after_dst;'
            end, ''
        )
        || coalesce(
            case
                when c.round_trip <> cast('2026-08-20 23:30:00' as timestamp)
                    then 'fs_convert_tz:round_trip;'
            end, ''
        )
        || coalesce(
            case
                when c.same_zone <> cast('2026-08-20 23:30:00' as timestamp)
                    then 'fs_convert_tz:same_zone_is_identity;'
            end, ''
        )
        || coalesce(
            case
                when c.cutoff_jakarta <> cast('2026-08-20 17:00:00' as timestamp)
                    then 'fs_knowledge_cutoff:end_of_local_day;'
            end, ''
        )
        || coalesce(
            case
                when c.cutoff_ny_dst <> cast('2026-03-09 04:00:00' as timestamp)
                    then 'fs_knowledge_cutoff:dst_day;'
            end, ''
        )
        || coalesce(
            case
                when c.cutoff_utc <> cast('2026-08-21 00:00:00' as timestamp)
                    then 'fs_knowledge_cutoff:end_of_utc_day;'
            end, ''
        )
        || '' as violations

    from set_a as a
    inner join set_b as b on a.g = b.g
    inner join set_nul as n on a.g = n.g
    inner join with_null_array as w on a.g = w.g
    inner join unioned as u on a.g = u.g
    inner join sketch_a as sa on a.g = sa.g
    inner join sketch_b as sb on a.g = sb.g
    inner join sketch_union as su on a.g = su.g
    inner join with_null_sketch as ns on a.g = ns.g
    cross join clocks as c

)

select
    '{{ target.type }}' as adapter,
    violations
from checked
where violations <> ''
