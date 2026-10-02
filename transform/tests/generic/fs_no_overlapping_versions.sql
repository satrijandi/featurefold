{#-
  Asserts a versioned table never has two versions of one key current at once:
  ordered by when they became visible, each version is closed no later than its
  successor opens. An open version followed by another, or two overlapping
  intervals, means every as-of date inside the overlap reads that row twice.

  `key` is a list of columns; `start` and `end` bound each version's interval,
  with `end` NULL (or a far-future sentinel) while it is still current.
-#}
{% test fs_no_overlapping_versions(model, key, start, end) %}

{%- set key_cols = key | join(", ") -%}

with versions as (

    select
        {{ key_cols }},
        {{ start }} as version_start,
        {{ end }} as version_end,
        lead({{ start }}) over (partition by {{ key_cols }} order by {{ start }}) as next_start
    from {{ model }}

)

select *
from versions
where
    next_start is not null
    and (version_end is null or version_end > next_start)

{% endtest %}
