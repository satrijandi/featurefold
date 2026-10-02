{#-
  ============================================================================
  Array primitives -- the entire cross-dialect surface of the feature store.

  Everything else (exact count_distinct, KMV approximate count_distinct,
  all_time state merges) is composed from these, so porting to a new warehouse
  means implementing this file and nothing else.

  CONTRACT -- every adapter implementation must satisfy:
    fs_collect_set(expr)      AGG    distinct NON-NULL values of expr -> array
    fs_array_union_agg(col)   AGG    union of array-valued col across rows,
                                     null rows ignored -> distinct array
    fs_array_union2(a, b)     SCALAR null-safe union of two arrays -> distinct
    fs_array_size(a)          SCALAR element count; NULL -> 0
    fs_array_sort(a)          SCALAR ascending sort
    fs_array_head(a, n)       SCALAR first n elements (fewer if shorter)
    fs_array_elem(a, i)       SCALAR 1-BASED i-th element; out of range -> NULL

  Conformance is enforced by transform/tests/conformance/. Do not add an
  adapter without making that suite pass.
  ============================================================================
-#}

{# -------------------------------------------------------------- collect_set #}
{% macro fs_collect_set(expr) -%}
  {{ return(adapter.dispatch('fs_collect_set', 'featurefold')(expr)) }}
{%- endmacro %}

{% macro default__fs_collect_set(expr) -%}
list_distinct(list({{ expr }}))
{%- endmacro %}

{% macro databricks__fs_collect_set(expr) -%}
collect_set({{ expr }})
{%- endmacro %}

{% macro snowflake__fs_collect_set(expr) -%}
array_compact(array_agg(distinct {{ expr }}))
{%- endmacro %}


{# ---------------------------------------------------------- array_union_agg #}
{% macro fs_array_union_agg(col) -%}
  {{ return(adapter.dispatch('fs_array_union_agg', 'featurefold')(col)) }}
{%- endmacro %}

{% macro default__fs_array_union_agg(col) -%}
list_distinct(flatten(list_filter(list({{ col }}), x -> x is not null)))
{%- endmacro %}

{% macro databricks__fs_array_union_agg(col) -%}
array_distinct(flatten(collect_list({{ col }})))
{%- endmacro %}

{% macro snowflake__fs_array_union_agg(col) -%}
array_distinct(array_flatten(array_compact(array_agg({{ col }}))))
{%- endmacro %}


{# ------------------------------------------------------------- array_union2 #}
{% macro fs_array_union2(a, b) -%}
  {{ return(adapter.dispatch('fs_array_union2', 'featurefold')(a, b)) }}
{%- endmacro %}

{% macro default__fs_array_union2(a, b) -%}
list_distinct(flatten(list_filter([{{ a }}, {{ b }}], x -> x is not null)))
{%- endmacro %}

{% macro databricks__fs_array_union2(a, b) -%}
array_distinct(flatten(array_compact(array({{ a }}, {{ b }}))))
{%- endmacro %}

{% macro snowflake__fs_array_union2(a, b) -%}
array_distinct(array_flatten(array_construct_compact({{ a }}, {{ b }})))
{%- endmacro %}


{# --------------------------------------------------------------- array_size #}
{#- NULL must map to 0, not to NULL and not to Spark legacy -1. -#}
{% macro fs_array_size(a) -%}
  {{ return(adapter.dispatch('fs_array_size', 'featurefold')(a)) }}
{%- endmacro %}

{% macro default__fs_array_size(a) -%}
(case when {{ a }} is null then 0 else len({{ a }}) end)
{%- endmacro %}

{% macro databricks__fs_array_size(a) -%}
(case when {{ a }} is null then 0 else size({{ a }}) end)
{%- endmacro %}

{% macro snowflake__fs_array_size(a) -%}
(case when {{ a }} is null then 0 else array_size({{ a }}) end)
{%- endmacro %}


{# --------------------------------------------------------------- array_sort #}
{% macro fs_array_sort(a) -%}
  {{ return(adapter.dispatch('fs_array_sort', 'featurefold')(a)) }}
{%- endmacro %}

{% macro default__fs_array_sort(a) -%}
list_sort({{ a }})
{%- endmacro %}

{% macro databricks__fs_array_sort(a) -%}
array_sort({{ a }})
{%- endmacro %}

{% macro snowflake__fs_array_sort(a) -%}
array_sort({{ a }})
{%- endmacro %}


{# --------------------------------------------------------------- array_head #}
{% macro fs_array_head(a, n) -%}
  {{ return(adapter.dispatch('fs_array_head', 'featurefold')(a, n)) }}
{%- endmacro %}

{% macro default__fs_array_head(a, n) -%}
list_slice({{ a }}, 1, {{ n }})
{%- endmacro %}

{% macro databricks__fs_array_head(a, n) -%}
slice({{ a }}, 1, {{ n }})
{%- endmacro %}

{% macro snowflake__fs_array_head(a, n) -%}
array_slice({{ a }}, 0, {{ n }})
{%- endmacro %}


{# --------------------------------------------------------------- array_elem #}
{#- 1-BASED. Snowflake is natively 0-based, so it subtracts one. -#}
{% macro fs_array_elem(a, i) -%}
  {{ return(adapter.dispatch('fs_array_elem', 'featurefold')(a, i)) }}
{%- endmacro %}

{% macro default__fs_array_elem(a, i) -%}
({{ a }})[{{ i }}]
{%- endmacro %}

{% macro databricks__fs_array_elem(a, i) -%}
element_at({{ a }}, {{ i }})
{%- endmacro %}

{% macro snowflake__fs_array_elem(a, i) -%}
get({{ a }}, {{ i }} - 1)::double
{%- endmacro %}
