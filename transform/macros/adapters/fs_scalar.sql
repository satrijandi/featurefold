{#-
  ============================================================================
  Scalar primitives.

  fs_hash_unit(expr)        SCALAR  stable hash of expr into a DOUBLE in [0,1)
  fs_datediff_day(s, e)     SCALAR  whole CALENDAR days from s to e
                                    (both truncated to DATE first, so the
                                    result is exactly e_date - s_date)
  fs_convert_tz(e, from, to) SCALAR the naive wall-clock TIMESTAMP e, read in
                                    IANA zone `from`, as wall-clock in `to`.
                                    Daylight saving is the zone database's
                                    business, never a fixed offset.

  fs_least2 / fs_greatest2 are pure ANSI and deliberately NOT dispatched:
  LEAST/GREATEST disagree on NULL handling across engines, and the all_time
  state merge depends on "NULL means no data yet", not "NULL poisons".
  ============================================================================
-#}

{#- Largest prime below 2^53, so the quotient stays exactly representable
    as a DOUBLE on every engine. -#}
{% macro fs_hash_mod() %}9007199254740881{% endmacro %}

{% macro fs_hash_unit(expr) -%}
  {{ return(adapter.dispatch('fs_hash_unit', 'feature_mart')(expr)) }}
{%- endmacro %}

{% macro default__fs_hash_unit(expr) -%}
((hash(cast({{ expr }} as varchar)) % {{ fs_hash_mod() }}) / {{ fs_hash_mod() }}.0)
{%- endmacro %}

{% macro databricks__fs_hash_unit(expr) -%}
(pmod(xxhash64(cast({{ expr }} as string)), {{ fs_hash_mod() }}) / {{ fs_hash_mod() }}.0)
{%- endmacro %}

{% macro snowflake__fs_hash_unit(expr) -%}
(mod(abs(hash(cast({{ expr }} as varchar))), {{ fs_hash_mod() }}) / {{ fs_hash_mod() }}.0)
{%- endmacro %}


{# ------------------------------------------------------------ datediff_day #}
{% macro fs_datediff_day(start_expr, end_expr) -%}
  {{ return(adapter.dispatch('fs_datediff_day', 'feature_mart')(start_expr, end_expr)) }}
{%- endmacro %}

{% macro default__fs_datediff_day(start_expr, end_expr) -%}
date_diff('day', cast({{ start_expr }} as date), cast({{ end_expr }} as date))
{%- endmacro %}

{#- Databricks 2-arg form is datediff(end, start) and is available on every
    runtime, unlike the 3-arg unit form. -#}
{% macro databricks__fs_datediff_day(start_expr, end_expr) -%}
datediff(cast({{ end_expr }} as date), cast({{ start_expr }} as date))
{%- endmacro %}

{% macro snowflake__fs_datediff_day(start_expr, end_expr) -%}
datediff(day, cast({{ start_expr }} as date), cast({{ end_expr }} as date))
{%- endmacro %}


{# -------------------------------------------------------------- convert_tz #}
{#- Every timestamp a feature reads passes through here, so the date an event
    falls on and the hour a condition sees are fixed by the spec rather than by
    the session time zone of whichever warehouse runs it. Same-zone calls skip
    the conversion but still pin the type. -#}
{% macro fs_convert_tz(expr, from_tz, to_tz) -%}
  {%- if from_tz == to_tz -%}
    cast({{ expr }} as timestamp)
  {%- else -%}
    {{ return(adapter.dispatch('fs_convert_tz', 'feature_mart')(expr, from_tz, to_tz)) }}
  {%- endif -%}
{%- endmacro %}

{#- timezone(zone, TIMESTAMP) reads wall-clock in `zone` and yields an instant;
    timezone(zone, TIMESTAMPTZ) renders an instant as wall-clock in `zone`. -#}
{% macro default__fs_convert_tz(expr, from_tz, to_tz) -%}
timezone('{{ to_tz }}', timezone('{{ from_tz }}', cast({{ expr }} as timestamp)))
{%- endmacro %}

{#- The three-argument form is defined on TIMESTAMP_NTZ, so the input is pinned
    to it first; otherwise a TIMESTAMP_LTZ column would be read through the
    session zone before the conversion even starts. -#}
{% macro databricks__fs_convert_tz(expr, from_tz, to_tz) -%}
cast(convert_timezone('{{ from_tz }}', '{{ to_tz }}', cast({{ expr }} as timestamp_ntz)) as timestamp)
{%- endmacro %}

{% macro snowflake__fs_convert_tz(expr, from_tz, to_tz) -%}
convert_timezone('{{ from_tz }}', '{{ to_tz }}', cast({{ expr }} as timestamp_ntz))
{%- endmacro %}


{# ------------------------------------------------- null-safe min/max merges #}
{% macro fs_least2(a, b) -%}
(case
    when {{ a }} is null then {{ b }}
    when {{ b }} is null then {{ a }}
    when {{ a }} <= {{ b }} then {{ a }}
    else {{ b }}
 end)
{%- endmacro %}

{% macro fs_greatest2(a, b) -%}
(case
    when {{ a }} is null then {{ b }}
    when {{ b }} is null then {{ a }}
    when {{ a }} >= {{ b }} then {{ a }}
    else {{ b }}
 end)
{%- endmacro %}


{# ------------------------------------------------------------- target_date #}
{#- Single choke point for reading the run's as-of date. Fails loudly rather
    than silently defaulting, because a wrong target_date silently produces
    plausible-looking but time-leaking features. -#}
{% macro fs_target_date() -%}
cast('{{ fs_target_date_obj().isoformat() }}' as date)
{%- endmacro %}


{# ------------------------------------------------- as-of date arithmetic #}
{#- Window bounds are resolved to literal dates at COMPILE time rather than
    computed in SQL. Two reasons: the generated models stay free of dialect
    date-arithmetic, and Databricks/Snowflake can prune partitions against a
    literal where they cannot against an expression. -#}

{#- Strict only when SQL is compiled to run. While dbt merely parses the
    project -- `dbt parse`, `dbt ls`, an IDE, a package that installs this one --
    nothing executes, and demanding a date there would make the project
    unusable to every tool that never runs it. -#}
{% macro fs_target_date_obj() %}
  {%- set td = var('target_date', '') -%}
  {%- if not execute and (td is none or td | string | trim == '') -%}
    {{ return(modules.datetime.date(1970, 1, 1)) }}
  {%- endif -%}
  {%- if td is none or td | string | trim == '' -%}
    {{ exceptions.raise_compiler_error(
         "var 'target_date' is required. Run with: dbt run --vars '{target_date: YYYY-MM-DD}'") }}
  {%- endif -%}
  {%- if not modules.re.match('^\\d{4}-\\d{2}-\\d{2}$', td | string | trim) -%}
    {{ exceptions.raise_compiler_error(
         "var 'target_date' must be an ISO date (YYYY-MM-DD), got '" ~ td ~ "'") }}
  {%- endif -%}
  {{ return(modules.datetime.date.fromisoformat(td | string | trim)) }}
{% endmacro %}

{#- Literal DATE for `days_back` days before the as-of date. -#}
{% macro fs_date_offset_lit(days_back) -%}
  {%- set d = fs_target_date_obj() - modules.datetime.timedelta(days=days_back | int) -%}
cast('{{ d.isoformat() }}' as date)
{%- endmacro %}


{#- Where a run's knowledge ends, as a literal UTC TIMESTAMP: the instant the
    as-of date ends on the business clock `tz`, or now if that is still in the
    future. A row version is knowable for the run exactly when it was loaded
    before this instant and not superseded before it, and the next run looks
    for changes from here on.

    The "or now" matters whenever a run starts before its day is over on the
    business clock -- a schedule in UTC serving a zone behind it, a manual
    run. Recording the end of the day instead would claim knowledge of rows
    not yet loaded, and the next run would skip them as already seen.

    Resolved at compile time, for the same reasons as the window bounds above:
    no dialect date arithmetic in the models, and a literal the warehouse can
    prune on. The columns it is compared with are UTC, normalised by the
    staging models. `as_of` overrides the run's target_date, for tests. -#}
{% macro fs_knowledge_cutoff(tz, as_of=none) -%}
  {%- set as_of = modules.datetime.date.fromisoformat(as_of) if as_of else fs_target_date_obj() -%}
  {%- set d = as_of + modules.datetime.timedelta(days=1) -%}
  {%- set local = modules.pytz.timezone(tz).localize(
        modules.datetime.datetime(d.year, d.month, d.day)) -%}
  {%- set day_end = local.astimezone(modules.pytz.utc) -%}
  {%- set now = modules.datetime.datetime.now(modules.pytz.utc).replace(microsecond=0) -%}
  {%- set cutoff = now if now < day_end else day_end -%}
cast('{{ cutoff.strftime("%Y-%m-%d %H:%M:%S") }}' as timestamp)
{%- endmacro %}

