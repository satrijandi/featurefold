{#-
  Asserts every row's half-open interval [start, end) is non-empty: when `end`
  is set, it falls strictly after `start`. On a knowledge-time pair this is the
  difference between a row version that was current for a while and one that
  was superseded before (or as) it became visible, which no as-of date can
  ever see and which usually means the two columns are swapped or mis-zoned.
-#}
{% test fs_ordered_interval(model, start, end) %}

select {{ start }}, {{ end }}
from {{ model }}
where {{ end }} is not null
  and {{ end }} <= {{ start }}

{% endtest %}
