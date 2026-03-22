{% test same_distinct_grain_count_as_upstream(model, compare_model, upstream_columns) %}

with down_stream as (

    select count(*) as down_stream_row_count
    from {{ model }}

),

upstream as (

    select
        count(*) as upstream_distinct_grain_count
    from (
        select distinct
            {% for col in upstream_columns %}
                {{ col }}{% if not loop.last %}, {% endif %}
            {% endfor %}
        from {{ compare_model }}
    ) d

)

select *
from down_stream
cross join upstream
where down_stream_row_count != upstream_distinct_grain_count

{% endtest %}