{#
shamelessly stolen from dbt-event-logging
#}
{% macro get_metrics_relation() %}
    {%- set metrics_table =
        api.Relation.create(
            identifier='dbt_metrics',
            schema=target.schema~'_meta',
            type='table'
        ) -%}
    {{ return(metrics_table) }}
{% endmacro %}

{% macro get_metrics_schema() %}
    {% set metrics_table = dbt_dv_utils.get_metrics_relation() %}
    {{ return(metrics_table.include(schema=True, identifier=False)) }}
{% endmacro %}

{#
for MS SQL, we could:
  select sum([rows])
  from sys.partitions where index_id = min(index_id) to be much(!) more efficient
#}
{% macro log_metrics_event() %}

        insert into {{ dbt_dv_utils.get_metrics_relation() }} (
            event_name,
            event_schema,
            event_model,
            invocation_id,
            initial_row_count,
            run_row_count,
            final_row_count
            )
        select
            '{{ event_name }}',
            this.schema,
            this.name,
            '{{ invocation_id }}',
            sum(case when invocation_id = '{{ invocation_id }}' then 0 else 1 end),
            sum(case when invocation_id = '{{ invocation_id }}' then 1 else 0 end),
            count(*)
            from  "{{ this.schema }}"."{{ this.relation }}"

{% endmacro %}

{% macro create_metrics_schema() %}
    create schema if not exists {{ dbt_dv_utils.get_metrics_schema() }}
{% endmacro %}


{% macro create_metrics_table() %}

    create table if not exists {{ dbt_dv_utils.get_metrics_relation() }}
    (
       event_name       varchar(512),
       event_schema     varchar(512),
       event_model      varchar(512),
       invocation_id    varchar(512),
       initial_row_count int,
       run_row_count  int,
       final_row_count int
    )

{% endmacro %}
