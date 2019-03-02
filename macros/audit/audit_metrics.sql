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
{% macro log_metrics_event(event_name, schema, relation) %}

    insert into {{ dbt_dv_utils.get_metrics_relation() }} (
        event_name,
        event_timestamp,
        event_schema,
        event_model,
        invocation_id,
        event_row_count
        )

    select
        '{{ event_name }}',
        {% if variable != None %}'{{ schema }}'{% else %}null::varchar(512){% endif %},
        {% if variable != None %}'{{ relation }}'{% else %}null::varchar(512){% endif %},
        '{{ invocation_id }}',
        count(*)
        from  "{{ schema }}"."{{ relation }}"        

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
       event_row_count  int
    )

{% endmacro %}

{% macro log_metrics_start_event() %}
    {% if is_incremental() %}
    {{dbt_dv_utils.log_metrics_event(
        'model initial rowcount', this.schema, this.name
        )}}
    {% endif %}
{% endmacro %}


{% macro log_metrics_end_event() %}
    {% if is_incremental() %}
    {{dbt_dv_utils.log_metrics_event(
        'model final rowcount', this.schema, this.name
        )}}
    {% endif %}
{% endmacro %}
