{#
shamelessly stolen from dbt-event-logging
#}

{% macro get_audit_metrics_relation() %}
    {%- set audit_table =
        api.Relation.create(
            identifier='dbt_audit_metrics',
            schema=target.schema~'_meta',
            type='table'
        ) -%}
    {{ return(audit_table) }}
{% endmacro %}


{% macro get_audit_metrics_schema() %}
    {% set audit_table = logging.get_audit_metrics_relation() %}
    {{ return(audit_table.include(schema=True, identifier=False)) }}
{% endmacro %}


{% macro log_audit_metrics_event(event_name, schema, relation) %}

    insert into {{ logging.get_audit_metrics_relation() }} (
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
        from  {{ref('{{ schema }}.{{ relation }}')}}
        )

{% endmacro %}


{% macro create_audit_metrics_schema() %}
    create schema if not exists {{ logging.get_audit_metrics_schema() }}
{% endmacro %}


{% macro create_audit_metrics_table() %}

    create table if not exists {{ logging.get_audit_metrics_relation() }}
    (
       event_name       varchar(512),
       event_schema     varchar(512),
       event_model      varchar(512),
       invocation_id    varchar(512),
       event_row_count  int
    )

{% endmacro %}

{% macro log_model_start_event() %}
    {% if is_incremental() %}
    {{logging.log_audit_metrics_event(
        'model initial rowcount', this.schema, this.name
        )}}
    {% endif %}
{% endmacro %}


{% macro log_model_end_event() %}
    {% if is_incremental() %}
    {{logging.log_audit_metrics_event(
        'model final rowcount', this.schema, this.name
        )}}
    {% endif %}
{% endmacro %}
