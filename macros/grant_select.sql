{#
No `role` and no `grant select on all views` are available in Redshift.
#}

{% macro grant_select(schema=target.schema, role=target.user) %}
    {% set query %}
        grant usage on schema {{ schema }} to {{ role }};
        grant select on all tables in schema {{ schema }} to {{ role }};
    {% endset %}

    {{ log('Granting select on all tables and views in schema ' ~ schema ~ ' to user ' ~ user, info=True) }}
    {% do run_query(query) %}
    {{ log('Priveleges granted.', info=True) }}

{% endmacro %}