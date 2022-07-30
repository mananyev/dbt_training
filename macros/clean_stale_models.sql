{#  
    -- let's develop a macro that 
    1. queries the information schema of a database
    2. finds objects that are > 1 year old (no longer maintained)
    3. generates automated drop statements
    4. has the ability to execute those drop statements

#}

{% macro clean_stale_models(database=target.database, schema=target.schema, days=365, dry_run=True) %}

    {% set get_drop_commands_query %}
        with inserts as (
            select MAX(query) as query, tbl, MAX(i.endtime) as last_insert
            from stl_insert i
            group by tbl
            order by tbl
        ) 
        select
            'drop table {{ database }}.' || sti.schema || '.' || sti.table || ';'
        from inserts i
            left join stl_query using (query)
            left join svv_table_info sti on sti.table_id = i.tbl
        where schema = '{{ schema }}'
            and endtime <= current_date - interval '{{ days }} days'
        order by endtime desc;
    {% endset %}

    {{ log('\nGenerating cleanup queries...\n', info=True) }}
    {% set drop_queries = run_query(get_drop_commands_query).columns[0].values() %}

    {% for query in drop_queries %}
        {% if dry_run %}
            {{ log(query, info=True) }}
        {% else %}
            {{ log('Dropping object with command: ' ~ query, info=True) }}
            {% do run_query(query) %} 
        {% endif %}       
    {% endfor %}

{% endmacro %}