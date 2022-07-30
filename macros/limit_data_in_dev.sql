{#
This is the code made to work in Redshift!
#}
{% macro limit_data_in_dev(column_name, dev_days_of_data) -%}
{%- if target.name == 'default' -%}
where {{ column_name }} >= current_date - interval '{{ dev_days_of_data }} days'
{%- endif -%}
{%- endmacro %}