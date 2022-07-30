{%- set methods = ['bank_transfer', 'coupon', 'credit_card', 'gift_card', ] -%}

with

payments as (

    select * from {{ ref('stg_payments') }}

),

pivoted as (

    select
        order_id,
        {%- for method in methods %}
        sum(case when payment_method = '{{ method }}' then amount else 0 end)
            as {{ method }}_amount
        {%- if not loop.last -%}
        ,
        {%- endif -%}
        {%- endfor %}
    
    from payments

    where payment_status = 'success'

    group by order_id

)

select * from pivoted