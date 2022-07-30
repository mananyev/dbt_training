with

unique_ids as (

    select
        {{ dbt_utils.surrogate_key(['customer_id', 'order_date']) }} as id,
        customer_id,
        order_date,
        count(*)

    from {{ ref('stg_orders') }}

    group by 1, 2, 3

)

select * from unique_ids