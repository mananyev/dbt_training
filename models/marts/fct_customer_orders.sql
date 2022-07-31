with 

orders as (

  select * from {{ ref('int_orders') }}

),


customers as (

  select * from {{ ref('stg_jaffle_shop__customers') }}

),


aggregations as (
    select
    customer_id,

    --- Customer level aggregations
    min(order_date) as customer_first_order_date,

    min(valid_order_date) as customer_first_non_returned_order_date,

    max(valid_order_date) as customer_most_recent_non_returned_order_date,

    count(*) as customer_order_count,

    sum(nvl2(
        valid_order_date,
        1,
        0)
    ) as customer_non_returned_order_count,

    sum(nvl2(
        valid_order_date,
        order_value_dollars,
        0)
    ) as customer_total_lifetime_value,

    listagg(distinct order_id) as customer_order_ids

    from orders

    group by 1
),


customer_orders as (

  select 

    orders.*,
    customers.full_name,
    customers.surname,
    customers.givenname,
    a.customer_first_order_date,
    a.customer_first_non_returned_order_date,
    a.customer_most_recent_non_returned_order_date,
    a.customer_order_count,
    a.customer_non_returned_order_count,
    a.customer_total_lifetime_value,
    a.customer_order_ids

  from orders

  inner join customers using (customer_id)

  left join aggregations a using (customer_id)

),


add_avg_order_values as (

  select
    *,
    customer_total_lifetime_value / customer_non_returned_order_count 
        as customer_avg_non_returned_order_value

  from customer_orders

),


final as (

  select 

    order_id,
    customer_id,
    surname,
    givenname,
    customer_first_order_date as first_order_date,
    customer_order_count as order_count,
    customer_total_lifetime_value as total_lifetime_value,
    order_value_dollars,
    order_status,
    payment_status

  from add_avg_order_values

)

select * from final