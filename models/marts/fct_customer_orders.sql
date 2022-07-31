with

-- Import CTEs
orders as (

    select * from {{ source('jaffle_shop', 'orders') }}

),


customers as (

    select * from {{ source('jaffle_shop', 'customers') }}

),


payments as (

    select * from {{ source('stripe', 'payment') }}

),


-- Logical CTEs
p as (

    select
        orderid as order_id,
        max(created) as payment_finalized_date,
        sum(amount) / 100.0 as total_amount_paid

    from payments

    where status <> 'fail'

    group by 1
    
),


paid_orders as (

    select
        o.id as order_id,
        o.user_id	as customer_id,
        o.order_date as order_placed_at,
        o.status as order_status,
        p.total_amount_paid,
        p.payment_finalized_date,
        c.first_name as customer_first_name,
        c.last_name as customer_last_name

    from orders o

    left join p on o.id = p.order_id
    
    left join customers c on o.user_id = c.id

),


customer_orders as (

    select
        c.id as customer_id
        , min(order_date) as first_order_date
        , max(order_date) as most_recent_order_date
        , count(o.id) as number_of_orders
    
    from customers c 

    left join orders o
        on o.user_id = c.id 

    group by 1

),


x as (

    select
        po.order_id,
        sum(t2.total_amount_paid) as clv_bad
    
    from paid_orders po
    
    left join paid_orders t2
        on po.customer_id = t2.customer_id
        and po.order_id >= t2.order_id
    
    group by 1
    
    order by po.order_id

),


-- Final CTE
final as (

    select
        po.*,
        row_number() over (order by po.order_id) as transaction_seq,
        row_number() over (
            partition by customer_id order by po.order_id
        ) as customer_sales_seq,
        case
            when c.first_order_date = po.order_placed_at
            then 'new'
            else 'return'
        end as nvsr,
        x.clv_bad as customer_lifetime_value,
        c.first_order_date as fdos

    from paid_orders po

    left join customer_orders as c using (customer_id)

    left outer join x on x.order_id = po.order_id

    order by order_id
)

select * from final