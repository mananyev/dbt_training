with


orders as (

    select * from {{ ref('stg_jaffle_shop__orders') }}

),


payments as (

    select * from {{ ref('stg_stripe__payments') }}

),


-- Logical CTEs
completed_payments as (

    select
        order_id,
        max(payment_created_at) as payment_finalized_date,
        sum(payment_amount) as total_amount_paid

    from payments

    where payment_status <> 'fail'

    group by 1
    
),


paid_orders as (

    select
        o.order_id,
        o.customer_id,
        o.order_placed_at,
        o.order_status,

        cp.total_amount_paid,
        cp.payment_finalized_date

    from orders o

    left join completed_payments cp using (order_id)

)

select * from paid_orders
