with

-- Import CTEs
orders as (

    select * from {{ ref('stg_jaffle_shop__orders') }}

),


customers as (

    select * from {{ ref('stg_jaffle_shop__customers') }}

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
        cp.payment_finalized_date,

        c.customer_first_name,
        c.customer_last_name

    from orders o

    left join completed_payments cp using (order_id)
    
    left join customers c using (customer_id)

),


-- Final CTE
final as (

    select
        order_id,
        customer_id,
        order_placed_at,
        order_status,
        total_amount_paid,
        payment_finalized_date,
        customer_first_name,
        customer_last_name,

        -- sales transaction sequence
        row_number() over (order by order_id) as transaction_seq,

        -- customer sales sequence
        row_number() over (
            partition by customer_id order by order_id
        ) as customer_sales_seq,

        -- new vs returning customer
        case  
            when (
                rank() over (
                    partition by customer_id
                    order by order_placed_at, order_id
                ) = 1
            ) then 'new'
            else 'return'
        end as nvsr,

        -- customer lifetime value
        sum(total_amount_paid) over (
            partition by customer_id
        ) as customer_lifetime_value,

        -- first day of sale
        first_value(order_placed_at) over (
            partition by customer_id
            order by order_placed_at
            rows unbounded preceding
        ) as fdos
    
    from paid_orders

)

select * from final