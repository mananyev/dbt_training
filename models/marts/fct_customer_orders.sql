with

customers as (

    select * from {{ ref('stg_jaffle_shop__customers') }}

),


paid_orders as (

    select * from {{ ref('int_orders') }}

),


-- Final CTE
final as (

    select
        po.order_id,
        po.customer_id,
        po.order_placed_at,
        po.order_status,
        po.total_amount_paid,
        po.payment_finalized_date,
        c.customer_first_name,
        c.customer_last_name,

        -- sales transaction sequence
        row_number() over (
            order by po.order_placed_at, po.order_id
        ) as transaction_seq,

        -- customer sales sequence
        row_number() over (
            partition by po.customer_id
            order by po.order_placed_at, po.order_id
        ) as customer_sales_seq,

        -- new vs returning customer
        case  
            when (
                rank() over (
                    partition by po.customer_id
                    order by po.order_placed_at, po.order_id
                ) = 1
            ) then 'new'
            else 'return'
        end as nvsr,

        -- customer lifetime value
        sum(po.total_amount_paid) over (
            partition by po.customer_id
            order by po.order_placed_at, po.order_id
            rows unbounded preceding
        ) as customer_lifetime_value,

        -- first day of sale
        first_value(po.order_placed_at) over (
            partition by po.customer_id
            order by po.order_placed_at, po.order_id
            rows unbounded preceding
        ) as fdos
    
    from paid_orders po
    
    left join customers c using (customer_id)

)

select * from final